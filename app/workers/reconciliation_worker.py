import asyncio
from datetime import datetime, timedelta, timezone
import logging
from typing import Any, Optional
from sqlalchemy import or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.database import AsyncSessionLocal
from app.models.dm_outbox import DMOutbox, DMStatus
from app.services.pseudogram_client import (
    PseudoGramBadRequestError,
    PseudoGramClient,
    PseudoGramNetworkError,
    PseudoGramRateLimitError,
    PseudoGramServerError,
)

logger = logging.getLogger(__name__)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class DeliveryReconciliationWorker:
    """Background worker that reconciles SENT DMOutbox records by polling PseudoGram GET /v1/dm/{dm_id}.
    
    IMPORTANT:
    - Status polling uses GET /v1/dm/{dm_id}, which is unrestricted and does NOT consume
      the 10 requests / rolling 60 seconds POST /v1/dm/send rate-limit budget.
    - Database transactions are kept ultra-short and are NEVER held during external HTTP requests.
    - Concurrency safety across multiple workers is guaranteed via row locking (FOR UPDATE SKIP LOCKED)
      and atomic lease reservation.
    """

    def __init__(
        self,
        session_factory: Optional[async_sessionmaker[AsyncSession]] = None,
        pseudogram_client: Optional[PseudoGramClient] = None,
        poll_interval: float = 1.0,
        recheck_interval: float = 5.0,
        batch_size: int = 10,
    ):
        self.session_factory = session_factory or AsyncSessionLocal
        self.client = pseudogram_client or PseudoGramClient()
        self.poll_interval = poll_interval
        self.recheck_interval = recheck_interval
        self.batch_size = batch_size
        self.is_running = False
        self._task: Optional[asyncio.Task] = None

    async def fetch_eligible_jobs(self, db: AsyncSession) -> list[dict[str, Any]]:
        """Atomically fetches and leases eligible SENT DMOutbox records for status reconciliation.
        
        A record is eligible if:
        1. status == 'SENT'
        2. dm_id is not null and not empty
        3. last_reconciled_at is NULL or last_reconciled_at <= (now - recheck_interval)
        
        Transitions lease immediately by updating last_reconciled_at under row lock before commit.
        """
        now = utc_now()
        cutoff = now - timedelta(seconds=self.recheck_interval)

        # 1. Clean up any invalid SENT records missing external dm_id
        invalid_stmt = (
            update(DMOutbox)
            .where(
                DMOutbox.status == DMStatus.SENT.value,
                or_(DMOutbox.dm_id.is_(None), DMOutbox.dm_id == ""),
            )
            .values(
                status=DMStatus.FAILED.value,
                last_error="Missing external dm_id for SENT record during reconciliation",
                last_reconciled_at=now,
                updated_at=now,
            )
        )
        await db.execute(invalid_stmt)

        # 2. Select eligible SENT records with row locking
        query = (
            select(DMOutbox.id, DMOutbox.dm_id)
            .where(
                DMOutbox.status == DMStatus.SENT.value,
                DMOutbox.dm_id.isnot(None),
                DMOutbox.dm_id != "",
                or_(
                    DMOutbox.last_reconciled_at.is_(None),
                    DMOutbox.last_reconciled_at <= cutoff,
                ),
            )
            .order_by(DMOutbox.sent_at.asc().nulls_last(), DMOutbox.created_at.asc())
            .limit(self.batch_size)
            .with_for_update(skip_locked=True)
        )
        res = await db.execute(query)
        rows = res.all()
        if not rows:
            await db.commit()
            return []

        job_ids = [row[0] for row in rows]
        job_data = [{"id": row[0], "dm_id": row[1]} for row in rows]

        # 3. Advance last_reconciled_at immediately to reserve lease before releasing lock
        lease_stmt = (
            update(DMOutbox)
            .where(DMOutbox.id.in_(job_ids))
            .values(last_reconciled_at=now, updated_at=now)
        )
        await db.execute(lease_stmt)
        await db.commit()

        logger.info("[ReconciliationWorker] Claimed %d SENT records for delivery verification", len(job_data))
        return job_data

    async def reconcile_single_job(self, job: dict[str, Any]) -> None:
        """Polls PseudoGram GET /v1/dm/{dm_id} and updates the DMOutbox delivery state."""
        job_id = job["id"]
        dm_id = job["dm_id"]
        now = utc_now()

        logger.info("[ReconciliationWorker] Checking delivery status for job id='%s', dm_id='%s'", job_id, dm_id)

        try:
            # External HTTP GET (No rate limiter required - read requests are unrestricted)
            resp = await self.client.get_dm_status(dm_id)
            raw_status = str(resp.get("status", "")).strip().lower()

            if raw_status == "delivered":
                logger.info("[ReconciliationWorker] Job id='%s' confirmed DELIVERED", job_id)
                async with self.session_factory() as session:
                    stmt = (
                        update(DMOutbox)
                        .where(DMOutbox.id == job_id, DMOutbox.status == DMStatus.SENT.value)
                        .values(
                            status=DMStatus.DELIVERED.value,
                            delivered_at=now,
                            last_reconciled_at=now,
                            last_error=None,
                            updated_at=now,
                        )
                    )
                    await session.execute(stmt)
                    await session.commit()

            elif raw_status == "failed":
                reason = (
                    resp.get("reason")
                    or resp.get("error")
                    or resp.get("detail")
                    or "Delivery marked as failed downstream"
                )
                logger.warning("[ReconciliationWorker] Job id='%s' failed downstream: %s", job_id, reason)
                async with self.session_factory() as session:
                    stmt = (
                        update(DMOutbox)
                        .where(DMOutbox.id == job_id, DMOutbox.status == DMStatus.SENT.value)
                        .values(
                            status=DMStatus.FAILED.value,
                            last_error=f"Downstream delivery failure: {reason}",
                            last_reconciled_at=now,
                            updated_at=now,
                        )
                    )
                    await session.execute(stmt)
                    await session.commit()

            else:
                # Still pending / processing / queued -> remains SENT
                logger.info(
                    "[ReconciliationWorker] Job id='%s' still in progress downstream (status='%s'). Remaining in SENT state.",
                    job_id,
                    raw_status or "pending",
                )
                async with self.session_factory() as session:
                    stmt = (
                        update(DMOutbox)
                        .where(DMOutbox.id == job_id, DMOutbox.status == DMStatus.SENT.value)
                        .values(last_reconciled_at=now, updated_at=now)
                    )
                    await session.execute(stmt)
                    await session.commit()

        except PseudoGramBadRequestError as bad_req_err:
            # 400/404 - Permanent failure
            logger.warning("[ReconciliationWorker] 400/404 for job id='%s': %s", job_id, bad_req_err.detail)
            async with self.session_factory() as session:
                stmt = (
                    update(DMOutbox)
                    .where(DMOutbox.id == job_id, DMOutbox.status == DMStatus.SENT.value)
                    .values(
                        status=DMStatus.FAILED.value,
                        last_error=f"Reconciliation fatal error: {bad_req_err.detail}",
                        last_reconciled_at=now,
                        updated_at=now,
                    )
                )
                await session.execute(stmt)
                await session.commit()

        except (PseudoGramServerError, PseudoGramNetworkError, PseudoGramRateLimitError) as transient_err:
            # Transient error - record remains in SENT, next cycle will re-verify
            logger.warning(
                "[ReconciliationWorker] Transient error checking status for job id='%s': %s",
                job_id,
                transient_err.message if hasattr(transient_err, "message") else str(transient_err),
            )
            async with self.session_factory() as session:
                stmt = (
                    update(DMOutbox)
                    .where(DMOutbox.id == job_id, DMOutbox.status == DMStatus.SENT.value)
                    .values(last_reconciled_at=now, updated_at=now)
                )
                await session.execute(stmt)
                await session.commit()

    async def process_one_cycle(self) -> int:
        """Executes a single reconciliation pass over eligible SENT records.
        
        Returns:
            Number of records reconciled in this cycle.
        """
        async with self.session_factory() as session:
            jobs = await self.fetch_eligible_jobs(session)

        if not jobs:
            return 0

        for job in jobs:
            await self.reconcile_single_job(job)

        return len(jobs)

    async def run_worker_loop(self) -> None:
        """Main continuous background reconciliation loop."""
        logger.info("[ReconciliationWorker] Delivery Reconciliation Worker loop started.")
        while self.is_running:
            try:
                processed_count = await self.process_one_cycle()
                if processed_count == 0:
                    await asyncio.sleep(self.poll_interval)
            except asyncio.CancelledError:
                logger.info("[ReconciliationWorker] Worker loop cancelled.")
                break
            except Exception as loop_err:
                logger.error("[ReconciliationWorker] Unexpected error in reconciliation loop: %s", loop_err, exc_info=True)
                await asyncio.sleep(self.poll_interval)

    def start(self) -> None:
        """Starts the background reconciliation worker task."""
        if not self.is_running:
            self.is_running = True
            self._task = asyncio.create_task(self.run_worker_loop())
            logger.info("[ReconciliationWorker] Background reconciliation task spawned.")

    async def stop(self) -> None:
        """Gracefully stops the background reconciliation worker task."""
        if self.is_running:
            self.is_running = False
            if self._task:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
                self._task = None
            logger.info("[ReconciliationWorker] Background reconciliation task stopped.")


# Default reconciliation worker singleton instance
reconciliation_worker = DeliveryReconciliationWorker()
