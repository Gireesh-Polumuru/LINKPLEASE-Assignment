import asyncio
from datetime import datetime, timedelta, timezone
import logging
import random
from typing import Any, Optional
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.rate_limiter import DMSendRateLimiter, dm_send_rate_limiter
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


class DMDispatchWorker:
    """Background worker that reliably processes and dispatches QUEUED DMOutbox records."""

    def __init__(
        self,
        session_factory: Optional[async_sessionmaker[AsyncSession]] = None,
        pseudogram_client: Optional[PseudoGramClient] = None,
        rate_limiter: Optional[DMSendRateLimiter] = None,
        poll_interval: float = 0.5,
        lease_seconds: int = 60,
    ):
        self.session_factory = session_factory or AsyncSessionLocal
        self.client = pseudogram_client or PseudoGramClient()
        self.rate_limiter = rate_limiter or dm_send_rate_limiter
        self.poll_interval = poll_interval
        self.lease_seconds = lease_seconds
        self.is_running = False
        self._task: Optional[asyncio.Task] = None

    async def recover_stale_jobs(self, db: AsyncSession) -> int:
        """Recovers jobs stuck in SENDING state due to worker process crashes or restarts.
        
        Resets stale SENDING jobs (whose lease expired) back to QUEUED status so they can be
        re-claimed safely.
        """
        stale_cutoff = utc_now() - timedelta(seconds=self.lease_seconds)
        stmt = (
            update(DMOutbox)
            .where(
                DMOutbox.status == DMStatus.SENDING.value,
                DMOutbox.updated_at <= stale_cutoff,
            )
            .values(
                status=DMStatus.QUEUED.value,
                last_error="Recovered from stale SENDING lease after process restart",
                updated_at=utc_now(),
            )
        )
        res = await db.execute(stmt)
        await db.commit()
        recovered_count = res.rowcount
        if recovered_count > 0:
            logger.info("[DMWorker] Recovered %d stale SENDING outbox jobs to QUEUED", recovered_count)
        return recovered_count

    async def claim_next_job(self, db: AsyncSession) -> Optional[dict[str, Any]]:
        """Safely claims the next eligible QUEUED outbox job using row locking.
        
        Transitions QUEUED -> SENDING and commits immediately before initiating external I/O.
        """
        now = utc_now()
        query = (
            select(DMOutbox)
            .where(
                DMOutbox.status == DMStatus.QUEUED.value,
                DMOutbox.next_retry_at <= now,
            )
            .order_by(DMOutbox.next_retry_at.asc(), DMOutbox.created_at.asc())
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        res = await db.execute(query)
        outbox = res.scalar_one_or_none()
        if outbox is None:
            return None

        # Transition to SENDING and increment attempt count
        outbox.status = DMStatus.SENDING.value
        outbox.attempts += 1
        outbox.updated_at = now

        job_data = {
            "id": outbox.id,
            "user_id": outbox.user_id,
            "rule_id": outbox.rule_id,
            "comment_id": outbox.comment_id,
            "message": outbox.message,
            "idempotency_key": outbox.idempotency_key,
            "attempts": outbox.attempts,
            "max_attempts": outbox.max_attempts,
        }

        # Commit immediately to release row lock & DB connection before HTTP request
        await db.commit()

        logger.info(
            "[DMWorker] Claimed outbox job id='%s', user='%s', attempt=%d/%d",
            job_data["id"],
            job_data["user_id"],
            job_data["attempts"],
            job_data["max_attempts"],
        )
        return job_data

    async def dispatch_job(self, job: dict[str, Any]) -> None:
        """Executes rate-limited HTTP dispatch and updates delivery state in a new transaction."""
        job_id = job["id"]
        logger.info("[DMWorker] Dispatching DM job id='%s' to PseudoGram...", job_id)

        # 1. Mandatory Rate Limiter Guard
        await self.rate_limiter.acquire(dm_outbox_id=job_id)

        # 2. Execute HTTP Send
        try:
            resp = await self.client.send_dm(
                recipient_user_id=job["user_id"],
                message=job["message"],
                comment_id=job["comment_id"],
                idempotency_key=job["idempotency_key"],
            )

            # 3. HTTP 202 Accepted -> Transition to SENT
            dm_id = resp.get("dm_id")
            logger.info("[DMWorker] 202 Accepted for job id='%s', external dm_id='%s'", job_id, dm_id)

            async with self.session_factory() as session:
                stmt = (
                    update(DMOutbox)
                    .where(DMOutbox.id == job_id, DMOutbox.status == DMStatus.SENDING.value)
                    .values(
                        status=DMStatus.SENT.value,
                        dm_id=dm_id,
                        sent_at=utc_now(),
                        last_error=None,
                        updated_at=utc_now(),
                    )
                )
                await session.execute(stmt)
                await session.commit()

        except PseudoGramBadRequestError as bad_req_err:
            # 4. HTTP 400 Bad Request -> Non-retryable Permanent Failure
            logger.warning("[DMWorker] 400 Bad Request for job id='%s': %s", job_id, bad_req_err.detail)
            async with self.session_factory() as session:
                stmt = (
                    update(DMOutbox)
                    .where(DMOutbox.id == job_id)
                    .values(
                        status=DMStatus.FAILED.value,
                        last_error=f"HTTP 400 Bad Request (Non-retryable): {bad_req_err.detail}",
                        updated_at=utc_now(),
                    )
                )
                await session.execute(stmt)
                await session.commit()

        except PseudoGramRateLimitError as rate_limit_err:
            # 5. HTTP 429 Rate Limited -> Retry after specified duration
            retry_delay = rate_limit_err.retry_after
            next_retry = utc_now() + timedelta(seconds=retry_delay)
            logger.warning("[DMWorker] 429 Rate Limited for job id='%s'. Scheduling retry at %s (in %ss)", job_id, next_retry, retry_delay)

            async with self.session_factory() as session:
                stmt = (
                    update(DMOutbox)
                    .where(DMOutbox.id == job_id)
                    .values(
                        status=DMStatus.QUEUED.value,
                        next_retry_at=next_retry,
                        last_error=f"HTTP 429 Rate Limited: retry after {retry_delay}s",
                        updated_at=utc_now(),
                    )
                )
                await session.execute(stmt)
                await session.commit()

        except (PseudoGramServerError, PseudoGramNetworkError) as retryable_err:
            # 6. HTTP 500+ / Network Timeout -> Retry with Exponential Backoff + Jitter
            attempts = job["attempts"]
            max_attempts = job["max_attempts"]

            async with self.session_factory() as session:
                if attempts >= max_attempts:
                    logger.error("[DMWorker] Permanent failure for job id='%s': max attempts reached (%d/%d)", job_id, attempts, max_attempts)
                    stmt = (
                        update(DMOutbox)
                        .where(DMOutbox.id == job_id)
                        .values(
                            status=DMStatus.FAILED.value,
                            last_error=f"Max retry attempts exceeded ({attempts}/{max_attempts}): {retryable_err.detail}",
                            updated_at=utc_now(),
                        )
                    )
                else:
                    # Exponential backoff formula: min(60, base * 2^(attempts-1)) * jitter
                    base_delay = min(60.0, 2.0 * (2.0 ** (attempts - 1)))
                    jitter_factor = random.uniform(0.75, 1.25)
                    delay_seconds = base_delay * jitter_factor
                    next_retry = utc_now() + timedelta(seconds=delay_seconds)

                    logger.warning(
                        "[DMWorker] Retryable error for job id='%s' (attempt %d/%d): %s. Next retry scheduled at %s (in %.2fs)",
                        job_id,
                        attempts,
                        max_attempts,
                        retryable_err.detail,
                        next_retry,
                        delay_seconds,
                    )
                    stmt = (
                        update(DMOutbox)
                        .where(DMOutbox.id == job_id)
                        .values(
                            status=DMStatus.QUEUED.value,
                            next_retry_at=next_retry,
                            last_error=f"Retryable failure (attempt {attempts}/{max_attempts}): {retryable_err.detail}",
                            updated_at=utc_now(),
                        )
                    )
                await session.execute(stmt)
                await session.commit()

    async def process_one_cycle(self) -> bool:
        """Runs a single worker processing pass. Returns True if work was processed, False otherwise."""
        async with self.session_factory() as session:
            # 1. Recover stale leases
            await self.recover_stale_jobs(session)

            # 2. Claim next available job
            job = await self.claim_next_job(session)

        if job is not None:
            # 3. Dispatch job (outside database transaction)
            await self.dispatch_job(job)
            return True

        return False

    async def run_worker_loop(self) -> None:
        """Main continuous background worker loop."""
        logger.info("[DMWorker] DM Dispatch Worker loop started.")
        while self.is_running:
            try:
                had_work = await self.process_one_cycle()
                if not had_work:
                    await asyncio.sleep(self.poll_interval)
            except asyncio.CancelledError:
                logger.info("[DMWorker] Worker loop cancelled.")
                break
            except Exception as loop_err:
                logger.error("[DMWorker] Unexpected error in worker loop: %s", loop_err, exc_info=True)
                await asyncio.sleep(self.poll_interval)

    def start(self) -> None:
        """Starts the background worker task."""
        if not self.is_running:
            self.is_running = True
            self._task = asyncio.create_task(self.run_worker_loop())
            logger.info("[DMWorker] Background dispatch task spawned.")

    async def stop(self) -> None:
        """Gracefully stops the background worker task."""
        if self.is_running:
            self.is_running = False
            if self._task:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
                self._task = None
            logger.info("[DMWorker] Background dispatch task stopped.")


# Default worker singleton instance
dm_worker = DMDispatchWorker()
