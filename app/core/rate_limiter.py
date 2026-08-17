import asyncio
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
import logging
from typing import Any, Optional, Protocol
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import settings
from app.database import AsyncSessionLocal
from app.models.rate_limit_log import RateLimitLog

logger = logging.getLogger(__name__)

# Constant advisory lock key for PostgreSQL transaction-level serialization
RATE_LIMIT_ADVISORY_LOCK_ID = 74291837
RATE_LIMIT_ENDPOINT = "POST /v1/dm/send"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ensure_utc(dt: datetime) -> datetime:
    """Normalizes naive or aware datetime to timezone-aware UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class RateLimiter(Protocol):
    """Protocol interface for rate limiters guarding downstream endpoints."""

    async def acquire(self, dm_outbox_id: Optional[str] = None) -> None:
        """Acquires permission/token to execute a request, waiting if necessary."""
        ...


class DMSendRateLimiter:
    """Strict database-backed rolling-window rate limiter for POST /v1/dm/send requests.
    
    Guarantees that no more than 10 outbound POST /v1/dm/send requests occur within
    any rolling 60.0-second interval.
    
    - Authoritative production concurrency is enforced via PostgreSQL advisory locking
      (pg_advisory_xact_lock).
    - Database transaction and advisory lock are held only for the microsecond duration
      of checking and reserving a slot.
    - If the window is full, the lock is committed/closed before sleeping.
    - Each outbound send attempt (202, 400, 429, 500, network timeout) consumes a slot.
    - Read requests (GET /v1/dm/{dm_id}) do NOT call this limiter and do not consume budget.
    """

    def __init__(
        self,
        session_factory: Optional[async_sessionmaker[AsyncSession]] = None,
        max_requests: Optional[int] = None,
        window_seconds: Optional[int] = None,
        time_provider: Optional[Callable[[], datetime]] = None,
        sleeper: Optional[Callable[[float], Any]] = None,
    ):
        self.session_factory = session_factory or AsyncSessionLocal
        self.max_requests = max_requests if max_requests is not None else settings.DM_SEND_RATE_LIMIT
        self.window_seconds = window_seconds if window_seconds is not None else settings.DM_SEND_RATE_WINDOW_SECONDS
        self.time_provider = time_provider or utc_now
        self.sleeper = sleeper or asyncio.sleep

    async def _try_reserve_slot(self, dm_outbox_id: Optional[str] = None) -> float:
        """Atomically checks rolling window and reserves a slot under database serialization.
        
        Returns:
            0.0 if a slot was successfully reserved immediately.
            float > 0 representing wait duration in seconds before slot opens.
        """
        async with self.session_factory() as session:
            # 1. Acquire PostgreSQL transaction-level advisory lock for production concurrency serialization
            try:
                bind = session.get_bind()
                dialect_name = bind.dialect.name if bind else ""
            except Exception:
                dialect_name = ""

            if dialect_name in ("postgresql", "postgres"):
                await session.execute(text(f"SELECT pg_advisory_xact_lock({RATE_LIMIT_ADVISORY_LOCK_ID})"))

            now = self.time_provider()
            now_utc = ensure_utc(now)
            window_cutoff = now_utc - timedelta(seconds=self.window_seconds)

            # 2. Query active POST /v1/dm/send reservations in the active rolling window (sent_at > now - 60s)
            query = (
                select(RateLimitLog)
                .where(
                    RateLimitLog.endpoint == RATE_LIMIT_ENDPOINT,
                    RateLimitLog.sent_at > window_cutoff,
                )
                .order_by(RateLimitLog.sent_at.asc())
            )
            res = await session.execute(query)
            active_logs = res.scalars().all()
            active_count = len(active_logs)

            # 3. Determine if slot is available
            if active_count < self.max_requests:
                # Slot is available: insert reservation and commit immediately
                new_log = RateLimitLog(
                    endpoint=RATE_LIMIT_ENDPOINT,
                    sent_at=now_utc,
                    dm_outbox_id=dm_outbox_id,
                )
                session.add(new_log)
                await session.commit()

                logger.info(
                    "[RateLimiter] Slot acquired for '%s' (dm_outbox_id=%s) at %s. Active window: %d/%d",
                    RATE_LIMIT_ENDPOINT,
                    dm_outbox_id or "N/A",
                    now_utc.isoformat(),
                    active_count + 1,
                    self.max_requests,
                )
                return 0.0

            else:
                # Window is full: find oldest active reservation occupying the 10-slot budget
                oldest_active_reservation = active_logs[active_count - self.max_requests]
                oldest_sent_at = ensure_utc(oldest_active_reservation.sent_at)
                elapsed = (now_utc - oldest_sent_at).total_seconds()
                # Calculate exact wait until oldest reservation exits the 60s window + epsilon
                wait_duration = max(0.001, (float(self.window_seconds) - elapsed) + 0.05)

                # Commit/close transaction immediately to release lock and DB connection before sleeping
                await session.commit()

                logger.info(
                    "[RateLimiter] Rate limit reached (%d/%d in last %ds). Oldest send at %s. Calculated wait duration: %.3fs",
                    active_count,
                    self.max_requests,
                    self.window_seconds,
                    oldest_sent_at.isoformat(),
                    wait_duration,
                )
                return wait_duration

    async def acquire(self, dm_outbox_id: Optional[str] = None) -> None:
        """Acquires a rate-limit slot prior to sending a POST /v1/dm/send request.
        
        If 10 active sends exist in the rolling 60-second window, waits outside the
        database transaction until the oldest active send exits the window.
        """
        while True:
            wait_duration = await self._try_reserve_slot(dm_outbox_id=dm_outbox_id)
            if wait_duration <= 0.0:
                return

            # Sleep outside any database connection or lock
            res = self.sleeper(wait_duration)
            if asyncio.iscoroutine(res):
                await res


# Global singleton instance used across the application
dm_send_rate_limiter = DMSendRateLimiter()
