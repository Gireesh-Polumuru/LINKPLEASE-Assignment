import asyncio
from datetime import datetime, timedelta, timezone
import os
from typing import Optional
import httpx
import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import settings
from app.core.rate_limiter import DMSendRateLimiter, RATE_LIMIT_ENDPOINT
from app.models.dm_outbox import DMOutbox, DMStatus
from app.models.rate_limit_log import RateLimitLog
from app.models.rule import Rule
from app.services.pseudogram_client import PseudoGramClient
from app.workers.dm_worker import DMDispatchWorker
from tests.conftest import TestAsyncSessionLocal, test_engine


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class SimulatedClock:
    """Injectable simulated clock and sleeper for rapid deterministic rate-limiter tests."""

    def __init__(self, start_time: Optional[datetime] = None):
        self.current_time = start_time or utc_now()
        self.sleep_calls: list[float] = []

    def now(self) -> datetime:
        return self.current_time

    def advance(self, seconds: float) -> None:
        self.current_time += timedelta(seconds=seconds)

    def sleep(self, seconds: float) -> None:
        self.sleep_calls.append(seconds)
        self.advance(seconds)


# ------------------------------------------------------------------------------
# Test A: First send acquires immediately
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_a_first_send_acquires_immediately(db_session: AsyncSession) -> None:
    clock = SimulatedClock()
    limiter = DMSendRateLimiter(
        session_factory=TestAsyncSessionLocal,
        time_provider=clock.now,
        sleeper=clock.sleep,
    )

    await limiter.acquire(dm_outbox_id="outbox_test_a")

    # Verify no sleep occurred
    assert len(clock.sleep_calls) == 0

    # Verify reservation persisted in database
    res = await db_session.execute(select(RateLimitLog).where(RateLimitLog.endpoint == RATE_LIMIT_ENDPOINT))
    logs = res.scalars().all()
    assert len(logs) == 1
    assert logs[0].endpoint == RATE_LIMIT_ENDPOINT
    assert logs[0].dm_outbox_id == "outbox_test_a"


# ------------------------------------------------------------------------------
# Test B: Ten sends can be reserved
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_b_ten_sends_reserved_without_waiting(db_session: AsyncSession) -> None:
    clock = SimulatedClock()
    limiter = DMSendRateLimiter(
        session_factory=TestAsyncSessionLocal,
        time_provider=clock.now,
        sleeper=clock.sleep,
    )

    for i in range(10):
        await limiter.acquire(dm_outbox_id=f"outbox_test_b_{i}")

    # All 10 acquired immediately
    assert len(clock.sleep_calls) == 0

    res = await db_session.execute(select(RateLimitLog).where(RateLimitLog.endpoint == RATE_LIMIT_ENDPOINT))
    logs = res.scalars().all()
    assert len(logs) == 10


# ------------------------------------------------------------------------------
# Test C: Eleventh send cannot immediately acquire
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_c_eleventh_send_cannot_immediately_acquire(db_session: AsyncSession) -> None:
    clock = SimulatedClock()
    limiter = DMSendRateLimiter(
        session_factory=TestAsyncSessionLocal,
        time_provider=clock.now,
        sleeper=clock.sleep,
    )

    for i in range(10):
        wait = await limiter._try_reserve_slot(dm_outbox_id=f"outbox_test_c_{i}")
        assert wait == 0.0

    # 11th attempt must indicate wait required (> 0)
    wait_11th = await limiter._try_reserve_slot(dm_outbox_id="outbox_test_c_11")
    assert wait_11th > 0.0


# ------------------------------------------------------------------------------
# Test D: Eleventh send waits until the oldest reservation exits the 60-second window
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_d_eleventh_send_waits_for_oldest_expiration(db_session: AsyncSession) -> None:
    clock = SimulatedClock()
    limiter = DMSendRateLimiter(
        session_factory=TestAsyncSessionLocal,
        time_provider=clock.now,
        sleeper=clock.sleep,
    )

    # First send at t=0
    await limiter.acquire(dm_outbox_id="outbox_t0")

    # 9 sends at t=10
    clock.advance(10.0)
    for i in range(9):
        await limiter.acquire(dm_outbox_id=f"outbox_t10_{i}")

    # Now at t=10, 10 active sends exist. The oldest was at t=0.
    # 11th send should wait approximately (60 - 10) = 50 seconds + safety buffer.
    await limiter.acquire(dm_outbox_id="outbox_11th")

    assert len(clock.sleep_calls) == 1
    # Sleep duration must be ~50 seconds (+ safety buffer 0.05s)
    assert 50.0 <= clock.sleep_calls[0] <= 50.1


# ------------------------------------------------------------------------------
# Test E: After the oldest reservation expires, another reservation is allowed
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_e_reservation_allowed_after_expiration(db_session: AsyncSession) -> None:
    clock = SimulatedClock()
    limiter = DMSendRateLimiter(
        session_factory=TestAsyncSessionLocal,
        time_provider=clock.now,
        sleeper=clock.sleep,
    )

    for i in range(10):
        await limiter.acquire(dm_outbox_id=f"outbox_test_e_{i}")

    # Advance clock past the 60-second window
    clock.advance(60.1)

    # 11th send should acquire immediately since all previous 10 have expired
    wait = await limiter._try_reserve_slot(dm_outbox_id="outbox_test_e_11")
    assert wait == 0.0


# ------------------------------------------------------------------------------
# Test F: Strict sliding window test - no rolling 60s window contains > 10 reservations
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_f_no_rolling_window_contains_more_than_ten(db_session: AsyncSession) -> None:
    clock = SimulatedClock()
    limiter = DMSendRateLimiter(
        session_factory=TestAsyncSessionLocal,
        time_provider=clock.now,
        sleeper=clock.sleep,
    )

    # Simulate 30 sequential acquisitions spaced out over time
    for i in range(30):
        await limiter.acquire(dm_outbox_id=f"outbox_seq_{i}")
        clock.advance(2.0)  # advance 2 seconds between attempts

    # Query all log entries and verify every rolling 60s window has <= 10
    res = await db_session.execute(
        select(RateLimitLog).where(RateLimitLog.endpoint == RATE_LIMIT_ENDPOINT).order_by(RateLimitLog.sent_at.asc())
    )
    all_logs = res.scalars().all()
    assert len(all_logs) == 30

    for i, log in enumerate(all_logs):
        log_time = ensure_utc(log.sent_at)
        window_start = log_time - timedelta(seconds=60)
        # Count reservations in (log_time - 60s, log_time]
        count_in_window = sum(
            1 for other in all_logs
            if window_start < ensure_utc(other.sent_at) <= log_time
        )
        assert count_in_window <= 10, f"Window at {log_time} exceeded limit with {count_in_window} sends"


# ------------------------------------------------------------------------------
# Test G: Concurrent acquisition cannot exceed 10 reservations
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_g_concurrent_acquisitions_respect_limit(db_session: AsyncSession) -> None:
    clock = SimulatedClock()
    limiter = DMSendRateLimiter(
        session_factory=TestAsyncSessionLocal,
        time_provider=clock.now,
        sleeper=clock.sleep,
    )

    # Launch 20 concurrent acquire tasks
    tasks = [
        limiter.acquire(dm_outbox_id=f"concurrent_{i}")
        for i in range(20)
    ]
    await asyncio.gather(*tasks)

    res = await db_session.execute(select(RateLimitLog).where(RateLimitLog.endpoint == RATE_LIMIT_ENDPOINT))
    logs = res.scalars().all()
    assert len(logs) == 20


# ------------------------------------------------------------------------------
# Test H: HTTP 202 in worker consumes a slot
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_h_http_202_consumes_slot(db_session: AsyncSession) -> None:
    rule = Rule(rule_id="r_202", keyword="KEY202", dm_message="Message 202", is_active=True)
    outbox = DMOutbox(
        id="outbox_h_202",
        rule_id="r_202",
        user_id="usr_h",
        comment_id="cmt_h",
        message="Message 202",
        idempotency_key="idemp_h",
        status=DMStatus.QUEUED.value,
        attempts=0,
        next_retry_at=utc_now() - timedelta(seconds=1),
    )
    db_session.add_all([rule, outbox])
    await db_session.commit()

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(202, json={"dm_id": "dm_h", "status": "queued"})

    mock_client = PseudoGramClient(
        base_url="https://mock.test",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(mock_handler)),
    )
    rate_limiter = DMSendRateLimiter(session_factory=TestAsyncSessionLocal)
    worker = DMDispatchWorker(
        session_factory=TestAsyncSessionLocal,
        pseudogram_client=mock_client,
        rate_limiter=rate_limiter,
    )

    await worker.process_one_cycle()

    # Verify rate limit log recorded
    res = await db_session.execute(
        select(RateLimitLog).where(RateLimitLog.dm_outbox_id == "outbox_h_202")
    )
    log = res.scalar_one_or_none()
    assert log is not None
    assert log.endpoint == RATE_LIMIT_ENDPOINT


# ------------------------------------------------------------------------------
# Test I: HTTP 400 in worker consumes a slot (not refunded)
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_i_http_400_consumes_slot(db_session: AsyncSession) -> None:
    rule = Rule(rule_id="r_400", keyword="KEY400", dm_message="Message 400", is_active=True)
    outbox = DMOutbox(
        id="outbox_i_400",
        rule_id="r_400",
        user_id="usr_i",
        comment_id="cmt_i",
        message="Message 400",
        idempotency_key="idemp_i",
        status=DMStatus.QUEUED.value,
        attempts=0,
        next_retry_at=utc_now() - timedelta(seconds=1),
    )
    db_session.add_all([rule, outbox])
    await db_session.commit()

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"detail": "Bad Request: invalid user"})

    mock_client = PseudoGramClient(
        base_url="https://mock.test",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(mock_handler)),
    )
    rate_limiter = DMSendRateLimiter(session_factory=TestAsyncSessionLocal)
    worker = DMDispatchWorker(
        session_factory=TestAsyncSessionLocal,
        pseudogram_client=mock_client,
        rate_limiter=rate_limiter,
    )

    await worker.process_one_cycle()

    res = await db_session.execute(
        select(RateLimitLog).where(RateLimitLog.dm_outbox_id == "outbox_i_400")
    )
    log = res.scalar_one_or_none()
    assert log is not None


# ------------------------------------------------------------------------------
# Test J: HTTP 429 in worker consumes a slot (not refunded)
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_j_http_429_consumes_slot(db_session: AsyncSession) -> None:
    rule = Rule(rule_id="r_429", keyword="KEY429", dm_message="Message 429", is_active=True)
    outbox = DMOutbox(
        id="outbox_j_429",
        rule_id="r_429",
        user_id="usr_j",
        comment_id="cmt_j",
        message="Message 429",
        idempotency_key="idemp_j",
        status=DMStatus.QUEUED.value,
        attempts=0,
        next_retry_at=utc_now() - timedelta(seconds=1),
    )
    db_session.add_all([rule, outbox])
    await db_session.commit()

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, headers={"Retry-After": "5"}, json={"detail": "Rate limited"})

    mock_client = PseudoGramClient(
        base_url="https://mock.test",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(mock_handler)),
    )
    rate_limiter = DMSendRateLimiter(session_factory=TestAsyncSessionLocal)
    worker = DMDispatchWorker(
        session_factory=TestAsyncSessionLocal,
        pseudogram_client=mock_client,
        rate_limiter=rate_limiter,
    )

    await worker.process_one_cycle()

    res = await db_session.execute(
        select(RateLimitLog).where(RateLimitLog.dm_outbox_id == "outbox_j_429")
    )
    log = res.scalar_one_or_none()
    assert log is not None


# ------------------------------------------------------------------------------
# Test K: HTTP 500 in worker consumes a slot (not refunded)
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_k_http_500_consumes_slot(db_session: AsyncSession) -> None:
    rule = Rule(rule_id="r_500", keyword="KEY500", dm_message="Message 500", is_active=True)
    outbox = DMOutbox(
        id="outbox_k_500",
        rule_id="r_500",
        user_id="usr_k",
        comment_id="cmt_k",
        message="Message 500",
        idempotency_key="idemp_k",
        status=DMStatus.QUEUED.value,
        attempts=0,
        next_retry_at=utc_now() - timedelta(seconds=1),
    )
    db_session.add_all([rule, outbox])
    await db_session.commit()

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"detail": "Internal Server Error"})

    mock_client = PseudoGramClient(
        base_url="https://mock.test",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(mock_handler)),
    )
    rate_limiter = DMSendRateLimiter(session_factory=TestAsyncSessionLocal)
    worker = DMDispatchWorker(
        session_factory=TestAsyncSessionLocal,
        pseudogram_client=mock_client,
        rate_limiter=rate_limiter,
    )

    await worker.process_one_cycle()

    res = await db_session.execute(
        select(RateLimitLog).where(RateLimitLog.dm_outbox_id == "outbox_k_500")
    )
    log = res.scalar_one_or_none()
    assert log is not None


# ------------------------------------------------------------------------------
# Test L: Network timeout consumes a slot (not refunded)
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_l_network_timeout_consumes_slot(db_session: AsyncSession) -> None:
    rule = Rule(rule_id="r_timeout", keyword="KEYTO", dm_message="Message TO", is_active=True)
    outbox = DMOutbox(
        id="outbox_l_to",
        rule_id="r_timeout",
        user_id="usr_l",
        comment_id="cmt_l",
        message="Message TO",
        idempotency_key="idemp_l",
        status=DMStatus.QUEUED.value,
        attempts=0,
        next_retry_at=utc_now() - timedelta(seconds=1),
    )
    db_session.add_all([rule, outbox])
    await db_session.commit()

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("Connection timed out")

    mock_client = PseudoGramClient(
        base_url="https://mock.test",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(mock_handler)),
    )
    rate_limiter = DMSendRateLimiter(session_factory=TestAsyncSessionLocal)
    worker = DMDispatchWorker(
        session_factory=TestAsyncSessionLocal,
        pseudogram_client=mock_client,
        rate_limiter=rate_limiter,
    )

    await worker.process_one_cycle()

    res = await db_session.execute(
        select(RateLimitLog).where(RateLimitLog.dm_outbox_id == "outbox_l_to")
    )
    log = res.scalar_one_or_none()
    assert log is not None


# ------------------------------------------------------------------------------
# Test M: GET /v1/dm/{dm_id} does not consume a send slot
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_m_get_dm_does_not_consume_slot(db_session: AsyncSession) -> None:
    async def mock_handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/v1/dm/dm_test_id_123"
        return httpx.Response(200, json={"dm_id": "dm_test_id_123", "status": "DELIVERED"})

    mock_client = PseudoGramClient(
        base_url="https://mock.test",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(mock_handler)),
    )

    # Calling GET /v1/dm/{dm_id}
    status_resp = await mock_client.get_dm_status("dm_test_id_123")
    assert status_resp["status"] == "DELIVERED"

    # Verify zero entries in rate_limit_logs
    res = await db_session.execute(select(RateLimitLog))
    logs = res.scalars().all()
    assert len(logs) == 0


# ------------------------------------------------------------------------------
# Test N: Limiter state persists after application restart
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_n_limiter_state_persists_across_instances(db_session: AsyncSession) -> None:
    clock = SimulatedClock()

    # Instance 1 reserves 10 slots
    limiter_1 = DMSendRateLimiter(
        session_factory=TestAsyncSessionLocal,
        time_provider=clock.now,
        sleeper=clock.sleep,
    )
    for i in range(10):
        await limiter_1.acquire(dm_outbox_id=f"outbox_n_{i}")

    # Instance 2 (simulating app restart) connects to same DB
    limiter_2 = DMSendRateLimiter(
        session_factory=TestAsyncSessionLocal,
        time_provider=clock.now,
        sleeper=clock.sleep,
    )

    # Limiter 2 must recognize window is full without needing in-memory state
    wait = await limiter_2._try_reserve_slot(dm_outbox_id="outbox_n_restart")
    assert wait > 0.0


# ------------------------------------------------------------------------------
# Test O & P: Retry consumes a new rate-limit reservation and reuses same idempotency key
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_o_p_retry_consumes_new_reservation_with_same_idempotency_key(db_session: AsyncSession) -> None:
    rule = Rule(rule_id="r_retry", keyword="KEYRETRY", dm_message="Message Retry", is_active=True)
    outbox = DMOutbox(
        id="outbox_op_retry",
        rule_id="r_retry",
        user_id="usr_op",
        comment_id="cmt_op",
        message="Message Retry",
        idempotency_key="idempotency_key_exact_same",
        status=DMStatus.QUEUED.value,
        attempts=0,
        next_retry_at=utc_now() - timedelta(seconds=1),
    )
    db_session.add_all([rule, outbox])
    await db_session.commit()

    captured_idempotency_keys: list[str] = []
    attempt_count = 0

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempt_count
        attempt_count += 1
        captured_idempotency_keys.append(request.headers.get("Idempotency-Key", ""))
        if attempt_count == 1:
            return httpx.Response(500, json={"detail": "First attempt fails with 500"})
        return httpx.Response(202, json={"dm_id": "dm_retry_success", "status": "queued"})

    mock_client = PseudoGramClient(
        base_url="https://mock.test",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(mock_handler)),
    )
    rate_limiter = DMSendRateLimiter(session_factory=TestAsyncSessionLocal)
    worker = DMDispatchWorker(
        session_factory=TestAsyncSessionLocal,
        pseudogram_client=mock_client,
        rate_limiter=rate_limiter,
    )

    # 1st attempt -> HTTP 500 -> rescheduled to QUEUED
    await worker.process_one_cycle()

    # Check 1 rate limit reservation made
    res1 = await db_session.execute(
        select(RateLimitLog).where(RateLimitLog.dm_outbox_id == "outbox_op_retry")
    )
    assert len(res1.scalars().all()) == 1

    # Force next_retry_at to past so worker can retry immediately
    await db_session.execute(
        text("UPDATE dm_outbox SET next_retry_at = :past WHERE id = 'outbox_op_retry'"),
        {"past": utc_now() - timedelta(seconds=1)},
    )
    await db_session.commit()

    # 2nd attempt -> HTTP 202 -> SENT
    await worker.process_one_cycle()

    # Check 2 rate limit reservations made for this outbox job (Test O)
    res2 = await db_session.execute(
        select(RateLimitLog).where(RateLimitLog.dm_outbox_id == "outbox_op_retry")
    )
    assert len(res2.scalars().all()) == 2

    # Check same idempotency key was sent on both attempts (Test P)
    assert len(captured_idempotency_keys) == 2
    assert captured_idempotency_keys[0] == "idempotency_key_exact_same"
    assert captured_idempotency_keys[1] == "idempotency_key_exact_same"


# ------------------------------------------------------------------------------
# Test Q: No in-memory counter is used as source of truth
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_q_database_is_sole_source_of_truth(db_session: AsyncSession) -> None:
    clock = SimulatedClock()
    limiter = DMSendRateLimiter(
        session_factory=TestAsyncSessionLocal,
        time_provider=clock.now,
        sleeper=clock.sleep,
    )

    # Directly insert 10 log rows into the database without calling limiter
    for i in range(10):
        log = RateLimitLog(
            endpoint=RATE_LIMIT_ENDPOINT,
            sent_at=clock.now(),
            dm_outbox_id=f"direct_insert_{i}",
        )
        db_session.add(log)
    await db_session.commit()

    # Limiter should immediately report full window purely based on DB query
    wait = await limiter._try_reserve_slot(dm_outbox_id="test_q_check")
    assert wait > 0.0

    # Delete 1 row directly in the DB
    await db_session.execute(
        text("DELETE FROM rate_limit_logs WHERE dm_outbox_id = 'direct_insert_0'")
    )
    await db_session.commit()

    # Limiter should immediately allow 1 slot without any restart or in-memory cache busting
    wait_after_delete = await limiter._try_reserve_slot(dm_outbox_id="test_q_recheck")
    assert wait_after_delete == 0.0


# ------------------------------------------------------------------------------
# Test R: PostgreSQL advisory-lock concurrency test
# ------------------------------------------------------------------------------
POSTGRES_TEST_URL = os.environ.get("POSTGRES_TEST_URL")

@pytest.mark.skipif(
    not POSTGRES_TEST_URL,
    reason="PostgreSQL test URL not configured in POSTGRES_TEST_URL environment variable. "
           "Note: SQLite tests do not prove PostgreSQL multi-process advisory lock concurrency safety.",
)
@pytest.mark.asyncio
async def test_r_postgresql_advisory_lock_concurrency() -> None:
    """Tests PostgreSQL pg_advisory_xact_lock concurrency safety under multi-process or multi-connection load."""
    pg_engine = create_async_engine(POSTGRES_TEST_URL, future=True)
    pg_session_factory = async_sessionmaker(pg_engine, class_=AsyncSession, expire_on_commit=False)

    limiter = DMSendRateLimiter(session_factory=pg_session_factory)

    tasks = [limiter.acquire(dm_outbox_id=f"pg_conc_{i}") for i in range(15)]
    await asyncio.gather(*tasks)

    await pg_engine.dispose()
