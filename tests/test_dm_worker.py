import asyncio
from datetime import datetime, timedelta, timezone
import httpx
import pytest
from typing import Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.dm_outbox import DMOutbox, DMStatus
from app.models.rule import Rule
from app.services.pseudogram_client import PseudoGramClient
from app.workers.dm_worker import DMDispatchWorker
from tests.conftest import TestAsyncSessionLocal


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ensure_utc(dt: datetime) -> datetime:
    """Ensures datetime is timezone-aware UTC for database compatibility."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class MockRateLimiter:
    """Mock rate limiter tracking acquire invocations."""

    def __init__(self):
        self.acquire_call_count = 0

    async def acquire(self, dm_outbox_id: Optional[str] = None) -> None:
        self.acquire_call_count += 1


@pytest.fixture
def mock_rate_limiter() -> MockRateLimiter:
    return MockRateLimiter()


@pytest.fixture
async def sample_rule(db_session: AsyncSession) -> Rule:
    rule = Rule(
        rule_id="rule_worker_test_01",
        keyword="DEMO",
        dm_message="Here is your demo link: https://example.com/demo",
        is_active=True,
    )
    db_session.add(rule)
    await db_session.commit()
    return rule


# ------------------------------------------------------------------------------
# Test A & B: QUEUED -> SENDING -> SENT on HTTP 202 and store dm_id
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_worker_202_accepted(
    db_session: AsyncSession,
    sample_rule: Rule,
    mock_rate_limiter: MockRateLimiter,
):
    outbox = DMOutbox(
        id="outbox_202_test",
        rule_id=sample_rule.rule_id,
        user_id="usr_buyer_202",
        comment_id="cmt_202",
        message=sample_rule.dm_message,
        idempotency_key="dm_usr_buyer_202_rule_worker_test_01_cmt_202",
        status=DMStatus.QUEUED.value,
        attempts=0,
        next_retry_at=utc_now() - timedelta(seconds=1),
    )
    db_session.add(outbox)
    await db_session.commit()

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/dm/send"
        assert request.headers.get("Idempotency-Key") == "dm_usr_buyer_202_rule_worker_test_01_cmt_202"
        return httpx.Response(202, json={"dm_id": "dm_mock_7c1f0a", "status": "queued"})

    mock_transport = httpx.MockTransport(mock_handler)
    mock_http_client = httpx.AsyncClient(transport=mock_transport)
    pg_client = PseudoGramClient(base_url="https://mock-api.test", http_client=mock_http_client)

    worker = DMDispatchWorker(
        session_factory=TestAsyncSessionLocal,
        pseudogram_client=pg_client,
        rate_limiter=mock_rate_limiter,
    )

    processed = await worker.process_one_cycle()
    assert processed is True
    assert mock_rate_limiter.acquire_call_count == 1

    # Verify outbox state
    db_session.expire_all()
    res = await db_session.execute(select(DMOutbox).where(DMOutbox.id == "outbox_202_test"))
    updated = res.scalar_one()

    assert updated.status == DMStatus.SENT.value
    assert updated.dm_id == "dm_mock_7c1f0a"
    assert updated.sent_at is not None
    assert updated.attempts == 1
    assert updated.last_error is None
    # Crucial requirement: Must NOT be marked DELIVERED yet (reconciliation handles that)
    assert updated.delivered_at is None


# ------------------------------------------------------------------------------
# Test C & D: HTTP 500 schedules retry with increasing exponential backoff
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_worker_http_500_retry_with_backoff(
    db_session: AsyncSession,
    sample_rule: Rule,
    mock_rate_limiter: MockRateLimiter,
):
    outbox = DMOutbox(
        id="outbox_500_test",
        rule_id=sample_rule.rule_id,
        user_id="usr_500",
        comment_id="cmt_500",
        message="500 test",
        idempotency_key="idemp_500_test",
        status=DMStatus.QUEUED.value,
        attempts=0,
        next_retry_at=utc_now() - timedelta(seconds=1),
    )
    db_session.add(outbox)
    await db_session.commit()

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "internal_error", "detail": "Database unavailable"})

    mock_transport = httpx.MockTransport(mock_handler)
    mock_http_client = httpx.AsyncClient(transport=mock_transport)
    pg_client = PseudoGramClient(base_url="https://mock-api.test", http_client=mock_http_client)

    worker = DMDispatchWorker(session_factory=TestAsyncSessionLocal, pseudogram_client=pg_client, rate_limiter=mock_rate_limiter)

    # Attempt 1
    await worker.process_one_cycle()
    db_session.expire_all()
    res1 = (await db_session.execute(select(DMOutbox).where(DMOutbox.id == "outbox_500_test"))).scalar_one()

    assert res1.status == DMStatus.QUEUED.value
    assert res1.attempts == 1
    assert ensure_utc(res1.next_retry_at) > utc_now()
    # Backoff for attempt 1: base ~2s (with jitter ~1.5s - 2.5s)
    delay_1 = (ensure_utc(res1.next_retry_at) - utc_now()).total_seconds()
    assert 1.0 <= delay_1 <= 4.0

    # Advance time to simulate arrival of next_retry_at and trigger Attempt 2
    res1.next_retry_at = utc_now() - timedelta(seconds=1)
    await db_session.commit()

    await worker.process_one_cycle()
    db_session.expire_all()
    res2 = (await db_session.execute(select(DMOutbox).where(DMOutbox.id == "outbox_500_test"))).scalar_one()

    assert res2.status == DMStatus.QUEUED.value
    assert res2.attempts == 2
    # Backoff for attempt 2: base ~4s (with jitter ~3.0s - 5.0s)
    delay_2 = (ensure_utc(res2.next_retry_at) - utc_now()).total_seconds()
    assert delay_2 > delay_1


# ------------------------------------------------------------------------------
# Test E: HTTP 429 respects Retry-After header & safe fallback
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_worker_http_429_respects_retry_after(
    db_session: AsyncSession,
    sample_rule: Rule,
    mock_rate_limiter: MockRateLimiter,
):
    outbox = DMOutbox(
        id="outbox_429_test",
        rule_id=sample_rule.rule_id,
        user_id="usr_429",
        comment_id="cmt_429",
        message="429 test",
        idempotency_key="idemp_429_test",
        status=DMStatus.QUEUED.value,
        attempts=0,
        next_retry_at=utc_now() - timedelta(seconds=1),
    )
    db_session.add(outbox)
    await db_session.commit()

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            429,
            json={"error": "rate_limited"},
            headers={"Retry-After": "12"},
        )

    mock_transport = httpx.MockTransport(mock_handler)
    pg_client = PseudoGramClient(base_url="https://mock-api.test", http_client=httpx.AsyncClient(transport=mock_transport))

    worker = DMDispatchWorker(session_factory=TestAsyncSessionLocal, pseudogram_client=pg_client, rate_limiter=mock_rate_limiter)

    await worker.process_one_cycle()
    db_session.expire_all()
    res = (await db_session.execute(select(DMOutbox).where(DMOutbox.id == "outbox_429_test"))).scalar_one()

    assert res.status == DMStatus.QUEUED.value
    # Next retry must be approximately 12s in future
    delay = (ensure_utc(res.next_retry_at) - utc_now()).total_seconds()
    assert 10.0 <= delay <= 13.0


@pytest.mark.asyncio
async def test_worker_http_429_missing_retry_after_fallback(
    db_session: AsyncSession,
    sample_rule: Rule,
    mock_rate_limiter: MockRateLimiter,
):
    outbox = DMOutbox(
        id="outbox_429_fallback",
        rule_id=sample_rule.rule_id,
        user_id="usr_429_fb",
        comment_id="cmt_429_fb",
        message="429 fallback test",
        idempotency_key="idemp_429_fallback",
        status=DMStatus.QUEUED.value,
        attempts=0,
        next_retry_at=utc_now() - timedelta(seconds=1),
    )
    db_session.add(outbox)
    await db_session.commit()

    # 429 without Retry-After header
    async def mock_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, json={"error": "rate_limited"})

    mock_transport = httpx.MockTransport(mock_handler)
    pg_client = PseudoGramClient(base_url="https://mock-api.test", http_client=httpx.AsyncClient(transport=mock_transport))

    worker = DMDispatchWorker(session_factory=TestAsyncSessionLocal, pseudogram_client=pg_client, rate_limiter=mock_rate_limiter)

    await worker.process_one_cycle()
    db_session.expire_all()
    res = (await db_session.execute(select(DMOutbox).where(DMOutbox.id == "outbox_429_fallback"))).scalar_one()

    assert res.status == DMStatus.QUEUED.value
    # Fallback default is 5.0s
    delay = (ensure_utc(res.next_retry_at) - utc_now()).total_seconds()
    assert 3.5 <= delay <= 6.5


# ------------------------------------------------------------------------------
# Test F: HTTP 400 becomes FAILED and is not retried
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_worker_http_400_marks_failed(
    db_session: AsyncSession,
    sample_rule: Rule,
    mock_rate_limiter: MockRateLimiter,
):
    outbox = DMOutbox(
        id="outbox_400_test",
        rule_id=sample_rule.rule_id,
        user_id="usr_400",
        comment_id="cmt_400",
        message="400 test",
        idempotency_key="idemp_400_test",
        status=DMStatus.QUEUED.value,
        attempts=0,
        next_retry_at=utc_now() - timedelta(seconds=1),
    )
    db_session.add(outbox)
    await db_session.commit()

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"error": "invalid_request", "detail": "Recipient user does not exist or has DMs disabled"})

    mock_transport = httpx.MockTransport(mock_handler)
    pg_client = PseudoGramClient(base_url="https://mock-api.test", http_client=httpx.AsyncClient(transport=mock_transport))

    worker = DMDispatchWorker(session_factory=TestAsyncSessionLocal, pseudogram_client=pg_client, rate_limiter=mock_rate_limiter)

    # Process cycle
    await worker.process_one_cycle()
    db_session.expire_all()
    res = (await db_session.execute(select(DMOutbox).where(DMOutbox.id == "outbox_400_test"))).scalar_one()

    assert res.status == DMStatus.FAILED.value
    assert "Recipient user does not exist" in res.last_error

    # Next cycle should find NO work because FAILED records are not eligible
    assert await worker.process_one_cycle() is False


# ------------------------------------------------------------------------------
# Test G & H: Network timeout schedules retry and reuses SAME Idempotency-Key
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_worker_network_timeout_and_idempotency_key_reuse(
    db_session: AsyncSession,
    sample_rule: Rule,
    mock_rate_limiter: MockRateLimiter,
):
    stable_key = "stable_idempotency_key_xyz_123"
    outbox = DMOutbox(
        id="outbox_timeout_test",
        rule_id=sample_rule.rule_id,
        user_id="usr_timeout",
        comment_id="cmt_timeout",
        message="timeout test",
        idempotency_key=stable_key,
        status=DMStatus.QUEUED.value,
        attempts=0,
        next_retry_at=utc_now() - timedelta(seconds=1),
    )
    db_session.add(outbox)
    await db_session.commit()

    call_count = 0
    received_keys = []

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        received_keys.append(request.headers.get("Idempotency-Key"))
        if call_count == 1:
            # First attempt: simulate network timeout
            raise httpx.ReadTimeout("Connection timed out after PseudoGram received request")
        # Second attempt: success
        return httpx.Response(202, json={"dm_id": "dm_retry_success", "status": "queued"})

    mock_transport = httpx.MockTransport(mock_handler)
    pg_client = PseudoGramClient(base_url="https://mock-api.test", http_client=httpx.AsyncClient(transport=mock_transport))

    worker = DMDispatchWorker(session_factory=TestAsyncSessionLocal, pseudogram_client=pg_client, rate_limiter=mock_rate_limiter)

    # 1. First attempt (fails with timeout)
    await worker.process_one_cycle()
    db_session.expire_all()
    res1 = (await db_session.execute(select(DMOutbox).where(DMOutbox.id == "outbox_timeout_test"))).scalar_one()

    assert res1.status == DMStatus.QUEUED.value
    assert res1.attempts == 1

    # 2. Advance next_retry_at and retry
    res1.next_retry_at = utc_now() - timedelta(seconds=1)
    await db_session.commit()

    # 3. Second attempt (succeeds)
    await worker.process_one_cycle()
    db_session.expire_all()
    res2 = (await db_session.execute(select(DMOutbox).where(DMOutbox.id == "outbox_timeout_test"))).scalar_one()

    assert res2.status == DMStatus.SENT.value
    assert res2.dm_id == "dm_retry_success"
    assert res2.attempts == 2

    # Verify both attempts used the EXACT SAME Idempotency-Key
    assert len(received_keys) == 2
    assert received_keys[0] == stable_key
    assert received_keys[1] == stable_key


# ------------------------------------------------------------------------------
# Test I: Concurrent worker claiming (Two workers compete for 1 job)
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_concurrent_worker_claiming_no_duplicate_dispatch(
    db_session: AsyncSession,
    sample_rule: Rule,
    mock_rate_limiter: MockRateLimiter,
):
    outbox = DMOutbox(
        id="outbox_compete_test",
        rule_id=sample_rule.rule_id,
        user_id="usr_compete",
        comment_id="cmt_compete",
        message="compete test",
        idempotency_key="idemp_compete_test",
        status=DMStatus.QUEUED.value,
        attempts=0,
        next_retry_at=utc_now() - timedelta(seconds=1),
    )
    db_session.add(outbox)
    await db_session.commit()

    send_counter = 0

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        nonlocal send_counter
        send_counter += 1
        await asyncio.sleep(0.05)  # Simulate small network delay
        return httpx.Response(202, json={"dm_id": f"dm_compete_{send_counter}", "status": "queued"})

    mock_transport = httpx.MockTransport(mock_handler)
    pg_client = PseudoGramClient(base_url="https://mock-api.test", http_client=httpx.AsyncClient(transport=mock_transport))

    worker1 = DMDispatchWorker(session_factory=TestAsyncSessionLocal, pseudogram_client=pg_client, rate_limiter=mock_rate_limiter)
    worker2 = DMDispatchWorker(session_factory=TestAsyncSessionLocal, pseudogram_client=pg_client, rate_limiter=mock_rate_limiter)

    # Run both workers simultaneously
    r1, r2 = await asyncio.gather(
        worker1.process_one_cycle(),
        worker2.process_one_cycle(),
    )

    # Exactly one worker should claim the job, the other should find no work
    assert (r1, r2) in [(True, False), (False, True)]
    assert send_counter == 1


# ------------------------------------------------------------------------------
# Test J: Stale SENDING lease recovery after worker crash simulation
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_stale_sending_recovery(
    db_session: AsyncSession,
    sample_rule: Rule,
    mock_rate_limiter: MockRateLimiter,
):
    # Simulate a job that was claimed 120s ago by a worker that died
    stale_time = utc_now() - timedelta(seconds=120)
    stale_outbox = DMOutbox(
        id="outbox_stale_test",
        rule_id=sample_rule.rule_id,
        user_id="usr_stale",
        comment_id="cmt_stale",
        message="stale recovery test",
        idempotency_key="idemp_stale_test",
        status=DMStatus.SENDING.value,  # Stuck in SENDING
        attempts=1,
        updated_at=stale_time,
    )
    db_session.add(stale_outbox)
    await db_session.commit()

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(202, json={"dm_id": "dm_recovered_ok", "status": "queued"})

    mock_transport = httpx.MockTransport(mock_handler)
    pg_client = PseudoGramClient(base_url="https://mock-api.test", http_client=httpx.AsyncClient(transport=mock_transport))

    # Configure lease_seconds = 60s
    worker = DMDispatchWorker(session_factory=TestAsyncSessionLocal, pseudogram_client=pg_client, rate_limiter=mock_rate_limiter, lease_seconds=60)

    # The cycle will recover the stale job to QUEUED and then claim & dispatch it
    processed = await worker.process_one_cycle()
    assert processed is True

    db_session.expire_all()
    res = (await db_session.execute(select(DMOutbox).where(DMOutbox.id == "outbox_stale_test"))).scalar_one()
    assert res.status == DMStatus.SENT.value
    assert res.dm_id == "dm_recovered_ok"


# ------------------------------------------------------------------------------
# Test K: No delivery is silently lost (Transitions to FAILED at max_attempts)
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_max_retries_transitions_to_failed(
    db_session: AsyncSession,
    sample_rule: Rule,
    mock_rate_limiter: MockRateLimiter,
):
    # Job at attempt 4 with max_attempts 5
    outbox = DMOutbox(
        id="outbox_max_retries_test",
        rule_id=sample_rule.rule_id,
        user_id="usr_max",
        comment_id="cmt_max",
        message="max retries test",
        idempotency_key="idemp_max_test",
        status=DMStatus.QUEUED.value,
        attempts=4,
        max_attempts=5,
        next_retry_at=utc_now() - timedelta(seconds=1),
    )
    db_session.add(outbox)
    await db_session.commit()

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "persistent_500"})

    mock_transport = httpx.MockTransport(mock_handler)
    pg_client = PseudoGramClient(base_url="https://mock-api.test", http_client=httpx.AsyncClient(transport=mock_transport))

    worker = DMDispatchWorker(session_factory=TestAsyncSessionLocal, pseudogram_client=pg_client, rate_limiter=mock_rate_limiter)

    # Attempt 5 will execute and fail
    await worker.process_one_cycle()
    db_session.expire_all()
    res = (await db_session.execute(select(DMOutbox).where(DMOutbox.id == "outbox_max_retries_test"))).scalar_one()

    assert res.attempts == 5
    assert res.status == DMStatus.FAILED.value
    assert "Max retry attempts exceeded (5/5)" in res.last_error


# ------------------------------------------------------------------------------
# Test L: Multiple queued deliveries processed
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_multiple_queued_deliveries_processed(
    db_session: AsyncSession,
    sample_rule: Rule,
    mock_rate_limiter: MockRateLimiter,
):
    for i in range(3):
        outbox = DMOutbox(
            id=f"outbox_batch_{i}",
            rule_id=sample_rule.rule_id,
            user_id=f"usr_batch_{i}",
            comment_id=f"cmt_batch_{i}",
            message=f"batch msg {i}",
            idempotency_key=f"idemp_batch_{i}",
            status=DMStatus.QUEUED.value,
            attempts=0,
            next_retry_at=utc_now() - timedelta(seconds=1),
        )
        db_session.add(outbox)
    await db_session.commit()

    sent_ids = []

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        key = request.headers.get("Idempotency-Key")
        sent_ids.append(key)
        return httpx.Response(202, json={"dm_id": f"dm_{key}", "status": "queued"})

    mock_transport = httpx.MockTransport(mock_handler)
    pg_client = PseudoGramClient(base_url="https://mock-api.test", http_client=httpx.AsyncClient(transport=mock_transport))

    worker = DMDispatchWorker(session_factory=TestAsyncSessionLocal, pseudogram_client=pg_client, rate_limiter=mock_rate_limiter)

    # Process all 3 items
    for _ in range(3):
        assert await worker.process_one_cycle() is True

    assert len(sent_ids) == 3
    # 4th cycle finds no work
    assert await worker.process_one_cycle() is False


# ------------------------------------------------------------------------------
# Test M & N: Ignores CANCELED, SENT, DELIVERED, FAILED deliveries
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_worker_ignores_terminal_and_ineligible_statuses(
    db_session: AsyncSession,
    sample_rule: Rule,
    mock_rate_limiter: MockRateLimiter,
):
    statuses = [
        DMStatus.CANCELED.value,
        DMStatus.SENT.value,
        DMStatus.DELIVERED.value,
        DMStatus.FAILED.value,
    ]
    for idx, st in enumerate(statuses):
        outbox = DMOutbox(
            id=f"outbox_ineligible_{idx}",
            rule_id=sample_rule.rule_id,
            user_id=f"usr_inel_{idx}",
            comment_id=f"cmt_inel_{idx}",
            message=f"msg {st}",
            idempotency_key=f"idemp_inel_{idx}",
            status=st,
            attempts=1,
            next_retry_at=utc_now() - timedelta(seconds=1),
        )
        db_session.add(outbox)
    await db_session.commit()

    called = False

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        nonlocal called
        called = True
        return httpx.Response(202, json={"dm_id": "should_not_happen"})

    mock_transport = httpx.MockTransport(mock_handler)
    pg_client = PseudoGramClient(base_url="https://mock-api.test", http_client=httpx.AsyncClient(transport=mock_transport))

    worker = DMDispatchWorker(session_factory=TestAsyncSessionLocal, pseudogram_client=pg_client, rate_limiter=mock_rate_limiter)

    assert await worker.process_one_cycle() is False
    assert called is False


# ------------------------------------------------------------------------------
# Test O: Verifies worker does NOT call GET /v1/dm/{dm_id} (Reconciliation)
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_worker_never_calls_get_dm_endpoint(
    db_session: AsyncSession,
    sample_rule: Rule,
    mock_rate_limiter: MockRateLimiter,
):
    outbox = DMOutbox(
        id="outbox_no_reconcile_test",
        rule_id=sample_rule.rule_id,
        user_id="usr_no_reconcile",
        comment_id="cmt_no_reconcile",
        message="no reconcile test",
        idempotency_key="idemp_no_reconcile",
        status=DMStatus.QUEUED.value,
        attempts=0,
        next_retry_at=utc_now() - timedelta(seconds=1),
    )
    db_session.add(outbox)
    await db_session.commit()

    called_paths = []

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        called_paths.append((request.method, request.url.path))
        return httpx.Response(202, json={"dm_id": "dm_dispatch_only", "status": "queued"})

    mock_transport = httpx.MockTransport(mock_handler)
    pg_client = PseudoGramClient(base_url="https://mock-api.test", http_client=httpx.AsyncClient(transport=mock_transport))

    worker = DMDispatchWorker(session_factory=TestAsyncSessionLocal, pseudogram_client=pg_client, rate_limiter=mock_rate_limiter)

    await worker.process_one_cycle()

    assert len(called_paths) == 1
    method, path = called_paths[0]
    assert method == "POST"
    assert path == "/v1/dm/send"
    # Never called GET
    assert not any(m == "GET" for m, _ in called_paths)
