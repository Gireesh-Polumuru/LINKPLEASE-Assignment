import asyncio
from datetime import datetime, timedelta, timezone
import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.dm_outbox import DMOutbox, DMStatus
from app.models.rate_limit_log import RateLimitLog
from app.models.rule import Rule
from app.services.pseudogram_client import PseudoGramClient
from app.workers.reconciliation_worker import DeliveryReconciliationWorker
from tests.conftest import TestAsyncSessionLocal


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@pytest.fixture
async def sample_rule(db_session: AsyncSession) -> Rule:
    rule = Rule(
        rule_id="rule_reconcile_01",
        keyword="PROMO",
        dm_message="Promo message: https://example.com/promo",
        is_active=True,
    )
    db_session.add(rule)
    await db_session.commit()
    return rule


# ------------------------------------------------------------------------------
# 1. SENT -> DELIVERED
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_reconciliation_sent_to_delivered(
    db_session: AsyncSession,
    sample_rule: Rule,
) -> None:
    outbox = DMOutbox(
        id="outbox_rec_del",
        rule_id=sample_rule.rule_id,
        user_id="usr_rec_1",
        comment_id="cmt_rec_1",
        message=sample_rule.dm_message,
        idempotency_key="idemp_rec_del",
        status=DMStatus.SENT.value,
        dm_id="dm_ext_delivered_123",
        attempts=1,
        sent_at=utc_now() - timedelta(seconds=20),
    )
    db_session.add(outbox)
    await db_session.commit()

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/v1/dm/dm_ext_delivered_123"
        return httpx.Response(200, json={"dm_id": "dm_ext_delivered_123", "status": "delivered"})

    mock_client = PseudoGramClient(
        base_url="https://mock.test",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(mock_handler)),
    )
    worker = DeliveryReconciliationWorker(
        session_factory=TestAsyncSessionLocal,
        pseudogram_client=mock_client,
        recheck_interval=5.0,
    )

    processed = await worker.process_one_cycle()
    assert processed == 1

    # Verify outbox state
    db_session.expire_all()
    res = await db_session.execute(select(DMOutbox).where(DMOutbox.id == "outbox_rec_del"))
    updated = res.scalar_one()
    assert updated.status == DMStatus.DELIVERED.value
    assert updated.delivered_at is not None
    assert updated.last_error is None


# ------------------------------------------------------------------------------
# 2. SENT -> FAILED
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_reconciliation_sent_to_failed(
    db_session: AsyncSession,
    sample_rule: Rule,
) -> None:
    outbox = DMOutbox(
        id="outbox_rec_fail",
        rule_id=sample_rule.rule_id,
        user_id="usr_rec_2",
        comment_id="cmt_rec_2",
        message=sample_rule.dm_message,
        idempotency_key="idemp_rec_fail",
        status=DMStatus.SENT.value,
        dm_id="dm_ext_failed_123",
        attempts=1,
        sent_at=utc_now() - timedelta(seconds=20),
    )
    db_session.add(outbox)
    await db_session.commit()

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/v1/dm/dm_ext_failed_123"
        return httpx.Response(200, json={"dm_id": "dm_ext_failed_123", "status": "failed", "reason": "user_blocked_dms"})

    mock_client = PseudoGramClient(
        base_url="https://mock.test",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(mock_handler)),
    )
    worker = DeliveryReconciliationWorker(
        session_factory=TestAsyncSessionLocal,
        pseudogram_client=mock_client,
        recheck_interval=5.0,
    )

    processed = await worker.process_one_cycle()
    assert processed == 1

    db_session.expire_all()
    res = await db_session.execute(select(DMOutbox).where(DMOutbox.id == "outbox_rec_fail"))
    updated = res.scalar_one()
    assert updated.status == DMStatus.FAILED.value
    assert "user_blocked_dms" in str(updated.last_error)


# ------------------------------------------------------------------------------
# 3. Pending status remains SENT
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_reconciliation_pending_remains_sent(
    db_session: AsyncSession,
    sample_rule: Rule,
) -> None:
    outbox = DMOutbox(
        id="outbox_rec_pend",
        rule_id=sample_rule.rule_id,
        user_id="usr_rec_3",
        comment_id="cmt_rec_3",
        message=sample_rule.dm_message,
        idempotency_key="idemp_rec_pend",
        status=DMStatus.SENT.value,
        dm_id="dm_ext_pending_123",
        attempts=1,
        sent_at=utc_now() - timedelta(seconds=20),
    )
    db_session.add(outbox)
    await db_session.commit()

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/v1/dm/dm_ext_pending_123"
        return httpx.Response(200, json={"dm_id": "dm_ext_pending_123", "status": "pending"})

    mock_client = PseudoGramClient(
        base_url="https://mock.test",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(mock_handler)),
    )
    worker = DeliveryReconciliationWorker(
        session_factory=TestAsyncSessionLocal,
        pseudogram_client=mock_client,
        recheck_interval=5.0,
    )

    processed = await worker.process_one_cycle()
    assert processed == 1

    db_session.expire_all()
    res = await db_session.execute(select(DMOutbox).where(DMOutbox.id == "outbox_rec_pend"))
    updated = res.scalar_one()
    assert updated.status == DMStatus.SENT.value
    assert updated.last_reconciled_at is not None


# ------------------------------------------------------------------------------
# 4. Network timeout / server error safely preserves SENT state
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_reconciliation_network_error_preserves_sent(
    db_session: AsyncSession,
    sample_rule: Rule,
) -> None:
    outbox = DMOutbox(
        id="outbox_rec_net_err",
        rule_id=sample_rule.rule_id,
        user_id="usr_rec_4",
        comment_id="cmt_rec_4",
        message=sample_rule.dm_message,
        idempotency_key="idemp_rec_net_err",
        status=DMStatus.SENT.value,
        dm_id="dm_ext_net_err_123",
        attempts=1,
        sent_at=utc_now() - timedelta(seconds=20),
    )
    db_session.add(outbox)
    await db_session.commit()

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("Downstream timed out")

    mock_client = PseudoGramClient(
        base_url="https://mock.test",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(mock_handler)),
    )
    worker = DeliveryReconciliationWorker(
        session_factory=TestAsyncSessionLocal,
        pseudogram_client=mock_client,
        recheck_interval=5.0,
    )

    processed = await worker.process_one_cycle()
    assert processed == 1

    db_session.expire_all()
    res = await db_session.execute(select(DMOutbox).where(DMOutbox.id == "outbox_rec_net_err"))
    updated = res.scalar_one()
    assert updated.status == DMStatus.SENT.value


# ------------------------------------------------------------------------------
# 5. Missing / invalid dm_id
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_reconciliation_missing_dm_id_marks_failed(
    db_session: AsyncSession,
    sample_rule: Rule,
) -> None:
    outbox = DMOutbox(
        id="outbox_rec_no_dm_id",
        rule_id=sample_rule.rule_id,
        user_id="usr_rec_5",
        comment_id="cmt_rec_5",
        message=sample_rule.dm_message,
        idempotency_key="idemp_rec_no_dm_id",
        status=DMStatus.SENT.value,
        dm_id=None,
        attempts=1,
        sent_at=utc_now() - timedelta(seconds=20),
    )
    db_session.add(outbox)
    await db_session.commit()

    mock_client = PseudoGramClient(base_url="https://mock.test")
    worker = DeliveryReconciliationWorker(
        session_factory=TestAsyncSessionLocal,
        pseudogram_client=mock_client,
        recheck_interval=5.0,
    )

    await worker.process_one_cycle()

    db_session.expire_all()
    res = await db_session.execute(select(DMOutbox).where(DMOutbox.id == "outbox_rec_no_dm_id"))
    updated = res.scalar_one()
    assert updated.status == DMStatus.FAILED.value
    assert "Missing external dm_id" in str(updated.last_error)


# ------------------------------------------------------------------------------
# 6. Multiple SENT records
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_reconciliation_multiple_sent_records(
    db_session: AsyncSession,
    sample_rule: Rule,
) -> None:
    for i in range(5):
        outbox = DMOutbox(
            id=f"outbox_rec_multi_{i}",
            rule_id=sample_rule.rule_id,
            user_id=f"usr_multi_{i}",
            comment_id=f"cmt_multi_{i}",
            message=sample_rule.dm_message,
            idempotency_key=f"idemp_multi_{i}",
            status=DMStatus.SENT.value,
            dm_id=f"dm_multi_{i}",
            attempts=1,
            sent_at=utc_now() - timedelta(seconds=20),
        )
        db_session.add(outbox)
    await db_session.commit()

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        dm_id = request.url.path.split("/")[-1]
        return httpx.Response(200, json={"dm_id": dm_id, "status": "delivered"})

    mock_client = PseudoGramClient(
        base_url="https://mock.test",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(mock_handler)),
    )
    worker = DeliveryReconciliationWorker(
        session_factory=TestAsyncSessionLocal,
        pseudogram_client=mock_client,
        recheck_interval=5.0,
        batch_size=10,
    )

    processed = await worker.process_one_cycle()
    assert processed == 5

    db_session.expire_all()
    res = await db_session.execute(
        select(DMOutbox).where(DMOutbox.status == DMStatus.DELIVERED.value)
    )
    delivered_list = res.scalars().all()
    assert len(delivered_list) == 5


# ------------------------------------------------------------------------------
# 7. Worker ignores QUEUED, SENDING, DELIVERED, FAILED, CANCELED
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_reconciliation_ignores_non_sent_statuses(
    db_session: AsyncSession,
    sample_rule: Rule,
) -> None:
    statuses = [
        (DMStatus.QUEUED.value, "outbox_stat_queued"),
        (DMStatus.SENDING.value, "outbox_stat_sending"),
        (DMStatus.DELIVERED.value, "outbox_stat_delivered"),
        (DMStatus.FAILED.value, "outbox_stat_failed"),
        (DMStatus.CANCELED.value, "outbox_stat_canceled"),
    ]

    for stat, ob_id in statuses:
        outbox = DMOutbox(
            id=ob_id,
            rule_id=sample_rule.rule_id,
            user_id="usr_ignored",
            comment_id=f"cmt_{ob_id}",
            message=sample_rule.dm_message,
            idempotency_key=f"idemp_{ob_id}",
            status=stat,
            dm_id=f"dm_{ob_id}",
            attempts=1,
            sent_at=utc_now() - timedelta(seconds=30),
        )
        db_session.add(outbox)
    await db_session.commit()

    mock_client = PseudoGramClient(base_url="https://mock.test")
    worker = DeliveryReconciliationWorker(
        session_factory=TestAsyncSessionLocal,
        pseudogram_client=mock_client,
    )

    processed = await worker.process_one_cycle()
    assert processed == 0

    # Ensure none of the statuses changed
    db_session.expire_all()
    for stat, ob_id in statuses:
        res = await db_session.execute(select(DMOutbox).where(DMOutbox.id == ob_id))
        ob = res.scalar_one()
        assert ob.status == stat


# ------------------------------------------------------------------------------
# 8. Reconciliation GET does NOT consume rate-limit slots
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_reconciliation_does_not_consume_rate_limiter(
    db_session: AsyncSession,
    sample_rule: Rule,
) -> None:
    outbox = DMOutbox(
        id="outbox_rec_rate_test",
        rule_id=sample_rule.rule_id,
        user_id="usr_rec_rl",
        comment_id="cmt_rec_rl",
        message=sample_rule.dm_message,
        idempotency_key="idemp_rec_rl",
        status=DMStatus.SENT.value,
        dm_id="dm_ext_rl_123",
        attempts=1,
        sent_at=utc_now() - timedelta(seconds=20),
    )
    db_session.add(outbox)
    await db_session.commit()

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"dm_id": "dm_ext_rl_123", "status": "delivered"})

    mock_client = PseudoGramClient(
        base_url="https://mock.test",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(mock_handler)),
    )
    worker = DeliveryReconciliationWorker(
        session_factory=TestAsyncSessionLocal,
        pseudogram_client=mock_client,
    )

    await worker.process_one_cycle()

    # Verify zero records in rate_limit_logs
    res = await db_session.execute(select(RateLimitLog))
    logs = res.scalars().all()
    assert len(logs) == 0


# ------------------------------------------------------------------------------
# 9. Concurrent reconciliation workers do not process the same record simultaneously
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_concurrent_reconciliation_workers_no_duplicate(
    db_session: AsyncSession,
    sample_rule: Rule,
) -> None:
    for i in range(10):
        outbox = DMOutbox(
            id=f"outbox_conc_rec_{i}",
            rule_id=sample_rule.rule_id,
            user_id=f"usr_conc_rec_{i}",
            comment_id=f"cmt_conc_rec_{i}",
            message=sample_rule.dm_message,
            idempotency_key=f"idemp_conc_rec_{i}",
            status=DMStatus.SENT.value,
            dm_id=f"dm_conc_rec_{i}",
            attempts=1,
            sent_at=utc_now() - timedelta(seconds=20),
        )
        db_session.add(outbox)
    await db_session.commit()

    poll_counts: dict[str, int] = {}

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        dm_id = request.url.path.split("/")[-1]
        poll_counts[dm_id] = poll_counts.get(dm_id, 0) + 1
        return httpx.Response(200, json={"dm_id": dm_id, "status": "delivered"})

    mock_client = PseudoGramClient(
        base_url="https://mock.test",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(mock_handler)),
    )

    worker1 = DeliveryReconciliationWorker(session_factory=TestAsyncSessionLocal, pseudogram_client=mock_client)
    worker2 = DeliveryReconciliationWorker(session_factory=TestAsyncSessionLocal, pseudogram_client=mock_client)

    # Run both workers simultaneously
    await asyncio.gather(
        worker1.process_one_cycle(),
        worker2.process_one_cycle(),
    )

    # Every DM should have been polled exactly once
    for i in range(10):
        dm_id = f"dm_conc_rec_{i}"
        assert poll_counts.get(dm_id) == 1

    db_session.expire_all()
    res = await db_session.execute(
        select(DMOutbox).where(DMOutbox.status == DMStatus.DELIVERED.value)
    )
    delivered_all = res.scalars().all()
    assert len(delivered_all) == 10


# ------------------------------------------------------------------------------
# 10. Worker starts and stops cleanly
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_reconciliation_worker_lifecycle() -> None:
    worker = DeliveryReconciliationWorker(
        session_factory=TestAsyncSessionLocal,
        poll_interval=0.1,
    )
    assert worker.is_running is False
    assert worker._task is None

    worker.start()
    assert worker.is_running is True
    assert worker._task is not None

    # Let loop tick briefly
    await asyncio.sleep(0.05)

    await worker.stop()
    assert worker.is_running is False
    assert worker._task is None
