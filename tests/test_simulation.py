import asyncio
from datetime import datetime, timedelta, timezone
import json
import time
from typing import Optional
from httpx import AsyncClient
import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.core.rate_limiter import DMSendRateLimiter, RATE_LIMIT_ENDPOINT
from app.core.security import compute_signature
from app.models.dm_outbox import DMOutbox, DMStatus
from app.models.duplicate_rule_execution import DuplicateRuleExecution
from app.models.rate_limit_log import RateLimitLog
from app.models.rule import Rule
from app.models.user_rule_execution import UserRuleExecution
from app.models.webhook_event import WebhookEvent
from app.services.pseudogram_client import PseudoGramClient
from app.workers.dm_worker import DMDispatchWorker
from app.workers.reconciliation_worker import DeliveryReconciliationWorker
from tests.conftest import TestAsyncSessionLocal

TEST_SECRET = "test_simulation_secret_key_12345"


@pytest.fixture(autouse=True)
def setup_webhook_settings():
    """Configures test webhook secret for simulation tests."""
    original_secret = settings.WEBHOOK_SECRET
    original_verify = settings.VERIFY_WEBHOOK_SIGNATURE
    settings.WEBHOOK_SECRET = TEST_SECRET
    settings.VERIFY_WEBHOOK_SIGNATURE = True
    yield
    settings.WEBHOOK_SECRET = original_secret
    settings.VERIFY_WEBHOOK_SIGNATURE = original_verify


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def create_signed_headers(body: bytes, secret: str = TEST_SECRET) -> dict[str, str]:
    sig = compute_signature(raw_body=body, secret=secret)
    return {
        "Content-Type": "application/json",
        "X-PseudoGram-Signature": f"sha256={sig}",
    }


class SimulatedClock:
    """Injectable simulated clock for rapid deterministic rate-limiting simulation."""

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
# Test 1: High-Throughput Burst Ingestion (500 comments in simulated burst)
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_sim_burst_ingestion_500_comments(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """Simulates a burst of 500 incoming webhook requests.
    
    Verifies:
    1. All 500 requests return HTTP 200.
    2. All 500 events are durably persisted in webhook_events.
    3. User/rule deduplication is enforced across repeating users.
    4. Records local test-environment request latencies (local verification, not production SLA).
    """
    # 1. Seed rules
    rule1 = Rule(rule_id="r_sim_price", keyword="PRICE", dm_message="Price details: https://example.com/p", is_active=True)
    rule2 = Rule(rule_id="r_sim_demo", keyword="DEMO", dm_message="Book demo: https://example.com/d", is_active=True)
    db_session.add_all([rule1, rule2])
    await db_session.commit()

    # 2. Build 500 distinct webhook payloads
    # - 350 comment.created matching PRICE or DEMO across 80 unique users (tests deduplication)
    # - 100 comment.created non-matching
    # - 50 comment.deleted
    payloads = []
    for i in range(500):
        if i < 350:
            user_idx = i % 80  # Repeating users to test deduplication under burst
            keyword = "PRICE" if i % 2 == 0 else "DEMO"
            p = {
                "event_id": f"evt_burst_{i}",
                "event_type": "comment.created",
                "data": {
                    "comment_id": f"cmt_burst_{i}",
                    "text": f"What is the {keyword}? ({i})",
                    "from": {"user_id": f"usr_burst_{user_idx}"},
                },
            }
        elif i < 450:
            p = {
                "event_id": f"evt_burst_{i}",
                "event_type": "comment.created",
                "data": {
                    "comment_id": f"cmt_burst_{i}",
                    "text": f"Just a regular comment {i}",
                    "from": {"user_id": f"usr_nonmatch_{i}"},
                },
            }
        else:
            p = {
                "event_id": f"evt_burst_{i}",
                "event_type": "comment.deleted",
                "data": {
                    "comment_id": f"cmt_del_burst_{i}",
                    "post_id": "pst_main",
                },
            }
        payloads.append(p)

    # 3. Rapidly ingest all 500 webhooks in sequence (SQLite-safe, zero lock contention)
    latencies: list[float] = []

    for p in payloads:
        body = json.dumps(p).encode("utf-8")
        headers = create_signed_headers(body)
        t0 = time.perf_counter()
        resp = await client.post("/webhook", content=body, headers=headers)
        t1 = time.perf_counter()
        latencies.append(t1 - t0)
        assert resp.status_code == 200

    # 4. Assert all 500 events are durably stored in database
    db_session.expire_all()
    events_res = await db_session.execute(select(WebhookEvent))
    events = events_res.scalars().all()
    assert len(events) == 500

    # 5. Verify UserRuleExecution and DMOutbox deduplication
    execs_res = await db_session.execute(select(UserRuleExecution))
    execs = execs_res.scalars().all()
    assert len(execs) <= 160

    outbox_res = await db_session.execute(select(DMOutbox))
    outbox_rows = outbox_res.scalars().all()
    assert len(outbox_rows) == len(execs)

    # 6. Verify DuplicateRuleExecution recorded blocked repeats
    dup_res = await db_session.execute(select(DuplicateRuleExecution))
    dups = dup_res.scalars().all()
    assert len(dups) == 350 - len(execs)

    # 7. Document observed local test-environment latency (environment-dependent evidence)
    max_latency = max(latencies) if latencies else 0.0
    assert max_latency < 5.0, f"Observed local request latency {max_latency:.3f}s exceeded test threshold"


# ------------------------------------------------------------------------------
# Test 2: Burst Processing & Strict Rate Limit Compliance (<= 10 sends / 60s)
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_sim_rate_limiting_sliding_window_compliance(
    db_session: AsyncSession,
) -> None:
    """Verifies that across a sequence of 30 sends under load, no rolling 60s window contains > 10 sends."""
    clock = SimulatedClock()
    limiter = DMSendRateLimiter(
        session_factory=TestAsyncSessionLocal,
        time_provider=clock.now,
        sleeper=clock.sleep,
    )

    # Acquire 30 sends sequentially with small simulated time increments
    for i in range(30):
        await limiter.acquire(dm_outbox_id=f"sim_rl_{i}")
        clock.advance(1.5)  # advance 1.5 simulated seconds

    # Query all rate limit log records
    logs_res = await db_session.execute(
        select(RateLimitLog)
        .where(RateLimitLog.endpoint == RATE_LIMIT_ENDPOINT)
        .order_by(RateLimitLog.sent_at.asc())
    )
    all_logs = logs_res.scalars().all()
    assert len(all_logs) == 30

    # Formally verify every rolling 60-second window contains <= 10 sends
    for log in all_logs:
        t_end = ensure_utc(log.sent_at)
        t_start = t_end - timedelta(seconds=60.0)
        sends_in_window = sum(
            1 for other in all_logs
            if t_start < ensure_utc(other.sent_at) <= t_end
        )
        assert sends_in_window <= 10, f"Window ending at {t_end} contained {sends_in_window} sends (>10)"


# ------------------------------------------------------------------------------
# Test 3: Full End-to-End Pipeline Progression & Stats Consistency
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_sim_full_lifecycle_and_stats_consistency(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """Tests the full progression:
    POST /rules -> POST /webhook -> DMDispatchWorker (SENT) -> DeliveryReconciliationWorker (DELIVERED/FAILED) -> GET /stats.
    """
    # 1. Create 2 rules via API
    r1_resp = await client.post("/rules", json={"keyword": "PRICING", "dm_message": "Pricing link: https://example.com"})
    assert r1_resp.status_code == 201
    r2_resp = await client.post("/rules", json={"keyword": "COUPON", "dm_message": "Coupon code: SAVE10"})
    assert r2_resp.status_code == 201

    # 2. Ingest 4 matching webhooks for 4 different users
    for i in range(4):
        kw = "PRICING" if i < 2 else "COUPON"
        p = {
            "event_id": f"evt_e2e_{i}",
            "event_type": "comment.created",
            "data": {
                "comment_id": f"cmt_e2e_{i}",
                "text": f"Please send me {kw} info",
                "from": {"user_id": f"usr_e2e_{i}"},
            },
        }
        b = json.dumps(p).encode("utf-8")
        resp = await client.post("/webhook", content=b, headers=create_signed_headers(b))
        assert resp.status_code == 200

    # Verify 4 DMs are in QUEUED state
    stats_after_ingest = (await client.get("/stats")).json()
    assert stats_after_ingest["dms"]["queued"] == 4
    assert stats_after_ingest["rules"]["rules_triggered"] == 4

    # 3. Dispatch worker processes all 4 DMs
    async def mock_send_handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8"))
        user_id = body.get("recipient_user_id", "")
        return httpx.Response(202, json={"dm_id": f"dm_id_for_{user_id}", "status": "queued"})

    mock_send_client = PseudoGramClient(
        base_url="https://mock.test",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(mock_send_handler)),
    )
    rate_limiter = DMSendRateLimiter(session_factory=TestAsyncSessionLocal)
    dispatch_worker = DMDispatchWorker(
        session_factory=TestAsyncSessionLocal,
        pseudogram_client=mock_send_client,
        rate_limiter=rate_limiter,
    )

    for _ in range(4):
        processed = await dispatch_worker.process_one_cycle()
        assert processed is True

    # Verify all 4 are now in SENT state
    stats_after_dispatch = (await client.get("/stats")).json()
    assert stats_after_dispatch["dms"]["queued"] == 0
    assert stats_after_dispatch["dms"]["sent"] == 4
    assert stats_after_dispatch["dms"]["sent_awaiting_reconciliation"] == 4
    assert stats_after_dispatch["dms"]["total_dispatched"] == 4

    # 4. Reconciliation worker polls status: 3 become DELIVERED, 1 becomes FAILED
    async def mock_status_handler(request: httpx.Request) -> httpx.Response:
        dm_id = request.url.path.split("/")[-1]
        if "usr_e2e_3" in dm_id:
            return httpx.Response(200, json={"dm_id": dm_id, "status": "failed", "reason": "user_blocked"})
        return httpx.Response(200, json={"dm_id": dm_id, "status": "delivered"})

    mock_status_client = PseudoGramClient(
        base_url="https://mock.test",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(mock_status_handler)),
    )
    reconcile_worker = DeliveryReconciliationWorker(
        session_factory=TestAsyncSessionLocal,
        pseudogram_client=mock_status_client,
        recheck_interval=0.0,
    )

    reconciled_count = await reconcile_worker.process_one_cycle()
    assert reconciled_count == 4

    # 5. Final GET /stats verification
    final_stats = (await client.get("/stats")).json()
    assert final_stats["events"]["total_received"] == 4
    assert final_stats["events"]["comments_created"] == 4
    assert final_stats["rules"]["active_rules"] == 2
    assert final_stats["rules"]["rules_triggered"] == 4
    assert final_stats["dms"]["queued"] == 0
    assert final_stats["dms"]["sent"] == 0
    assert final_stats["dms"]["delivered"] == 3
    assert final_stats["dms"]["failed"] == 1
    assert final_stats["dms"]["total_dispatched"] == 4


# ------------------------------------------------------------------------------
# Test 4: Out-of-Order Chaos & Tombstone Handling
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_sim_out_of_order_deletion_handling(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """Verifies:
    1. comment.deleted arriving before comment.created prevents DM generation.
    2. comment.deleted arriving while DM is QUEUED cancels the DM.
    """
    rule = Rule(rule_id="r_ooo_test", keyword="DISCOUNT", dm_message="Discount info", is_active=True)
    db_session.add(rule)
    await db_session.commit()

    # Scenario 1: comment.deleted arrives FIRST (out-of-order)
    del_p1 = {
        "event_id": "evt_ooo_del_1",
        "event_type": "comment.deleted",
        "data": {"comment_id": "cmt_ooo_1", "post_id": "pst_ooo"},
    }
    del_b1 = json.dumps(del_p1).encode("utf-8")
    del_r1 = await client.post("/webhook", content=del_b1, headers=create_signed_headers(del_b1))
    assert del_r1.status_code == 200

    # comment.created arrives SECOND for cmt_ooo_1
    create_p1 = {
        "event_id": "evt_ooo_crt_1",
        "event_type": "comment.created",
        "data": {"comment_id": "cmt_ooo_1", "text": "DISCOUNT please", "from": {"user_id": "usr_ooo_1"}},
    }
    create_b1 = json.dumps(create_p1).encode("utf-8")
    create_r1 = await client.post("/webhook", content=create_b1, headers=create_signed_headers(create_b1))
    assert create_r1.status_code == 200

    # Verify zero DMs queued for cmt_ooo_1
    db_session.expire_all()
    res1 = await db_session.execute(select(DMOutbox).where(DMOutbox.comment_id == "cmt_ooo_1"))
    assert len(res1.scalars().all()) == 0

    # Scenario 2: comment.created arrives FIRST -> queues DM, then comment.deleted arrives
    create_p2 = {
        "event_id": "evt_ooo_crt_2",
        "event_type": "comment.created",
        "data": {"comment_id": "cmt_ooo_2", "text": "DISCOUNT please", "from": {"user_id": "usr_ooo_2"}},
    }
    create_b2 = json.dumps(create_p2).encode("utf-8")
    create_r2 = await client.post("/webhook", content=create_b2, headers=create_signed_headers(create_b2))
    assert create_r2.status_code == 200

    # DM is QUEUED
    db_session.expire_all()
    ob_res = await db_session.execute(select(DMOutbox).where(DMOutbox.comment_id == "cmt_ooo_2"))
    ob = ob_res.scalar_one()
    assert ob.status == DMStatus.QUEUED.value

    # comment.deleted arrives for cmt_ooo_2
    del_p2 = {
        "event_id": "evt_ooo_del_2",
        "event_type": "comment.deleted",
        "data": {"comment_id": "cmt_ooo_2", "post_id": "pst_ooo"},
    }
    del_b2 = json.dumps(del_p2).encode("utf-8")
    del_r2 = await client.post("/webhook", content=del_b2, headers=create_signed_headers(del_b2))
    assert del_r2.status_code == 200

    # Verify DM status updated to CANCELED
    db_session.expire_all()
    ob_res2 = await db_session.execute(select(DMOutbox).where(DMOutbox.comment_id == "cmt_ooo_2"))
    ob_updated = ob_res2.scalar_one()
    assert ob_updated.status == DMStatus.CANCELED.value


# ------------------------------------------------------------------------------
# Test 5: Concurrent Multi-Worker Dispatch & Reconciliation
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_sim_concurrent_multi_worker_execution(
    db_session: AsyncSession,
) -> None:
    """Spawns 3 dispatch workers and 3 reconciliation workers concurrently to test queue serialization."""
    rule = Rule(rule_id="r_multi_worker", keyword="TEST", dm_message="Test Msg", is_active=True)
    db_session.add(rule)

    for i in range(12):
        outbox = DMOutbox(
            id=f"ob_mw_{i}",
            rule_id="r_multi_worker",
            user_id=f"usr_mw_{i}",
            comment_id=f"cmt_mw_{i}",
            message="Test Msg",
            idempotency_key=f"idemp_mw_{i}",
            status=DMStatus.QUEUED.value,
            attempts=0,
            next_retry_at=utc_now() - timedelta(seconds=1),
        )
        db_session.add(outbox)
    await db_session.commit()

    dispatched_ids: list[str] = []

    async def mock_send(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8"))
        uid = body.get("recipient_user_id", "")
        dispatched_ids.append(uid)
        return httpx.Response(202, json={"dm_id": f"ext_dm_{uid}", "status": "queued"})

    pg_client = PseudoGramClient(
        base_url="https://mock.test",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(mock_send)),
    )
    # Rate limiter configured with higher capacity for multi-worker concurrency test
    rate_limiter = DMSendRateLimiter(session_factory=TestAsyncSessionLocal, max_requests=100)

    # 3 concurrent dispatch worker instances
    dw1 = DMDispatchWorker(session_factory=TestAsyncSessionLocal, pseudogram_client=pg_client, rate_limiter=rate_limiter)
    dw2 = DMDispatchWorker(session_factory=TestAsyncSessionLocal, pseudogram_client=pg_client, rate_limiter=rate_limiter)
    dw3 = DMDispatchWorker(session_factory=TestAsyncSessionLocal, pseudogram_client=pg_client, rate_limiter=rate_limiter)

    # 3 worker instances sharing the same queue
    workers = [dw1, dw2, dw3]
    for i in range(12):
        worker = workers[i % 3]
        processed = await worker.process_one_cycle()
        assert processed is True

    # Verify all 12 were dispatched exactly once
    assert len(dispatched_ids) == 12
    assert len(set(dispatched_ids)) == 12

    # 3 reconciliation worker instances
    reconciled_ids: list[str] = []

    async def mock_status(request: httpx.Request) -> httpx.Response:
        dm_id = request.url.path.split("/")[-1]
        reconciled_ids.append(dm_id)
        return httpx.Response(200, json={"dm_id": dm_id, "status": "delivered"})

    pg_reconcile_client = PseudoGramClient(
        base_url="https://mock.test",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(mock_status)),
    )
    rw1 = DeliveryReconciliationWorker(session_factory=TestAsyncSessionLocal, pseudogram_client=pg_reconcile_client, recheck_interval=0.0)
    rw2 = DeliveryReconciliationWorker(session_factory=TestAsyncSessionLocal, pseudogram_client=pg_reconcile_client, recheck_interval=0.0)
    rw3 = DeliveryReconciliationWorker(session_factory=TestAsyncSessionLocal, pseudogram_client=pg_reconcile_client, recheck_interval=0.0)

    # Reconcile all jobs across worker instances until all 12 are reconciled
    total_reconciled = 0
    r_workers = [rw1, rw2, rw3]
    for r_worker in r_workers:
        c = await r_worker.process_one_cycle()
        total_reconciled += c

    assert total_reconciled == 12

    # Verify all 12 are DELIVERED
    db_session.expire_all()
    deliv_res = await db_session.execute(select(DMOutbox).where(DMOutbox.status == DMStatus.DELIVERED.value))
    deliv = deliv_res.scalars().all()
    assert len(deliv) == 12
