import asyncio
from datetime import datetime, timedelta, timezone
import json
from httpx import AsyncClient
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.core.security import compute_signature
from app.models.dm_outbox import DMOutbox, DMStatus
from app.models.duplicate_rule_execution import DuplicateRuleExecution
from app.models.rate_limit_log import RateLimitLog
from app.models.rule import Rule
from app.models.user_rule_execution import UserRuleExecution
from app.models.webhook_event import WebhookEvent
from tests.conftest import TestAsyncSessionLocal

TEST_SECRET = "test_pseudogram_secret_key_12345"


@pytest.fixture(autouse=True)
def setup_webhook_settings():
    """Configures test webhook secret for all tests."""
    original_secret = settings.WEBHOOK_SECRET
    original_verify = settings.VERIFY_WEBHOOK_SIGNATURE
    settings.WEBHOOK_SECRET = TEST_SECRET
    settings.VERIFY_WEBHOOK_SIGNATURE = True
    yield
    settings.WEBHOOK_SECRET = original_secret
    settings.VERIFY_WEBHOOK_SIGNATURE = original_verify


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def create_signed_headers(body: bytes, secret: str = TEST_SECRET) -> dict[str, str]:
    sig = compute_signature(raw_body=body, secret=secret)
    return {
        "Content-Type": "application/json",
        "X-PseudoGram-Signature": f"sha256={sig}",
    }


# ------------------------------------------------------------------------------
# Test A & Q & R: Empty database returns correct zero stats, HTTP 200, exact fields
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_a_q_r_empty_database_zero_stats(client: AsyncClient) -> None:
    response = await client.get("/stats")
    assert response.status_code == 200
    data = response.json()

    # Exact required top-level sections
    assert set(data.keys()) == {"events", "rules", "dms", "rate_limiter"}

    # Events section
    assert data["events"]["total_received"] == 0
    assert data["events"]["unique_processed"] == 0
    assert data["events"]["duplicates_ignored"] == 0
    assert data["events"]["comments_created"] == 0
    assert data["events"]["comments_deleted"] == 0

    # Rules section
    assert data["rules"]["active_rules"] == 0
    assert data["rules"]["rules_triggered"] == 0
    assert data["rules"]["duplicate_executions_blocked"] == 0

    # DMs section
    assert data["dms"]["queued"] == 0
    assert data["dms"]["sending"] == 0
    assert data["dms"]["sent"] == 0
    assert data["dms"]["sent_awaiting_reconciliation"] == 0
    assert data["dms"]["delivered"] == 0
    assert data["dms"]["failed"] == 0
    assert data["dms"]["canceled"] == 0
    assert data["dms"]["total_dispatched"] == 0

    # Rate limiter section
    assert data["rate_limiter"]["sends_last_60s"] == 0
    assert data["rate_limiter"]["send_limit"] == 10
    assert data["rate_limiter"]["tokens_available"] == 10
    assert data["rate_limiter"]["retry_after_seconds"] == 0.0
    assert data["rate_limiter"]["window_seconds"] == 60


# ------------------------------------------------------------------------------
# Test B: Webhook event counts are calculated correctly
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_b_webhook_event_counts(client: AsyncClient, db_session: AsyncSession) -> None:
    # Insert 3 comment.created, 2 comment.deleted, 1 unsupported
    e1 = WebhookEvent(event_id="evt_1", event_type="comment.created", status="PROCESSED")
    e2 = WebhookEvent(event_id="evt_2", event_type="comment.created", status="PROCESSED")
    e3 = WebhookEvent(event_id="evt_3", event_type="comment.created", status="PROCESSED")
    e4 = WebhookEvent(event_id="evt_4", event_type="comment.deleted", status="PROCESSED")
    e5 = WebhookEvent(event_id="evt_5", event_type="comment.deleted", status="PROCESSED")
    e6 = WebhookEvent(event_id="evt_6", event_type="reaction.created", status="IGNORED")
    db_session.add_all([e1, e2, e3, e4, e5, e6])
    await db_session.commit()

    resp = await client.get("/stats")
    assert resp.status_code == 200
    events = resp.json()["events"]
    assert events["total_received"] == 6
    assert events["comments_created"] == 3
    assert events["comments_deleted"] == 2
    assert events["unique_processed"] == 5


# ------------------------------------------------------------------------------
# Tests C to H: DMOutbox counts for all individual states
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_c_to_h_dm_outbox_state_counts(client: AsyncClient, db_session: AsyncSession) -> None:
    rule = Rule(rule_id="r_stats_dm", keyword="KEY", dm_message="Msg", is_active=True)
    db_session.add(rule)

    dms = [
        # 3 QUEUED
        DMOutbox(id="ob_q1", rule_id="r_stats_dm", user_id="u1", comment_id="c1", message="M", idempotency_key="k_q1", status=DMStatus.QUEUED.value),
        DMOutbox(id="ob_q2", rule_id="r_stats_dm", user_id="u2", comment_id="c2", message="M", idempotency_key="k_q2", status=DMStatus.QUEUED.value),
        DMOutbox(id="ob_q3", rule_id="r_stats_dm", user_id="u3", comment_id="c3", message="M", idempotency_key="k_q3", status=DMStatus.QUEUED.value),
        # 2 SENDING
        DMOutbox(id="ob_s1", rule_id="r_stats_dm", user_id="u4", comment_id="c4", message="M", idempotency_key="k_s1", status=DMStatus.SENDING.value),
        DMOutbox(id="ob_s2", rule_id="r_stats_dm", user_id="u5", comment_id="c5", message="M", idempotency_key="k_s2", status=DMStatus.SENDING.value),
        # 4 SENT
        DMOutbox(id="ob_st1", rule_id="r_stats_dm", user_id="u6", comment_id="c6", message="M", idempotency_key="k_st1", status=DMStatus.SENT.value, dm_id="dm_1"),
        DMOutbox(id="ob_st2", rule_id="r_stats_dm", user_id="u7", comment_id="c7", message="M", idempotency_key="k_st2", status=DMStatus.SENT.value, dm_id="dm_2"),
        DMOutbox(id="ob_st3", rule_id="r_stats_dm", user_id="u8", comment_id="c8", message="M", idempotency_key="k_st3", status=DMStatus.SENT.value, dm_id="dm_3"),
        DMOutbox(id="ob_st4", rule_id="r_stats_dm", user_id="u9", comment_id="c9", message="M", idempotency_key="k_st4", status=DMStatus.SENT.value, dm_id="dm_4"),
        # 5 DELIVERED
        DMOutbox(id="ob_d1", rule_id="r_stats_dm", user_id="u10", comment_id="c10", message="M", idempotency_key="k_d1", status=DMStatus.DELIVERED.value, dm_id="dm_d1"),
        DMOutbox(id="ob_d2", rule_id="r_stats_dm", user_id="u11", comment_id="c11", message="M", idempotency_key="k_d2", status=DMStatus.DELIVERED.value, dm_id="dm_d2"),
        DMOutbox(id="ob_d3", rule_id="r_stats_dm", user_id="u12", comment_id="c12", message="M", idempotency_key="k_d3", status=DMStatus.DELIVERED.value, dm_id="dm_d3"),
        DMOutbox(id="ob_d4", rule_id="r_stats_dm", user_id="u13", comment_id="c13", message="M", idempotency_key="k_d4", status=DMStatus.DELIVERED.value, dm_id="dm_d4"),
        DMOutbox(id="ob_d5", rule_id="r_stats_dm", user_id="u14", comment_id="c14", message="M", idempotency_key="k_d5", status=DMStatus.DELIVERED.value, dm_id="dm_d5"),
        # 1 FAILED
        DMOutbox(id="ob_f1", rule_id="r_stats_dm", user_id="u15", comment_id="c15", message="M", idempotency_key="k_f1", status=DMStatus.FAILED.value),
        # 2 CANCELED
        DMOutbox(id="ob_c1", rule_id="r_stats_dm", user_id="u16", comment_id="c16", message="M", idempotency_key="k_c1", status=DMStatus.CANCELED.value),
        DMOutbox(id="ob_c2", rule_id="r_stats_dm", user_id="u17", comment_id="c17", message="M", idempotency_key="k_c2", status=DMStatus.CANCELED.value),
    ]
    db_session.add_all(dms)
    await db_session.commit()

    resp = await client.get("/stats")
    assert resp.status_code == 200
    dm_stats = resp.json()["dms"]
    assert dm_stats["queued"] == 3
    assert dm_stats["sending"] == 2
    assert dm_stats["sent"] == 4
    assert dm_stats["sent_awaiting_reconciliation"] == 4
    assert dm_stats["delivered"] == 5
    assert dm_stats["failed"] == 1
    assert dm_stats["canceled"] == 2
    # total_dispatched = sent (4) + delivered (5) + failed (1) = 10
    assert dm_stats["total_dispatched"] == 10


# ------------------------------------------------------------------------------
# Tests I & J: UserRuleExecution and DuplicateRuleExecution counts
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_i_j_rule_execution_and_duplicate_counts(client: AsyncClient, db_session: AsyncSession) -> None:
    rule1 = Rule(rule_id="r_stat_1", keyword="KEY1", dm_message="M1", is_active=True)
    rule2 = Rule(rule_id="r_stat_2", keyword="KEY2", dm_message="M2", is_active=True)
    rule3 = Rule(rule_id="r_stat_3", keyword="KEY3", dm_message="M3", is_active=False)  # inactive
    db_session.add_all([rule1, rule2, rule3])

    # 4 unique executions
    execs = [
        UserRuleExecution(user_id="usr_a", rule_id="r_stat_1"),
        UserRuleExecution(user_id="usr_b", rule_id="r_stat_1"),
        UserRuleExecution(user_id="usr_c", rule_id="r_stat_2"),
        UserRuleExecution(user_id="usr_d", rule_id="r_stat_2"),
    ]
    # 3 duplicate attempts blocked
    dups = [
        DuplicateRuleExecution(user_id="usr_a", rule_id="r_stat_1"),
        DuplicateRuleExecution(user_id="usr_a", rule_id="r_stat_1"),
        DuplicateRuleExecution(user_id="usr_c", rule_id="r_stat_2"),
    ]
    db_session.add_all(execs + dups)
    await db_session.commit()

    resp = await client.get("/stats")
    assert resp.status_code == 200
    rule_stats = resp.json()["rules"]
    assert rule_stats["active_rules"] == 2
    assert rule_stats["rules_triggered"] == 4
    assert rule_stats["duplicate_executions_blocked"] == 3


# ------------------------------------------------------------------------------
# Tests K, L, M: Rate limiter stats derived from rate_limit_logs
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_k_l_m_rate_limiter_stats(client: AsyncClient, db_session: AsyncSession) -> None:
    now = utc_now()

    # 4 active send reservations in last 60s
    active_sends = [
        RateLimitLog(endpoint="POST /v1/dm/send", sent_at=now - timedelta(seconds=10)),
        RateLimitLog(endpoint="POST /v1/dm/send", sent_at=now - timedelta(seconds=20)),
        RateLimitLog(endpoint="POST /v1/dm/send", sent_at=now - timedelta(seconds=30)),
        RateLimitLog(endpoint="POST /v1/dm/send", sent_at=now - timedelta(seconds=40)),
    ]
    # 2 expired send reservations (> 60s ago)
    expired_sends = [
        RateLimitLog(endpoint="POST /v1/dm/send", sent_at=now - timedelta(seconds=70)),
        RateLimitLog(endpoint="POST /v1/dm/send", sent_at=now - timedelta(seconds=120)),
    ]
    # 3 GET status requests (should NOT count towards send budget)
    get_logs = [
        RateLimitLog(endpoint="GET /v1/dm/123", sent_at=now - timedelta(seconds=5)),
        RateLimitLog(endpoint="GET /v1/dm/456", sent_at=now - timedelta(seconds=15)),
        RateLimitLog(endpoint="GET /v1/dm/789", sent_at=now - timedelta(seconds=25)),
    ]
    db_session.add_all(active_sends + expired_sends + get_logs)
    await db_session.commit()

    resp = await client.get("/stats")
    assert resp.status_code == 200
    rl_stats = resp.json()["rate_limiter"]
    assert rl_stats["sends_last_60s"] == 4
    assert rl_stats["send_limit"] == 10
    assert rl_stats["tokens_available"] == 6
    assert rl_stats["retry_after_seconds"] == 0.0
    assert rl_stats["window_seconds"] == 60


# ------------------------------------------------------------------------------
# Test N: Statistics across complete end-to-end webhook ingestion workflow
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_n_stats_after_webhook_ingestion(client: AsyncClient, db_session: AsyncSession) -> None:
    # 1. Create rule
    rule_payload = {"keyword": "DISCOUNT", "dm_message": "Here is 10% off!"}
    rule_resp = await client.post("/rules", json=rule_payload)
    assert rule_resp.status_code == 201

    # 2. Ingest matching comment.created
    p1 = {
        "event_id": "evt_stat_01",
        "event_type": "comment.created",
        "data": {"comment_id": "cmt_st_1", "text": "Can I have a DISCOUNT?", "from": {"user_id": "usr_st_1"}},
    }
    b1 = json.dumps(p1).encode("utf-8")
    w1 = await client.post("/webhook", content=b1, headers=create_signed_headers(b1))
    assert w1.status_code == 200

    # 3. Ingest second matching comment from same user (duplicate)
    p2 = {
        "event_id": "evt_stat_02",
        "event_type": "comment.created",
        "data": {"comment_id": "cmt_st_2", "text": "Give me another DISCOUNT please", "from": {"user_id": "usr_st_1"}},
    }
    b2 = json.dumps(p2).encode("utf-8")
    w2 = await client.post("/webhook", content=b2, headers=create_signed_headers(b2))
    assert w2.status_code == 200

    # 4. Ingest comment.deleted
    p3 = {
        "event_id": "evt_stat_03",
        "event_type": "comment.deleted",
        "data": {"comment_id": "cmt_st_3", "post_id": "pst_1"},
    }
    b3 = json.dumps(p3).encode("utf-8")
    w3 = await client.post("/webhook", content=b3, headers=create_signed_headers(b3))
    assert w3.status_code == 200

    # Query stats
    stats_resp = await client.get("/stats")
    assert stats_resp.status_code == 200
    stats = stats_resp.json()

    assert stats["events"]["total_received"] == 3
    assert stats["events"]["comments_created"] == 2
    assert stats["events"]["comments_deleted"] == 1
    assert stats["rules"]["active_rules"] == 1
    assert stats["rules"]["rules_triggered"] == 1
    assert stats["rules"]["duplicate_executions_blocked"] == 1
    assert stats["dms"]["queued"] == 1


# ------------------------------------------------------------------------------
# Test O & P: Database source of truth & Concurrent stability
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_o_p_database_source_of_truth_and_concurrency(client: AsyncClient, db_session: AsyncSession) -> None:
    # Concurrently insert data and query /stats
    async def insert_worker_activity(user_idx: int) -> None:
        async with TestAsyncSessionLocal() as session:
            rule = Rule(rule_id=f"r_conc_{user_idx}", keyword=f"CONC_{user_idx}", dm_message="Msg", is_active=True)
            outbox = DMOutbox(
                id=f"ob_conc_{user_idx}",
                rule_id=rule.rule_id,
                user_id=f"usr_c_{user_idx}",
                comment_id=f"cmt_c_{user_idx}",
                message="Msg",
                idempotency_key=f"idemp_c_{user_idx}",
                status=DMStatus.DELIVERED.value,
                dm_id=f"dm_c_{user_idx}",
            )
            session.add_all([rule, outbox])
            await session.commit()

    # Launch 5 concurrent inserts and 5 concurrent /stats requests
    insert_tasks = [insert_worker_activity(i) for i in range(5)]
    stats_tasks = [client.get("/stats") for _ in range(5)]

    await asyncio.gather(*(insert_tasks + stats_tasks))

    # Final stats check must reflect all 5 committed items
    resp = await client.get("/stats")
    assert resp.status_code == 200
    data = resp.json()
    assert data["rules"]["active_rules"] >= 5
    assert data["dms"]["delivered"] >= 5
