from datetime import datetime, timedelta, timezone
import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.deleted_comment import DeletedComment
from app.models.dm_outbox import DMOutbox, DMStatus
from app.models.rate_limit_log import RateLimitLog
from app.models.rule import Rule
from app.models.user_rule_execution import UserRuleExecution
from app.models.webhook_event import WebhookEvent


@pytest.mark.asyncio
async def test_rule_creation_and_fields(db_session: AsyncSession) -> None:
    """Test creating a Rule and verifying default fields and values."""
    rule = Rule(
        keyword="link",
        dm_message="Here is your link: https://example.com/item",
        is_active=True,
    )
    db_session.add(rule)
    await db_session.commit()
    await db_session.refresh(rule)

    assert rule.rule_id is not None
    assert rule.keyword == "link"
    assert rule.dm_message == "Here is your link: https://example.com/item"
    assert rule.is_active is True
    assert rule.created_at is not None
    assert rule.updated_at is not None


@pytest.mark.asyncio
async def test_webhook_event_unique_event_id(db_session: AsyncSession) -> None:
    """Test that WebhookEvent enforces a database-level UNIQUE constraint on event_id."""
    event1 = WebhookEvent(
        event_id="evt_12345",
        event_type="comment.created",
        comment_id="cmt_999",
        post_id="post_888",
        user_id="usr_777",
        username="testuser",
        text="Send me the link please!",
        raw_payload={"id": "evt_12345", "type": "comment.created"},
    )
    db_session.add(event1)
    await db_session.commit()

    # Attempt to insert duplicate event_id
    event2 = WebhookEvent(
        event_id="evt_12345",
        event_type="comment.created",
        comment_id="cmt_999_dup",
        user_id="usr_777",
    )
    db_session.add(event2)
    with pytest.raises(IntegrityError):
        await db_session.commit()
    await db_session.rollback()


@pytest.mark.asyncio
async def test_deleted_comment_tombstone(db_session: AsyncSession) -> None:
    """Test storing and querying deleted comment tombstones."""
    tombstone = DeletedComment(
        comment_id="cmt_deleted_001",
        event_id="evt_del_999",
    )
    db_session.add(tombstone)
    await db_session.commit()
    await db_session.refresh(tombstone)

    assert tombstone.comment_id == "cmt_deleted_001"
    assert tombstone.event_id == "evt_del_999"
    assert tombstone.deleted_at is not None

    # Verify duplicate comment_id fails as primary key using a separate session or distinct object
    tombstone_dup = DeletedComment(comment_id="cmt_deleted_001", event_id="evt_del_dup")
    db_session.expunge(tombstone)
    db_session.add(tombstone_dup)
    with pytest.raises(IntegrityError):
        await db_session.commit()
    await db_session.rollback()


@pytest.mark.asyncio
async def test_user_rule_execution_unique_constraint(db_session: AsyncSession) -> None:
    """Test that UserRuleExecution strictly enforces UNIQUE(user_id, rule_id)."""
    rule = Rule(keyword="price", dm_message="Pricing details: $99/mo")
    db_session.add(rule)
    await db_session.commit()
    await db_session.refresh(rule)
    rule_id = rule.rule_id

    exec1 = UserRuleExecution(
        user_id="usr_alice_100",
        rule_id=rule_id,
        comment_id="cmt_001",
    )
    db_session.add(exec1)
    await db_session.commit()

    # Second execution for the same user and same rule MUST fail at DB level
    exec2 = UserRuleExecution(
        user_id="usr_alice_100",
        rule_id=rule_id,
        comment_id="cmt_002",
    )
    db_session.add(exec2)
    with pytest.raises(IntegrityError):
        await db_session.commit()
    await db_session.rollback()

    # Execution for a DIFFERENT user on the same rule must succeed
    exec_bob = UserRuleExecution(
        user_id="usr_bob_200",
        rule_id=rule_id,
        comment_id="cmt_003",
    )
    db_session.add(exec_bob)
    await db_session.commit()
    assert exec_bob.id is not None



@pytest.mark.asyncio
async def test_dm_outbox_state_transitions(db_session: AsyncSession) -> None:
    """Test DMOutbox lifecycle progression: QUEUED -> SENDING -> SENT -> DELIVERED."""
    rule = Rule(keyword="demo", dm_message="Book a demo at link")
    db_session.add(rule)
    await db_session.commit()
    await db_session.refresh(rule)

    # 1. State: QUEUED
    outbox_item = DMOutbox(
        rule_id=rule.rule_id,
        user_id="usr_target_1",
        comment_id="cmt_xyz",
        message=rule.dm_message,
        idempotency_key="idemp_usr_target_1_rule_demo_cmt_xyz",
        status=DMStatus.QUEUED.value,
    )
    db_session.add(outbox_item)
    await db_session.commit()
    await db_session.refresh(outbox_item)

    assert outbox_item.status == DMStatus.QUEUED.value
    assert outbox_item.attempts == 0
    assert outbox_item.dm_id is None
    assert outbox_item.sent_at is None
    assert outbox_item.delivered_at is None

    # 2. State: SENDING (Worker locks item for dispatch)
    outbox_item.status = DMStatus.SENDING.value
    outbox_item.attempts += 1
    await db_session.commit()
    await db_session.refresh(outbox_item)
    assert outbox_item.status == DMStatus.SENDING.value
    assert outbox_item.attempts == 1

    # 3. State: SENT (PseudoGram returned 202 Accepted with external dm_id)
    outbox_item.status = DMStatus.SENT.value
    outbox_item.dm_id = "pg_dm_abc123"
    outbox_item.sent_at = datetime.now(timezone.utc)
    await db_session.commit()
    await db_session.refresh(outbox_item)
    assert outbox_item.status == DMStatus.SENT.value
    assert outbox_item.dm_id == "pg_dm_abc123"
    assert outbox_item.sent_at is not None

    # 4. State: DELIVERED (Reconciliation worker confirmed delivery status)
    outbox_item.status = DMStatus.DELIVERED.value
    outbox_item.delivered_at = datetime.now(timezone.utc)
    outbox_item.last_reconciled_at = datetime.now(timezone.utc)
    await db_session.commit()
    await db_session.refresh(outbox_item)
    assert outbox_item.status == DMStatus.DELIVERED.value
    assert outbox_item.delivered_at is not None


@pytest.mark.asyncio
async def test_dm_outbox_failed_and_canceled_states(db_session: AsyncSession) -> None:
    """Test FAILED and CANCELED terminal states in DMOutbox."""
    rule = Rule(keyword="promo", dm_message="Use code 50OFF")
    db_session.add(rule)
    await db_session.commit()
    await db_session.refresh(rule)

    # Failed item
    item_failed = DMOutbox(
        rule_id=rule.rule_id,
        user_id="usr_fail_1",
        comment_id="cmt_fail",
        message=rule.dm_message,
        idempotency_key="idemp_fail_1",
        status=DMStatus.FAILED.value,
        last_error="HTTP 400: Recipient user does not accept DMs",
        attempts=1,
    )
    # Canceled item (e.g. comment.deleted arrived while queued)
    item_canceled = DMOutbox(
        rule_id=rule.rule_id,
        user_id="usr_cancel_1",
        comment_id="cmt_cancel",
        message=rule.dm_message,
        idempotency_key="idemp_cancel_1",
        status=DMStatus.CANCELED.value,
    )
    db_session.add_all([item_failed, item_canceled])
    await db_session.commit()

    assert item_failed.status == DMStatus.FAILED.value
    assert "HTTP 400" in (item_failed.last_error or "")
    assert item_canceled.status == DMStatus.CANCELED.value


@pytest.mark.asyncio
async def test_dm_outbox_unique_idempotency_key(db_session: AsyncSession) -> None:
    """Test that DMOutbox strictly enforces UNIQUE(idempotency_key)."""
    rule = Rule(keyword="test", dm_message="Hello")
    db_session.add(rule)
    await db_session.commit()
    await db_session.refresh(rule)

    d1 = DMOutbox(
        rule_id=rule.rule_id,
        user_id="usr_1",
        comment_id="cmt_1",
        message="Hello",
        idempotency_key="idemp_key_unique_123",
        status=DMStatus.QUEUED.value,
    )
    db_session.add(d1)
    await db_session.commit()

    d2 = DMOutbox(
        rule_id=rule.rule_id,
        user_id="usr_2",
        comment_id="cmt_2",
        message="Hello 2",
        idempotency_key="idemp_key_unique_123",
        status=DMStatus.QUEUED.value,
    )
    db_session.add(d2)
    with pytest.raises(IntegrityError):
        await db_session.commit()
    await db_session.rollback()


@pytest.mark.asyncio
async def test_rate_limit_log_window_queries(db_session: AsyncSession) -> None:
    """Test RateLimitLog recording and rolling window time-filtering."""
    now = datetime.now(timezone.utc)

    # Log 3 calls inside the last 60s window
    recent_logs = [
        RateLimitLog(endpoint="POST /v1/dm/send", sent_at=now - timedelta(seconds=10), status_code=202),
        RateLimitLog(endpoint="POST /v1/dm/send", sent_at=now - timedelta(seconds=20), status_code=202),
        RateLimitLog(endpoint="POST /v1/dm/send", sent_at=now - timedelta(seconds=30), status_code=202),
    ]
    # Log 1 call outside the 60s window (75 seconds ago)
    old_log = RateLimitLog(
        endpoint="POST /v1/dm/send",
        sent_at=now - timedelta(seconds=75),
        status_code=202,
    )

    db_session.add_all([*recent_logs, old_log])
    await db_session.commit()

    # Query calls in last 60 seconds
    window_threshold = now - timedelta(seconds=60)
    query = select(RateLimitLog).where(
        RateLimitLog.endpoint == "POST /v1/dm/send",
        RateLimitLog.sent_at >= window_threshold,
    )
    result = await db_session.execute(query)
    active_sends = result.scalars().all()

    assert len(active_sends) == 3


@pytest.mark.asyncio
async def test_database_schema_indexes_and_constraints() -> None:
    """Verify that all required tables, unique constraints, foreign keys, and indexes are defined on metadata."""
    from app.database import Base

    tables = Base.metadata.tables

    # 1. Check all 6 required tables exist
    required_tables = {
        "rules",
        "webhook_events",
        "deleted_comments",
        "user_rule_executions",
        "dm_outbox",
        "rate_limit_logs",
    }
    assert required_tables.issubset(tables.keys())

    # 2. Check WebhookEvent unique constraint / index on event_id
    webhook_table = tables["webhook_events"]
    assert webhook_table.columns["event_id"].unique is True

    # 3. Check UserRuleExecution unique constraint on (user_id, rule_id)
    user_exec_table = tables["user_rule_executions"]
    unique_constraint_cols = [
        set(c.name for c in uq.columns) for uq in user_exec_table.constraints if hasattr(uq, "columns")
    ]
    assert {"user_id", "rule_id"} in unique_constraint_cols

    # 4. Check DMOutbox unique constraint on idempotency_key
    outbox_table = tables["dm_outbox"]
    assert outbox_table.columns["idempotency_key"].unique is True

    # 5. Check DMOutbox indexes for worker queries
    outbox_index_names = {idx.name for idx in outbox_table.indexes}
    assert "ix_dm_outbox_worker_fetch" in outbox_index_names
    assert "ix_dm_outbox_comment_status" in outbox_index_names
    assert "ix_dm_outbox_reconciliation" in outbox_index_names

    # 6. Check RateLimitLog window index
    rate_table = tables["rate_limit_logs"]
    rate_index_names = {idx.name for idx in rate_table.indexes}
    assert "ix_rate_limit_logs_window" in rate_index_names

