from datetime import datetime, timedelta, timezone
from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.dm_outbox import DMOutbox, DMStatus
from app.models.duplicate_rule_execution import DuplicateRuleExecution
from app.models.rate_limit_log import RateLimitLog
from app.models.rule import Rule
from app.models.user_rule_execution import UserRuleExecution
from app.models.webhook_event import WebhookEvent
from app.schemas.stats import (
    DMsStats,
    EventsStats,
    RateLimiterStats,
    RulesStats,
    StatsResponse,
)


def ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


async def get_system_stats(db: AsyncSession) -> StatsResponse:
    """Calculates real-time system metrics directly from committed database state.
    
    Zero in-memory drift: All metrics are derived directly from PostgreSQL/database records,
    ensuring exact metrics under concurrency and across application restarts.
    """
    # 1. Aggregate Webhook Event Statistics
    events_query = select(
        func.count(WebhookEvent.id).label("total_received"),
        func.count(case((WebhookEvent.event_type == "comment.created", 1))).label("comments_created"),
        func.count(case((WebhookEvent.event_type == "comment.deleted", 1))).label("comments_deleted"),
        func.count(case((WebhookEvent.status.in_(["PROCESSED", "IGNORED_DELETED"]), 1))).label("unique_processed"),
        func.count(case((WebhookEvent.status == "DUPLICATE", 1))).label("duplicates_ignored"),
    ).select_from(WebhookEvent)

    events_res = await db.execute(events_query)
    events_row = events_res.one()

    events_stats = EventsStats(
        total_received=events_row.total_received or 0,
        unique_processed=events_row.unique_processed or 0,
        duplicates_ignored=events_row.duplicates_ignored or 0,
        comments_created=events_row.comments_created or 0,
        comments_deleted=events_row.comments_deleted or 0,
    )

    # 2. Aggregate Rule Management and Trigger Statistics
    active_rules_res = await db.execute(
        select(func.count(Rule.rule_id)).where(Rule.is_active == True)
    )
    active_rules = active_rules_res.scalar() or 0

    rules_triggered_res = await db.execute(
        select(func.count(UserRuleExecution.id))
    )
    rules_triggered = rules_triggered_res.scalar() or 0

    duplicate_blocked_res = await db.execute(
        select(func.count(DuplicateRuleExecution.id))
    )
    duplicate_executions_blocked = duplicate_blocked_res.scalar() or 0

    rules_stats = RulesStats(
        active_rules=active_rules,
        rules_triggered=rules_triggered,
        duplicate_executions_blocked=duplicate_executions_blocked,
    )

    # 3. Aggregate DM Outbox State Machine Metrics
    dms_query = select(
        func.count(case((DMOutbox.status == DMStatus.QUEUED.value, 1))).label("queued"),
        func.count(case((DMOutbox.status == DMStatus.SENDING.value, 1))).label("sending"),
        func.count(case((DMOutbox.status == DMStatus.SENT.value, 1))).label("sent"),
        func.count(case((DMOutbox.status == DMStatus.DELIVERED.value, 1))).label("delivered"),
        func.count(case((DMOutbox.status == DMStatus.FAILED.value, 1))).label("failed"),
        func.count(case((DMOutbox.status == DMStatus.CANCELED.value, 1))).label("canceled"),
    ).select_from(DMOutbox)

    dms_res = await db.execute(dms_query)
    dms_row = dms_res.one()

    queued_count = dms_row.queued or 0
    sending_count = dms_row.sending or 0
    sent_count = dms_row.sent or 0
    delivered_count = dms_row.delivered or 0
    failed_count = dms_row.failed or 0
    canceled_count = dms_row.canceled or 0
    total_dispatched = sent_count + delivered_count + failed_count

    dms_stats = DMsStats(
        queued=queued_count,
        sending=sending_count,
        sent=sent_count,
        sent_awaiting_reconciliation=sent_count,
        delivered=delivered_count,
        failed=failed_count,
        canceled=canceled_count,
        total_dispatched=total_dispatched,
    )

    # 4. Aggregate POST /v1/dm/send Rate Limiter Metrics (GET requests do NOT consume budget)
    now = datetime.now(timezone.utc)
    window_seconds = settings.DM_SEND_RATE_WINDOW_SECONDS
    send_limit = settings.DM_SEND_RATE_LIMIT
    window_cutoff = now - timedelta(seconds=window_seconds)

    rate_logs_query = (
        select(RateLimitLog.sent_at)
        .where(
            RateLimitLog.endpoint == "POST /v1/dm/send",
            RateLimitLog.sent_at > window_cutoff,
        )
        .order_by(RateLimitLog.sent_at.asc())
    )
    rate_logs_res = await db.execute(rate_logs_query)
    active_sends = rate_logs_res.scalars().all()
    sends_last_60s = len(active_sends)
    tokens_available = max(0, send_limit - sends_last_60s)

    retry_after_seconds = 0.0
    if sends_last_60s >= send_limit and active_sends:
        oldest_active_send = ensure_utc(active_sends[sends_last_60s - send_limit])
        elapsed = (now - oldest_active_send).total_seconds()
        retry_after_seconds = max(0.0, round(float(window_seconds) - elapsed, 2))

    rate_limiter_stats = RateLimiterStats(
        sends_last_60s=sends_last_60s,
        send_limit=send_limit,
        tokens_available=tokens_available,
        retry_after_seconds=retry_after_seconds,
        window_seconds=window_seconds,
    )

    return StatsResponse(
        events=events_stats,
        rules=rules_stats,
        dms=dms_stats,
        rate_limiter=rate_limiter_stats,
    )
