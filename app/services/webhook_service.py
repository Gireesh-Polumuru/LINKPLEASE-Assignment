import logging
from datetime import datetime, timezone
from typing import Any
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.deleted_comment import DeletedComment
from app.models.dm_outbox import DMOutbox, DMStatus
from app.models.duplicate_rule_execution import DuplicateRuleExecution
from app.models.rule import Rule
from app.models.user_rule_execution import UserRuleExecution
from app.models.webhook_event import WebhookEvent
from app.schemas.webhook import CommentCreatedData, CommentDeletedData, WebhookPayload

logger = logging.getLogger(__name__)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


async def process_webhook_event(
    db: AsyncSession,
    payload: WebhookPayload,
    raw_payload: dict[str, Any],
) -> dict[str, Any]:
    """Processes an incoming webhook event atomically and idempotently.
    
    Handles:
    - Exact-once event ingestion deduplicated by event_id at the DB constraint level.
    - comment.deleted tombstone recording and queued outbox cancellation.
    - comment.created out-of-order tombstone verification.
    - Case-insensitive substring rule matching.
    - User/rule deduplication with database constraint UNIQUE(user_id, rule_id).
    - Atomic UserRuleExecution and DMOutbox creation with status QUEUED.
    - Durable duplicate-blocked tracking for stats.
    - Safe handling of unsupported event types.
    """
    # Extract event fields upfront
    comment_id = None
    post_id = None
    user_id = None
    username = None
    text = None

    if payload.event_type == "comment.created":
        comment_data: CommentCreatedData = payload.data
        comment_id = comment_data.comment_id
        post_id = comment_data.post_id
        user_id = comment_data.from_user.user_id
        username = comment_data.from_user.username
        text = comment_data.text or ""
    elif payload.event_type == "comment.deleted":
        deleted_data: CommentDeletedData = payload.data
        comment_id = deleted_data.comment_id
        post_id = deleted_data.post_id

    event_record = WebhookEvent(
        event_id=payload.event_id,
        event_type=payload.event_type,
        comment_id=comment_id,
        post_id=post_id,
        user_id=user_id,
        username=username,
        text=text,
        sent_at=payload.sent_at,
        received_at=utc_now(),
        status="PROCESSED" if payload.event_type in ("comment.created", "comment.deleted") else "IGNORED",
        raw_payload=raw_payload,
    )

    # 1. Event-level Deduplication using Database UNIQUE(event_id) Constraint
    try:
        db.add(event_record)
        await db.flush()
    except IntegrityError:
        # A concurrent or prior request with the same event_id already exists.
        await db.rollback()
        logger.info("Duplicate event_id '%s' received. Ignored safely.", payload.event_id)
        return {"status": "ok", "message": "Duplicate event ignored"}

    # 2. Process based on event_type
    if payload.event_type == "comment.deleted":
        # Insert tombstone into deleted_comments (idempotent / safe on conflict)
        try:
            async with db.begin_nested():
                tombstone = DeletedComment(
                    comment_id=comment_id,
                    event_id=payload.event_id,
                    deleted_at=utc_now(),
                )
                db.add(tombstone)
                await db.flush()
        except IntegrityError:
            # Tombstone already exists from a prior event, continue
            pass

        # Cancel any pending QUEUED DMs for this deleted comment
        # (do NOT cancel already DELIVERED DMs)
        cancel_stmt = (
            update(DMOutbox)
            .where(
                DMOutbox.comment_id == comment_id,
                DMOutbox.status == DMStatus.QUEUED.value,
            )
            .values(
                status=DMStatus.CANCELED.value,
                last_error="Canceled due to comment.deleted event",
                updated_at=utc_now(),
            )
        )
        await db.execute(cancel_stmt)

        await db.commit()
        return {"status": "ok", "message": "Comment deletion processed"}

    elif payload.event_type == "comment.created":
        # Check for out-of-order deletion in deleted_comments tombstone
        tombstone_check = await db.execute(
            select(DeletedComment).where(DeletedComment.comment_id == comment_id)
        )
        if tombstone_check.scalar_one_or_none() is not None:
            logger.info("Comment '%s' was deleted before comment.created arrived. Skipping DM creation.", comment_id)
            event_record.status = "IGNORED_DELETED"
            await db.commit()
            return {"status": "ok", "message": "Comment already deleted; skipped DM creation"}

        # Fetch all active rules
        rules_query = await db.execute(
            select(Rule).where(Rule.is_active == True)
        )
        active_rules = rules_query.scalars().all()

        lower_text = text.lower()

        # Evaluate rules and atomically create UserRuleExecution + DMOutbox
        for rule in active_rules:
            if rule.keyword.lower() in lower_text:
                # Rule matches! Attempt atomic execution and outbox creation
                try:
                    async with db.begin_nested():
                        execution = UserRuleExecution(
                            user_id=user_id,
                            rule_id=rule.rule_id,
                            comment_id=comment_id,
                            triggered_at=utc_now(),
                        )
                        db.add(execution)
                        await db.flush()  # Enforces UNIQUE(user_id, rule_id) constraint

                        outbox_item = DMOutbox(
                            rule_id=rule.rule_id,
                            user_id=user_id,
                            comment_id=comment_id,
                            message=rule.dm_message,
                            idempotency_key=f"dm_{user_id}_{rule.rule_id}_{comment_id}",
                            status=DMStatus.QUEUED.value,
                            attempts=0,
                            max_attempts=5,
                            next_retry_at=utc_now(),
                        )
                        db.add(outbox_item)
                        await db.flush()
                except IntegrityError:
                    # uq_user_rule_execution constraint prevented duplicate execution.
                    # Savepoint rolled back both execution and outbox insertion.
                    # Durably record the blocked duplicate for stats.
                    dup_record = DuplicateRuleExecution(
                        user_id=user_id,
                        rule_id=rule.rule_id,
                        comment_id=comment_id,
                        event_id=payload.event_id,
                        blocked_at=utc_now(),
                    )
                    db.add(dup_record)
                    logger.info(
                        "Duplicate rule execution blocked for user '%s' on rule '%s'.",
                        user_id,
                        rule.rule_id,
                    )

        await db.commit()
        return {"status": "ok", "message": "Event processed successfully"}

    else:
        # Unsupported event type: acknowledged safely with HTTP 200 without creating DMs
        await db.commit()
        return {"status": "ok", "message": f"Unsupported event type '{payload.event_type}' acknowledged"}
