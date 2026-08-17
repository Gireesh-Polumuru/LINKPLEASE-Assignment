import uuid
from datetime import datetime, timezone
from typing import Any, Optional
from sqlalchemy import JSON, DateTime, Index, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class WebhookEvent(Base):
    """Stores incoming webhook events with strict deduplication by event_id."""

    __tablename__ = "webhook_events"

    id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid.uuid4()),
        doc="Internal primary key for the webhook event record",
    )
    event_id: Mapped[str] = mapped_column(
        String(255),
        unique=True,
        nullable=False,
        index=True,
        doc="Unique external event identifier from PseudoGram webhook",
    )
    event_type: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        index=True,
        doc="Type of event (e.g., 'comment.created', 'comment.deleted')",
    )
    comment_id: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
        index=True,
        doc="ID of the comment associated with this event",
    )
    post_id: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
        index=True,
        doc="ID of the post/media on which the comment was made",
    )
    user_id: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
        index=True,
        doc="Unique user identifier of the commenter (authoritative identity)",
    )
    username: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
        doc="Username handle of the commenter",
    )
    text: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        doc="Text content of the comment",
    )
    sent_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        doc="Timestamp when event was sent by external platform",
    )
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
        index=True,
        doc="Timestamp when event was received by LinkPlease webhook",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
        doc="Timestamp when record was inserted into database",
    )
    status: Mapped[str] = mapped_column(
        String(50),
        default="PROCESSED",
        nullable=False,
        index=True,
        doc="Processing status: PROCESSED, DUPLICATE, IGNORED, ERROR",
    )
    raw_payload: Mapped[Optional[dict[str, Any]]] = mapped_column(
        JSON,
        nullable=True,
        doc="Raw JSON payload received for auditability",
    )

    __table_args__ = (
        Index("ix_webhook_events_user_type", "user_id", "event_type"),
        Index("ix_webhook_events_comment_type", "comment_id", "event_type"),
    )

    def __repr__(self) -> str:
        return f"<WebhookEvent(event_id='{self.event_id}', event_type='{self.event_type}', status='{self.status}')>"
