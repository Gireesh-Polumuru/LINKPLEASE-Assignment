import enum
import uuid
from datetime import datetime, timezone
from typing import Optional
from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class DMStatus(str, enum.Enum):
    """Explicit state machine enumeration for DM delivery outbox lifecycle."""
    QUEUED = "QUEUED"        # Initial state, ready to be dispatched by worker
    SENDING = "SENDING"      # In-flight lock acquired by dispatch worker
    SENT = "SENT"            # HTTP 202 Accepted received from PseudoGram (pending reconciliation)
    DELIVERED = "DELIVERED"  # Confirmed delivered via GET /v1/dm/{dm_id} reconciliation
    FAILED = "FAILED"        # Permanent failure (400 Bad Request or max retries exceeded)
    CANCELED = "CANCELED"    # Canceled prior to send (e.g. comment.deleted received)


class DMOutbox(Base):
    """Transactional Outbox for reliable, asynchronous DM dispatch and delivery reconciliation."""

    __tablename__ = "dm_outbox"

    id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid.uuid4()),
        doc="Internal unique outbox delivery identifier",
    )
    rule_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("rules.rule_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        doc="Foreign key to the triggering rule",
    )
    user_id: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        index=True,
        doc="Recipient user identifier",
    )
    comment_id: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        index=True,
        doc="ID of the comment that triggered this DM",
    )
    message: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        doc="Direct message content to deliver",
    )
    idempotency_key: Mapped[str] = mapped_column(
        String(255),
        unique=True,
        nullable=False,
        index=True,
        doc="Unique idempotency key passed to PseudoGram API header",
    )
    status: Mapped[str] = mapped_column(
        String(50),
        default=DMStatus.QUEUED.value,
        nullable=False,
        index=True,
        doc="Current delivery lifecycle status: QUEUED, SENDING, SENT, DELIVERED, FAILED, CANCELED",
    )
    dm_id: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
        index=True,
        doc="External DM identifier returned by PseudoGram upon 202 Accepted",
    )
    attempts: Mapped[int] = mapped_column(
        Integer,
        default=0,
        nullable=False,
        doc="Number of dispatch attempts executed",
    )
    max_attempts: Mapped[int] = mapped_column(
        Integer,
        default=5,
        nullable=False,
        doc="Maximum allowed retry attempts before marking as FAILED",
    )
    next_retry_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
        index=True,
        doc="Scheduled timestamp for next dispatch attempt or retry",
    )
    last_error: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        doc="Last error message or HTTP detail returned on failure",
    )
    sent_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        doc="Timestamp when PseudoGram accepted the DM (HTTP 202)",
    )
    delivered_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        doc="Timestamp when delivery was verified via reconciliation",
    )
    last_reconciled_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        index=True,
        doc="Timestamp of the most recent delivery status check",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
        doc="Timestamp when outbox item was created",
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
        nullable=False,
        doc="Timestamp when outbox item was last modified",
    )

    # Relationships
    rule = relationship("Rule", back_populates="outbox_dms")

    __table_args__ = (
        Index("ix_dm_outbox_worker_fetch", "status", "next_retry_at"),
        Index("ix_dm_outbox_comment_status", "comment_id", "status"),
        Index("ix_dm_outbox_reconciliation", "status", "last_reconciled_at"),
    )

    def __repr__(self) -> str:
        return f"<DMOutbox(id={self.id}, user_id='{self.user_id}', status='{self.status}', dm_id='{self.dm_id}')>"
