import uuid
from datetime import datetime, timezone
from typing import Optional
from sqlalchemy import DateTime, ForeignKey, Index, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class DuplicateRuleExecution(Base):
    """Durable record of blocked duplicate rule executions for auditability and GET /stats."""

    __tablename__ = "duplicate_rule_executions"

    id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid.uuid4()),
        doc="Internal primary key for the duplicate record",
    )
    user_id: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        index=True,
        doc="Unique user identifier of the recipient",
    )
    rule_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("rules.rule_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        doc="Foreign key reference to the matched rule",
    )
    comment_id: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
        doc="Comment ID of the duplicate triggering attempt",
    )
    event_id: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
        doc="Webhook event ID of the duplicate attempt",
    )
    blocked_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
        doc="Timestamp when the duplicate attempt was blocked",
    )

    # Relationships
    rule = relationship("Rule")

    __table_args__ = (
        Index("ix_duplicate_rule_executions_user_rule", "user_id", "rule_id"),
        Index("ix_duplicate_rule_executions_blocked_at", "blocked_at"),
    )

    def __repr__(self) -> str:
        return f"<DuplicateRuleExecution(user_id='{self.user_id}', rule_id='{self.rule_id}', blocked_at='{self.blocked_at}')>"
