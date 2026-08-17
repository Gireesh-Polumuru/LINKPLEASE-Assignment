import uuid
from datetime import datetime, timezone
from typing import Optional
from sqlalchemy import DateTime, ForeignKey, Index, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class UserRuleExecution(Base):
    """Tracks rule executions per user to strictly enforce that a user never receives the same rule DM twice."""

    __tablename__ = "user_rule_executions"

    id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid.uuid4()),
        doc="Internal primary key for the execution record",
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
        doc="Foreign key reference to the triggered rule",
    )
    comment_id: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
        doc="Comment ID that triggered this initial rule execution",
    )
    triggered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
        doc="Timestamp when the rule was first triggered for this user",
    )

    # Relationships
    rule = relationship("Rule", back_populates="executions")

    __table_args__ = (
        UniqueConstraint("user_id", "rule_id", name="uq_user_rule_execution"),
        Index("ix_user_rule_executions_user_rule", "user_id", "rule_id"),
    )

    def __repr__(self) -> str:
        return f"<UserRuleExecution(user_id='{self.user_id}', rule_id='{self.rule_id}')>"
