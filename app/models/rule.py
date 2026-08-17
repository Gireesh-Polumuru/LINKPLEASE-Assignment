import uuid
from datetime import datetime, timezone
from sqlalchemy import Boolean, DateTime, Index, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class Rule(Base):
    """Rule entity for matching keywords in incoming comments and triggering automated DMs."""

    __tablename__ = "rules"

    rule_id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid.uuid4()),
        doc="Unique identifier for the rule",
    )
    keyword: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        index=True,
        doc="Keyword to match in comments (case-insensitive search)",
    )
    dm_message: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        doc="Direct message content to send to user upon rule match",
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean,
        default=True,
        nullable=False,
        doc="Whether this rule is active and eligible for matching",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
        doc="Timestamp when rule was created",
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
        nullable=False,
        doc="Timestamp when rule was last updated",
    )

    # Relationships
    executions = relationship(
        "UserRuleExecution",
        back_populates="rule",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    outbox_dms = relationship(
        "DMOutbox",
        back_populates="rule",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    __table_args__ = (
        Index("ix_rules_keyword_active", "keyword", "is_active"),
    )

    def __repr__(self) -> str:
        return f"<Rule(rule_id={self.rule_id}, keyword='{self.keyword}', is_active={self.is_active})>"
