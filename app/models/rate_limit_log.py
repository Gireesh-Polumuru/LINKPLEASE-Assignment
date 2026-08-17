from datetime import datetime, timezone
from typing import Optional
from sqlalchemy import BigInteger, DateTime, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class RateLimitLog(Base):
    """Tracks outbound POST /v1/dm/send requests to enforce PseudoGram's 10 requests / rolling 60s limit."""

    __tablename__ = "rate_limit_logs"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
        doc="Auto-incrementing primary key for rate limit log record",
    )
    endpoint: Mapped[str] = mapped_column(
        String(100),
        default="POST /v1/dm/send",
        nullable=False,
        doc="API endpoint called (only send calls consume the rate limit)",
    )
    sent_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
        index=True,
        doc="Timestamp when the outbound request was sent",
    )
    status_code: Mapped[Optional[int]] = mapped_column(
        Integer,
        nullable=True,
        doc="HTTP status code returned by PseudoGram",
    )
    dm_outbox_id: Mapped[Optional[str]] = mapped_column(
        String(36),
        nullable=True,
        index=True,
        doc="Associated DM outbox record ID",
    )

    __table_args__ = (
        Index("ix_rate_limit_logs_window", "endpoint", "sent_at"),
    )

    def __repr__(self) -> str:
        return f"<RateLimitLog(id={self.id}, endpoint='{self.endpoint}', sent_at='{self.sent_at}')>"
