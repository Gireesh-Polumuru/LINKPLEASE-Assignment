from datetime import datetime, timezone
from typing import Optional
from sqlalchemy import DateTime, String
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class DeletedComment(Base):
    """Tombstone entity recording deleted comments to handle out-of-order webhook delivery."""

    __tablename__ = "deleted_comments"

    comment_id: Mapped[str] = mapped_column(
        String(255),
        primary_key=True,
        doc="Unique identifier of the deleted comment",
    )
    event_id: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
        doc="Event ID of the comment.deleted webhook event",
    )
    deleted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
        doc="Timestamp when the deletion was recorded",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
        doc="Record creation timestamp",
    )

    def __repr__(self) -> str:
        return f"<DeletedComment(comment_id='{self.comment_id}', deleted_at='{self.deleted_at}')>"
