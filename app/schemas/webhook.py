from datetime import datetime
from typing import Any, Optional
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class WebhookUser(BaseModel):
    """User identity contained in webhook payloads."""

    user_id: str = Field(
        ...,
        min_length=1,
        description="Unique authoritative identifier of the user",
        examples=["usr_3b91fe"],
    )
    username: Optional[str] = Field(
        default=None,
        description="Username handle of the user",
        examples=["arjun.shoots"],
    )

    @field_validator("user_id")
    @classmethod
    def validate_user_id(cls, v: str) -> str:
        trimmed = v.strip()
        if not trimmed:
            raise ValueError("user_id must not be empty or whitespace-only.")
        return trimmed


class CommentCreatedData(BaseModel):
    """Data payload for comment.created events."""

    comment_id: str = Field(
        ...,
        min_length=1,
        description="Unique comment identifier",
        examples=["cmt_9f2a7c"],
    )
    post_id: Optional[str] = Field(
        default=None,
        description="Post identifier on which comment was made",
        examples=["post_44de1b"],
    )
    text: str = Field(
        ...,
        description="Text body of the comment",
        examples=["PRICE please 🙏"],
    )
    created_at: Optional[datetime] = Field(
        default=None,
        description="Creation timestamp from external platform",
    )
    from_user: WebhookUser = Field(
        ...,
        alias="from",
        description="Authoritative user information",
    )

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    @field_validator("comment_id")
    @classmethod
    def validate_comment_id(cls, v: str) -> str:
        trimmed = v.strip()
        if not trimmed:
            raise ValueError("comment_id must not be empty or whitespace-only.")
        return trimmed


class CommentDeletedData(BaseModel):
    """Data payload for comment.deleted events."""

    comment_id: str = Field(
        ...,
        min_length=1,
        description="Unique comment identifier of deleted comment",
        examples=["cmt_9f2a7c"],
    )
    post_id: Optional[str] = Field(
        default=None,
        description="Post identifier if available",
        examples=["post_44de1b"],
    )

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    @field_validator("comment_id")
    @classmethod
    def validate_comment_id(cls, v: str) -> str:
        trimmed = v.strip()
        if not trimmed:
            raise ValueError("comment_id must not be empty or whitespace-only.")
        return trimmed


class WebhookPayload(BaseModel):
    """Envelope for all incoming PseudoGram webhook deliveries."""

    event_id: str = Field(
        ...,
        min_length=1,
        description="Unique external event identifier",
        examples=["evt_01J8ZQ4K2N7RXA"],
    )
    event_type: str = Field(
        ...,
        min_length=1,
        description="Event classification (e.g. comment.created, comment.deleted)",
        examples=["comment.created"],
    )
    sent_at: Optional[datetime] = Field(
        default=None,
        description="Timestamp when event was dispatched by sender",
    )
    data: Optional[Any] = Field(
        default=None,
        description="Event data payload specific to event_type",
    )

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    @field_validator("event_id")
    @classmethod
    def validate_event_id(cls, v: str) -> str:
        trimmed = v.strip()
        if not trimmed:
            raise ValueError("event_id must not be empty or whitespace-only.")
        return trimmed

    @field_validator("event_type")
    @classmethod
    def validate_event_type(cls, v: str) -> str:
        trimmed = v.strip()
        if not trimmed:
            raise ValueError("event_type must not be empty or whitespace-only.")
        return trimmed

    @model_validator(mode="after")
    def validate_payload_data(self) -> "WebhookPayload":
        """Strictly validates event-specific payload data based on event_type."""
        if self.event_type == "comment.created":
            if self.data is None or not isinstance(self.data, dict):
                raise ValueError("Payload 'data' object is required for comment.created events.")
            # Validate and parse into CommentCreatedData
            parsed_comment = CommentCreatedData.model_validate(self.data)
            self.data = parsed_comment
        elif self.event_type == "comment.deleted":
            if self.data is None or not isinstance(self.data, dict):
                raise ValueError("Payload 'data' object is required for comment.deleted events.")
            # Validate and parse into CommentDeletedData
            parsed_deleted = CommentDeletedData.model_validate(self.data)
            self.data = parsed_deleted
        return self


class WebhookResponse(BaseModel):
    """Standard HTTP response returned by POST /webhook."""

    status: str = Field(
        default="ok",
        description="Ingestion status acknowledgment",
        examples=["ok", "duplicate"],
    )
    message: Optional[str] = Field(
        default=None,
        description="Detailed processing message or note",
    )
