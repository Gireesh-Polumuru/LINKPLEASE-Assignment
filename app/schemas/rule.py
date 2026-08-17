from pydantic import BaseModel, ConfigDict, Field, field_validator


class RuleCreate(BaseModel):
    """Request payload for creating a new automated DM rule."""

    keyword: str = Field(
        ...,
        description="The keyword to match in comments (case-insensitive during matching)",
        examples=["PRICE", "link", "demo"],
    )
    dm_message: str = Field(
        ...,
        description="The direct message content to dispatch when the rule is triggered",
        examples=["Here's the price list: https://example.com/pricing"],
    )

    @field_validator("keyword")
    @classmethod
    def validate_keyword(cls, v: str) -> str:
        trimmed = v.strip()
        if not trimmed:
            raise ValueError("Keyword must not be empty or whitespace-only.")
        return trimmed

    @field_validator("dm_message")
    @classmethod
    def validate_dm_message(cls, v: str) -> str:
        trimmed = v.strip()
        if not trimmed:
            raise ValueError("dm_message must not be empty or whitespace-only.")
        return trimmed


class RuleResponse(BaseModel):
    """Response payload returned upon successful rule creation."""

    rule_id: str = Field(
        ...,
        description="Unique identifier for the created rule",
        examples=["b3c58f01-7009-4e78-a8ee-f03dfc8fa440"],
    )
    keyword: str = Field(
        ...,
        description="The matched keyword",
        examples=["PRICE"],
    )
    dm_message: str = Field(
        ...,
        description="The direct message content to send",
        examples=["Here's the price list: https://example.com/pricing"],
    )

    model_config = ConfigDict(from_attributes=True)
