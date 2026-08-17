from pydantic import BaseModel, Field


class EventsStats(BaseModel):
    """Statistics for incoming webhook events."""
    total_received: int = Field(0, description="Total unique webhook events recorded in database")
    unique_processed: int = Field(0, description="Unique webhook events processed")
    duplicates_ignored: int = Field(0, description="Duplicate webhook events ignored")
    comments_created: int = Field(0, description="Total comment.created events received")
    comments_deleted: int = Field(0, description="Total comment.deleted events received")


class RulesStats(BaseModel):
    """Statistics for keyword rules and rule execution tracking."""
    active_rules: int = Field(0, description="Number of currently active keyword rules")
    rules_triggered: int = Field(0, description="Total unique (user_id, rule_id) triggers executed")
    duplicate_executions_blocked: int = Field(0, description="Duplicate rule triggers blocked for same user")


class DMsStats(BaseModel):
    """Statistics for transactional DM outbox deliveries across all lifecycle states."""
    queued: int = Field(0, description="DMs queued for dispatch")
    sending: int = Field(0, description="DMs currently being dispatched")
    sent: int = Field(0, description="DMs accepted by PseudoGram (HTTP 202)")
    sent_awaiting_reconciliation: int = Field(0, description="DMs sent and awaiting delivery reconciliation")
    delivered: int = Field(0, description="DMs verified as DELIVERED via reconciliation")
    failed: int = Field(0, description="DMs marked as FAILED")
    canceled: int = Field(0, description="DMs canceled prior to dispatch")
    total_dispatched: int = Field(0, description="Total DMs dispatched (sent + delivered + failed)")


class RateLimiterStats(BaseModel):
    """Real-time metrics for POST /v1/dm/send rate limiter."""
    sends_last_60s: int = Field(0, description="Outbound send attempts in last 60 seconds")
    send_limit: int = Field(10, description="Configured rate limit budget")
    tokens_available: int = Field(10, description="Available send tokens remaining in current window")
    retry_after_seconds: float = Field(0.0, description="Calculated wait duration if rate limit is reached")
    window_seconds: int = Field(60, description="Rolling window duration in seconds")


class StatsResponse(BaseModel):
    """Consolidated system statistics response for GET /stats."""
    events: EventsStats
    rules: RulesStats
    dms: DMsStats
    rate_limiter: RateLimiterStats
