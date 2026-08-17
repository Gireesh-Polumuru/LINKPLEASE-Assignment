# System Failure Modes & Architectural Limitations

This document outlines four real-world failure scenarios and architectural limitations present in the current LinkPlease implementation.

---

### 1. Potential Loss of a DM: Bounded Retries During Extended Downstream Outages
- **Condition**: The downstream PseudoGram API experiences an outage or elevated error rate (such as sustained HTTP 500/503 responses or continuous network failures) lasting longer than the total duration of the configured retry schedule (default `max_retries = 5`, spanning ~30–60 seconds of exponential backoff).
- **What Happens**: In [`app/workers/dm_worker.py`](file:///c:/Users/pc/OneDrive/Desktop/LINKPLEASE%20Assignment/app/workers/dm_worker.py), once `attempts >= max_retries`, the worker transitions the `DMOutbox` record to `status = 'FAILED'`.
- **Why It Happens**: The system design bounds retry attempts to avoid infinite worker loops on permanently broken deliveries. Because there is currently no secondary Dead-Letter Queue (DLQ) automated replay mechanism or extended multi-hour backoff tier, records marked `FAILED` are abandoned and will not be dispatched to the recipient unless manually re-queued in the database.
- **Classification**: Observed in testing (`test_max_retries_transitions_to_failed`) as an intended circuit-breaking design boundary, but an operational limitation during prolonged outages.

---

### 2. Potential Duplicate DM Delivery: Dependence on Downstream Idempotency Contract
- **Condition**: A dispatch worker transmits `POST /v1/dm/send` with a deterministic `Idempotency-Key`. The downstream service receives and executes the send, but a network interruption or timeout occurs before the HTTP 202 response reaches LinkPlease, triggering a `httpx.ReadTimeout`.
- **What Happens**: LinkPlease retains the outbox record in `QUEUED` status and re-transmits the dispatch during the next retry cycle using the exact same `Idempotency-Key` (`dm_{user_id}_{rule_id}_{comment_id}`).
- **Why It Happens**: Delivery deduplication across network partitions fundamentally relies on the downstream service correctly honoring the documented `Idempotency-Key` contract. If the downstream provider fails to recognize or deduplicate against previously seen idempotency keys on retried requests, a duplicate DM could be delivered to the recipient.
- **Classification**: Theoretical distributed systems limitation (Two-Generals Problem) inherent to at-least-once delivery architectures relying on third-party idempotency guarantees.

---

### 3. Metric & Rolling-Window Inaccuracy: Clock Skew Across Distributed Nodes
- **Condition**: Multiple application and worker instances run across separate physical hosts or containers whose system clocks are not strictly synchronized via NTP.
- **What Happens**: Both `DMSendRateLimiter` and `GET /stats` calculate the rolling 60-second window by evaluating `RateLimitLog.sent_at > (now - 60s)`, where `now` is determined using the local application runtime (`datetime.now(timezone.utc)`).
- **Why It Happens**: If an application or worker node's clock drifts relative to the node that recorded a `RateLimitLog` entry or relative to the database server, boundary calculations for the 60-second sliding window may evaluate slightly different time windows across nodes. In a multi-node deployment, this poses a theoretical risk of minor metric distortion in `GET /stats` or slight skew in rate-limit throttling around boundary edges.
- **Classification**: Theoretical multi-node distributed deployment risk when application clocks are not centrally synchronized.

---

### 4. Behavioral Race Condition: Comment Deletion Arriving After Dispatch Claim
- **Condition**: A user posts a keyword-matching comment and immediately deletes it, but the `comment.deleted` webhook reaches the application after the dispatch worker has already claimed the outbox item (`SENDING`) or after PseudoGram accepted the dispatch (`SENT`).
- **What Happens**: The `POST /webhook` handler for `comment.deleted` queries `dm_outbox` for records in `QUEUED` status to mark them `CANCELED`. Because the outbox record has already transitioned to `SENDING` or `SENT`, `handle_comment_deleted` leaves the record intact, and the DM is delivered to the recipient.
- **Why It Happens**: Once an outbound HTTP request is in-flight or accepted downstream, it cannot be recalled because the downstream API does not provide a cancellation or recall endpoint (`DELETE /v1/dm/{dm_id}`).
- **Classification**: Observed in integration testing; an unavoidable asynchronous race condition between external user actions and in-flight external API dispatches.
