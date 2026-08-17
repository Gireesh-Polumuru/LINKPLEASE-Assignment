# System Failure Modes & Architectural Limitations

This document outlines four real-world failure scenarios and architectural limitations present in the current LinkPlease implementation.

---

## 1. Potential Loss of a DM: Bounded Retries During Extended Downstream Outages

- **Impact**: A recipient never receives their requested DM.
- **Trigger (Condition)**: The downstream PseudoGram API experiences a prolonged outage or network partition lasting longer than the total duration of the configured retry schedule (default `max_retries = 5`, spanning ~30–60 seconds of exponential backoff).
- **What Happens**: In [`app/workers/dm_worker.py`](file:///c:/Users/pc/OneDrive/Desktop/LINKPLEASE%20Assignment/app/workers/dm_worker.py), once `attempts >= max_retries`, the dispatch worker permanently marks the outbox record as `status = 'FAILED'`.
- **Root Cause**: The retry budget is intentionally bounded to prevent worker threads from looping infinitely on permanent errors. Because the system currently lacks a secondary Dead-Letter Queue (DLQ) with automated multi-hour replay, records marked `FAILED` are abandoned unless an engineer manually re-queues them in the database.
- **Classification**: **Observed in test suite** (`test_max_retries_transitions_to_failed`) as an intended circuit-breaking design boundary, but an operational limitation during prolonged downstream outages.

---

## 2. Potential Duplicate DM: Dependence on Downstream Idempotency Contract

- **Impact**: A recipient receives the exact same DM twice.
- **Trigger (Condition)**: The dispatch worker transmits `POST /v1/dm/send` with a unique `Idempotency-Key`. Downstream receives and delivers the DM, but a network interruption occurs before the HTTP 202 response returns to LinkPlease (causing an `httpx.ReadTimeout`).
- **What Happens**: LinkPlease cannot confirm delivery, retains the record in `QUEUED` status, and re-transmits the request during the next retry cycle with the exact same deterministic `Idempotency-Key` (`dm_{user_id}_{rule_id}_{comment_id}`).
- **Root Cause**: In distributed systems (the Two-Generals Problem), exact-once delivery across network partitions fundamentally depends on the downstream service honoring the documented `Idempotency-Key` contract. If the downstream provider fails to deduplicate against previously processed keys across retries, duplicate delivery occurs.
- **Classification**: **Theoretical distributed systems limitation** inherent to at-least-once delivery architectures relying on third-party idempotency guarantees.

---

## 3. Rate-Limit & Metric Skew: Clock Drift Across Distributed Nodes

- **Impact**: Outbound rate limit throttling or `GET /stats` metrics skew slightly around rolling-window boundary edges.
- **Trigger (Condition)**: Multiple application or worker instances run on separate physical hosts or cloud containers whose local system clocks drift out of synchronization without strict NTP coordination.
- **What Happens**: Both `DMSendRateLimiter` and `GET /stats` evaluate the rolling 60-second window cutoff as `datetime.now(timezone.utc) - timedelta(seconds=60)` using the local runtime clock.
- **Root Cause**: If Node A's clock runs 3 seconds ahead of Node B, each node calculates a slightly different 60-second time window against the shared `rate_limit_logs` table. This creates a boundary calculation skew where records near the 60-second boundary may be included by one worker and excluded by another.
- **Classification**: **Theoretical distributed deployment risk** in multi-node clusters lacking synchronized network clocks.

---

## 4. Unwanted DM Delivery: Comment Deletion Arriving After Dispatch Claim

- **Impact**: A user deletes their comment on Instagram, but still receives the automated DM.
- **Trigger (Condition)**: A user posts a keyword comment and immediately deletes it, but the `comment.deleted` webhook arrives after the dispatch worker has already claimed the outbox item (`SENDING`) or dispatched the HTTP request (`SENT`).
- **What Happens**: The `POST /webhook` handler for `comment.deleted` queries `dm_outbox` for records in `QUEUED` status to mark them `CANCELED`. Because the record has already transitioned to `SENDING` or `SENT`, it is left untouched and the DM is delivered.
- **Root Cause**: Once an outbound HTTP dispatch is in-flight or accepted downstream, it cannot be canceled or revoked because the downstream API does not provide a recall endpoint (`DELETE /v1/dm/{dm_id}`).
- **Classification**: **Observed in integration testing** (`test_sim_out_of_order_deletion_handling`); an unavoidable asynchronous race condition between external user actions and in-flight external API calls.
