import asyncio
import json
import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.core.security import compute_signature
from app.models.deleted_comment import DeletedComment
from app.models.dm_outbox import DMOutbox, DMStatus
from app.models.duplicate_rule_execution import DuplicateRuleExecution
from app.models.rule import Rule
from app.models.user_rule_execution import UserRuleExecution
from app.models.webhook_event import WebhookEvent

TEST_SECRET = "test_pseudogram_secret_key_12345"


def create_signed_headers(body: bytes, secret: str = TEST_SECRET) -> dict[str, str]:
    """Generates headers with valid HMAC-SHA256 signature for testing."""
    sig = compute_signature(raw_body=body, secret=secret)
    return {
        "Content-Type": "application/json",
        "X-PseudoGram-Signature": f"sha256={sig}",
    }


@pytest.fixture(autouse=True)
def setup_webhook_settings():
    """Sets consistent test settings for HMAC verification."""
    original_secret = settings.PSEUDOGRAM_API_KEY
    original_verify = settings.VERIFY_WEBHOOK_SIGNATURE
    settings.PSEUDOGRAM_API_KEY = TEST_SECRET
    settings.VERIFY_WEBHOOK_SIGNATURE = True
    yield
    settings.PSEUDOGRAM_API_KEY = original_secret
    settings.VERIFY_WEBHOOK_SIGNATURE = original_verify


# ------------------------------------------------------------------------------
# Test A: Valid comment.created with matching rule
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_valid_comment_created_matching_rule(client: AsyncClient, db_session: AsyncSession):
    # Setup active rule
    rule = Rule(
        rule_id="rule_price_01",
        keyword="PRICE",
        dm_message="Check our pricing here: https://example.com/pricing",
        is_active=True,
    )
    db_session.add(rule)
    await db_session.commit()

    payload_dict = {
        "event_id": "evt_test_a_01",
        "event_type": "comment.created",
        "sent_at": "2026-08-10T09:14:22.481Z",
        "data": {
            "comment_id": "cmt_test_a_01",
            "post_id": "post_test_a_01",
            "text": "Can I know the PRICE please?",
            "created_at": "2026-08-10T09:14:21.900Z",
            "from": {
                "user_id": "usr_test_a_01",
                "username": "arjun.shoots",
            },
        },
    }
    raw_body = json.dumps(payload_dict).encode("utf-8")
    headers = create_signed_headers(raw_body)

    response = await client.post("/webhook", content=raw_body, headers=headers)
    assert response.status_code == 200
    resp_data = response.json()
    assert resp_data["status"] == "ok"

    # Verify WebhookEvent in DB
    ev_res = await db_session.execute(
        select(WebhookEvent).where(WebhookEvent.event_id == "evt_test_a_01")
    )
    event_db = ev_res.scalar_one_or_none()
    assert event_db is not None
    assert event_db.status == "PROCESSED"
    assert event_db.user_id == "usr_test_a_01"
    assert event_db.comment_id == "cmt_test_a_01"

    # Verify UserRuleExecution in DB
    exec_res = await db_session.execute(
        select(UserRuleExecution).where(UserRuleExecution.user_id == "usr_test_a_01")
    )
    exec_db = exec_res.scalar_one_or_none()
    assert exec_db is not None
    assert exec_db.rule_id == "rule_price_01"

    # Verify DMOutbox in DB
    outbox_res = await db_session.execute(
        select(DMOutbox).where(DMOutbox.user_id == "usr_test_a_01")
    )
    outbox_db = outbox_res.scalar_one_or_none()
    assert outbox_db is not None
    assert outbox_db.status == DMStatus.QUEUED.value
    assert outbox_db.rule_id == "rule_price_01"
    assert outbox_db.comment_id == "cmt_test_a_01"
    assert outbox_db.message == "Check our pricing here: https://example.com/pricing"
    assert outbox_db.attempts == 0
    assert outbox_db.idempotency_key == "dm_usr_test_a_01_rule_price_01_cmt_test_a_01"


# ------------------------------------------------------------------------------
# Test B: Valid comment.created with no matching rule
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_valid_comment_created_no_matching_rule(client: AsyncClient, db_session: AsyncSession):
    rule = Rule(
        rule_id="rule_discount_01",
        keyword="DISCOUNT",
        dm_message="Here is your 20% discount code: PROMO20",
        is_active=True,
    )
    db_session.add(rule)
    await db_session.commit()

    payload_dict = {
        "event_id": "evt_test_b_01",
        "event_type": "comment.created",
        "data": {
            "comment_id": "cmt_test_b_01",
            "text": "Great photo! Beautiful view!",
            "from": {
                "user_id": "usr_test_b_01",
                "username": "sarah.travels",
            },
        },
    }
    raw_body = json.dumps(payload_dict).encode("utf-8")
    headers = create_signed_headers(raw_body)

    response = await client.post("/webhook", content=raw_body, headers=headers)
    assert response.status_code == 200

    # Event stored
    ev_res = await db_session.execute(
        select(WebhookEvent).where(WebhookEvent.event_id == "evt_test_b_01")
    )
    assert ev_res.scalar_one_or_none() is not None

    # No executions or outbox records
    exec_res = await db_session.execute(select(UserRuleExecution))
    assert len(exec_res.scalars().all()) == 0

    outbox_res = await db_session.execute(select(DMOutbox))
    assert len(outbox_res.scalars().all()) == 0


# ------------------------------------------------------------------------------
# Test C & D: Case-insensitive and Substring matching
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "comment_text",
    [
        "PRICE please",
        "price please",
        "Price please",
        "PrIcE please",
        "Can I get the price?",
        "what is the PRICE?",
        "hey, price? thanks",
    ],
)
async def test_case_insensitive_and_substring_matching(
    client: AsyncClient,
    db_session: AsyncSession,
    comment_text: str,
):
    rule = Rule(
        rule_id="rule_price_match",
        keyword="PRICE",
        dm_message="Pricing details sent!",
        is_active=True,
    )
    db_session.add(rule)
    await db_session.commit()

    event_id = f"evt_param_{abs(hash(comment_text))}"
    user_id = f"usr_param_{abs(hash(comment_text))}"

    payload_dict = {
        "event_id": event_id,
        "event_type": "comment.created",
        "data": {
            "comment_id": f"cmt_{event_id}",
            "text": comment_text,
            "from": {
                "user_id": user_id,
            },
        },
    }
    raw_body = json.dumps(payload_dict).encode("utf-8")
    headers = create_signed_headers(raw_body)

    response = await client.post("/webhook", content=raw_body, headers=headers)
    assert response.status_code == 200

    outbox_res = await db_session.execute(
        select(DMOutbox).where(DMOutbox.user_id == user_id)
    )
    outbox = outbox_res.scalar_one_or_none()
    assert outbox is not None
    assert outbox.rule_id == "rule_price_match"


# ------------------------------------------------------------------------------
# Test E: Duplicate event_id (Sequential)
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_duplicate_event_id_handling(client: AsyncClient, db_session: AsyncSession):
    rule = Rule(
        rule_id="rule_dup_event",
        keyword="DEMO",
        dm_message="Here is your demo link!",
        is_active=True,
    )
    db_session.add(rule)
    await db_session.commit()

    payload_dict = {
        "event_id": "evt_dup_same_id_01",
        "event_type": "comment.created",
        "data": {
            "comment_id": "cmt_dup_01",
            "text": "Send me the DEMO",
            "from": {
                "user_id": "usr_dup_01",
            },
        },
    }
    raw_body = json.dumps(payload_dict).encode("utf-8")
    headers = create_signed_headers(raw_body)

    # First delivery
    resp1 = await client.post("/webhook", content=raw_body, headers=headers)
    assert resp1.status_code == 200

    # Second delivery with exact same event_id
    resp2 = await client.post("/webhook", content=raw_body, headers=headers)
    assert resp2.status_code == 200

    # Exactly 1 WebhookEvent, 1 UserRuleExecution, and 1 DMOutbox
    events = (await db_session.execute(select(WebhookEvent).where(WebhookEvent.event_id == "evt_dup_same_id_01"))).scalars().all()
    assert len(events) == 1

    execs = (await db_session.execute(select(UserRuleExecution).where(UserRuleExecution.user_id == "usr_dup_01"))).scalars().all()
    assert len(execs) == 1

    outboxes = (await db_session.execute(select(DMOutbox).where(DMOutbox.user_id == "usr_dup_01"))).scalars().all()
    assert len(outboxes) == 1


# ------------------------------------------------------------------------------
# Test E2: Concurrent Duplicate event_id
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_concurrent_duplicate_event_id(client: AsyncClient, db_session: AsyncSession):
    rule = Rule(
        rule_id="rule_concurrent_evt",
        keyword="INFO",
        dm_message="Here is the info.",
        is_active=True,
    )
    db_session.add(rule)
    await db_session.commit()

    payload_dict = {
        "event_id": "evt_concurrent_exact_same",
        "event_type": "comment.created",
        "data": {
            "comment_id": "cmt_concurrent_01",
            "text": "More INFO please",
            "from": {
                "user_id": "usr_concurrent_01",
            },
        },
    }
    raw_body = json.dumps(payload_dict).encode("utf-8")
    headers = create_signed_headers(raw_body)

    # Send multiple simultaneous requests with exact same event_id
    responses = await asyncio.gather(
        client.post("/webhook", content=raw_body, headers=headers),
        client.post("/webhook", content=raw_body, headers=headers),
        client.post("/webhook", content=raw_body, headers=headers),
    )

    for resp in responses:
        assert resp.status_code == 200

    events = (await db_session.execute(select(WebhookEvent).where(WebhookEvent.event_id == "evt_concurrent_exact_same"))).scalars().all()
    assert len(events) == 1

    outboxes = (await db_session.execute(select(DMOutbox).where(DMOutbox.user_id == "usr_concurrent_01"))).scalars().all()
    assert len(outboxes) == 1


# ------------------------------------------------------------------------------
# Test F: Same user, same rule, different event_ids (Deduplication & Stats)
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_same_user_same_rule_different_events(client: AsyncClient, db_session: AsyncSession):
    rule = Rule(
        rule_id="rule_price_dedup",
        keyword="PRICE",
        dm_message="Pricing: https://example.com/pricing",
        is_active=True,
    )
    db_session.add(rule)
    await db_session.commit()

    # Event 1
    p1 = {
        "event_id": "evt_f_01",
        "event_type": "comment.created",
        "data": {"comment_id": "cmt_f_01", "text": "PRICE please", "from": {"user_id": "usr_123"}},
    }
    b1 = json.dumps(p1).encode("utf-8")
    resp1 = await client.post("/webhook", content=b1, headers=create_signed_headers(b1))
    assert resp1.status_code == 200

    # Event 2 from same user matching same rule
    p2 = {
        "event_id": "evt_f_02",
        "event_type": "comment.created",
        "data": {"comment_id": "cmt_f_02", "text": "Can I get price again?", "from": {"user_id": "usr_123"}},
    }
    b2 = json.dumps(p2).encode("utf-8")
    resp2 = await client.post("/webhook", content=b2, headers=create_signed_headers(b2))
    assert resp2.status_code == 200

    # Event 3 from same user matching same rule
    p3 = {
        "event_id": "evt_f_03",
        "event_type": "comment.created",
        "data": {"comment_id": "cmt_f_03", "text": "What was the price?", "from": {"user_id": "usr_123"}},
    }
    b3 = json.dumps(p3).encode("utf-8")
    resp3 = await client.post("/webhook", content=b3, headers=create_signed_headers(b3))
    assert resp3.status_code == 200

    # Verify exactly 1 execution and 1 DMOutbox
    execs = (await db_session.execute(select(UserRuleExecution).where(UserRuleExecution.user_id == "usr_123"))).scalars().all()
    assert len(execs) == 1

    outboxes = (await db_session.execute(select(DMOutbox).where(DMOutbox.user_id == "usr_123"))).scalars().all()
    assert len(outboxes) == 1

    # Verify duplicate attempts are recorded in DuplicateRuleExecution for stats
    dup_logs = (await db_session.execute(select(DuplicateRuleExecution).where(DuplicateRuleExecution.user_id == "usr_123"))).scalars().all()
    assert len(dup_logs) == 2


# ------------------------------------------------------------------------------
# Test G: Different users, same rule
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_different_users_same_rule(client: AsyncClient, db_session: AsyncSession):
    rule = Rule(
        rule_id="rule_price_multi_user",
        keyword="PRICE",
        dm_message="Pricing info",
        is_active=True,
    )
    db_session.add(rule)
    await db_session.commit()

    p1 = {
        "event_id": "evt_g_01",
        "event_type": "comment.created",
        "data": {"comment_id": "cmt_g_01", "text": "PRICE please", "from": {"user_id": "usr_123"}},
    }
    b1 = json.dumps(p1).encode("utf-8")
    await client.post("/webhook", content=b1, headers=create_signed_headers(b1))

    p2 = {
        "event_id": "evt_g_02",
        "event_type": "comment.created",
        "data": {"comment_id": "cmt_g_02", "text": "PRICE please", "from": {"user_id": "usr_456"}},
    }
    b2 = json.dumps(p2).encode("utf-8")
    await client.post("/webhook", content=b2, headers=create_signed_headers(b2))

    execs = (await db_session.execute(select(UserRuleExecution))).scalars().all()
    assert len(execs) == 2

    outboxes = (await db_session.execute(select(DMOutbox))).scalars().all()
    assert len(outboxes) == 2


# ------------------------------------------------------------------------------
# Test H: Multiple matching rules on same comment
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_multiple_matching_rules(client: AsyncClient, db_session: AsyncSession):
    rule1 = Rule(rule_id="r_price", keyword="PRICE", dm_message="Price details", is_active=True)
    rule2 = Rule(rule_id="r_link", keyword="LINK", dm_message="Direct link", is_active=True)
    db_session.add_all([rule1, rule2])
    await db_session.commit()

    payload = {
        "event_id": "evt_multi_rule_01",
        "event_type": "comment.created",
        "data": {
            "comment_id": "cmt_multi_01",
            "text": "Send me the PRICE and the LINK please!",
            "from": {"user_id": "usr_multi_01"},
        },
    }
    b = json.dumps(payload).encode("utf-8")
    resp = await client.post("/webhook", content=b, headers=create_signed_headers(b))
    assert resp.status_code == 200

    execs = (await db_session.execute(select(UserRuleExecution).where(UserRuleExecution.user_id == "usr_multi_01"))).scalars().all()
    assert len(execs) == 2
    rule_ids = {e.rule_id for e in execs}
    assert rule_ids == {"r_price", "r_link"}

    outboxes = (await db_session.execute(select(DMOutbox).where(DMOutbox.user_id == "usr_multi_01"))).scalars().all()
    assert len(outboxes) == 2


# ------------------------------------------------------------------------------
# Test I: Concurrent duplicate user/rule requests
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_concurrent_duplicate_user_rule_requests(client: AsyncClient, db_session: AsyncSession):
    rule = Rule(rule_id="r_conc_dup", keyword="COUPON", dm_message="Your coupon code", is_active=True)
    db_session.add(rule)
    await db_session.commit()

    # 3 concurrent requests from same user with distinct event_ids and comment_ids matching same rule
    requests_data = [
        {"event_id": f"evt_conc_{i}", "event_type": "comment.created", "data": {"comment_id": f"cmt_conc_{i}", "text": "I want a COUPON", "from": {"user_id": "usr_same_concurrent"}}}
        for i in range(3)
    ]

    async def send_req(p_dict):
        raw = json.dumps(p_dict).encode("utf-8")
        return await client.post("/webhook", content=raw, headers=create_signed_headers(raw))

    responses = await asyncio.gather(*(send_req(p) for p in requests_data))
    for r in responses:
        assert r.status_code == 200

    # Exactly 1 UserRuleExecution and 1 DMOutbox must be created
    execs = (await db_session.execute(select(UserRuleExecution).where(UserRuleExecution.user_id == "usr_same_concurrent"))).scalars().all()
    assert len(execs) == 1

    outboxes = (await db_session.execute(select(DMOutbox).where(DMOutbox.user_id == "usr_same_concurrent"))).scalars().all()
    assert len(outboxes) == 1


# ------------------------------------------------------------------------------
# Test J: Valid comment.deleted after queued delivery
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_comment_deleted_cancels_queued_dm(client: AsyncClient, db_session: AsyncSession):
    rule = Rule(rule_id="r_del", keyword="BUY", dm_message="Purchase link", is_active=True)
    db_session.add(rule)
    await db_session.commit()

    # 1. comment.created arrives
    create_payload = {
        "event_id": "evt_created_first",
        "event_type": "comment.created",
        "data": {"comment_id": "cmt_to_delete_01", "text": "I want to BUY now", "from": {"user_id": "usr_buyer_01"}},
    }
    cb = json.dumps(create_payload).encode("utf-8")
    resp1 = await client.post("/webhook", content=cb, headers=create_signed_headers(cb))
    assert resp1.status_code == 200

    # Verify status is QUEUED
    outbox = (await db_session.execute(select(DMOutbox).where(DMOutbox.comment_id == "cmt_to_delete_01"))).scalar_one()
    assert outbox.status == DMStatus.QUEUED.value

    # 2. comment.deleted arrives
    delete_payload = {
        "event_id": "evt_deleted_second",
        "event_type": "comment.deleted",
        "data": {"comment_id": "cmt_to_delete_01", "post_id": "post_100"},
    }
    db_body = json.dumps(delete_payload).encode("utf-8")
    resp2 = await client.post("/webhook", content=db_body, headers=create_signed_headers(db_body))
    assert resp2.status_code == 200

    # Verify status is now CANCELED
    await db_session.refresh(outbox)
    assert outbox.status == DMStatus.CANCELED.value


# ------------------------------------------------------------------------------
# Test K: comment.deleted before comment.created (Out of order)
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_out_of_order_comment_deletion(client: AsyncClient, db_session: AsyncSession):
    rule = Rule(rule_id="r_ooo", keyword="DISCOUNT", dm_message="Discount info", is_active=True)
    db_session.add(rule)
    await db_session.commit()

    # 1. comment.deleted arrives FIRST
    del_payload = {
        "event_id": "evt_ooo_delete_01",
        "event_type": "comment.deleted",
        "data": {"comment_id": "cmt_ooo_01"},
    }
    db_bytes = json.dumps(del_payload).encode("utf-8")
    resp1 = await client.post("/webhook", content=db_bytes, headers=create_signed_headers(db_bytes))
    assert resp1.status_code == 200

    # Verify tombstone exists
    tombstone = (await db_session.execute(select(DeletedComment).where(DeletedComment.comment_id == "cmt_ooo_01"))).scalar_one_or_none()
    assert tombstone is not None

    # 2. comment.created arrives SECOND for the same comment_id
    create_payload = {
        "event_id": "evt_ooo_create_02",
        "event_type": "comment.created",
        "data": {"comment_id": "cmt_ooo_01", "text": "Need DISCOUNT please", "from": {"user_id": "usr_ooo_01"}},
    }
    cb_bytes = json.dumps(create_payload).encode("utf-8")
    resp2 = await client.post("/webhook", content=cb_bytes, headers=create_signed_headers(cb_bytes))
    assert resp2.status_code == 200

    # Verify NO DMOutbox record was created
    outboxes = (await db_session.execute(select(DMOutbox).where(DMOutbox.comment_id == "cmt_ooo_01"))).scalars().all()
    assert len(outboxes) == 0


# ------------------------------------------------------------------------------
# Test L: Duplicate comment.deleted events
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_duplicate_comment_deleted(client: AsyncClient, db_session: AsyncSession):
    payload = {
        "event_id": "evt_del_dup_01",
        "event_type": "comment.deleted",
        "data": {"comment_id": "cmt_del_dup_01"},
    }
    raw = json.dumps(payload).encode("utf-8")
    headers = create_signed_headers(raw)

    resp1 = await client.post("/webhook", content=raw, headers=headers)
    assert resp1.status_code == 200

    # Exact duplicate delivery
    resp2 = await client.post("/webhook", content=raw, headers=headers)
    assert resp2.status_code == 200

    # Different event_id deleting same comment
    payload2 = {
        "event_id": "evt_del_dup_02",
        "event_type": "comment.deleted",
        "data": {"comment_id": "cmt_del_dup_01"},
    }
    raw2 = json.dumps(payload2).encode("utf-8")
    resp3 = await client.post("/webhook", content=raw2, headers=create_signed_headers(raw2))
    assert resp3.status_code == 200

    tombstones = (await db_session.execute(select(DeletedComment).where(DeletedComment.comment_id == "cmt_del_dup_01"))).scalars().all()
    assert len(tombstones) == 1


# ------------------------------------------------------------------------------
# Test M: Invalid HMAC signature rejected
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_invalid_hmac_rejected(client: AsyncClient):
    payload = {
        "event_id": "evt_bad_sig_01",
        "event_type": "comment.created",
        "data": {"comment_id": "c1", "text": "hello", "from": {"user_id": "u1"}},
    }
    raw = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "X-PseudoGram-Signature": "sha256=invalid_forged_hex_signature_here",
    }
    resp = await client.post("/webhook", content=raw, headers=headers)
    assert resp.status_code == 401
    assert "Invalid or missing webhook signature" in resp.json()["detail"]


# ------------------------------------------------------------------------------
# Test N: Valid HMAC signature accepted
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_valid_hmac_accepted(client: AsyncClient):
    payload = {
        "event_id": "evt_good_sig_01",
        "event_type": "comment.created",
        "data": {"comment_id": "c_good", "text": "hello", "from": {"user_id": "u_good"}},
    }
    raw = json.dumps(payload).encode("utf-8")
    headers = create_signed_headers(raw)
    resp = await client.post("/webhook", content=raw, headers=headers)
    assert resp.status_code == 200


# ------------------------------------------------------------------------------
# Test O: Modified request body with old signature rejected
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_tampered_payload_rejected(client: AsyncClient):
    orig_payload = {"event_id": "evt_tamper_01", "event_type": "comment.created", "data": {"comment_id": "c1", "text": "orig", "from": {"user_id": "u1"}}}
    orig_raw = json.dumps(orig_payload).encode("utf-8")
    headers = create_signed_headers(orig_raw)

    # Tamper the body while sending old signature header
    tampered_payload = {"event_id": "evt_tamper_01", "event_type": "comment.created", "data": {"comment_id": "c1", "text": "tampered_text", "from": {"user_id": "u1"}}}
    tampered_raw = json.dumps(tampered_payload).encode("utf-8")

    resp = await client.post("/webhook", content=tampered_raw, headers=headers)
    assert resp.status_code == 401


# ------------------------------------------------------------------------------
# Test P: Unsupported event type safely ignored
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_unsupported_event_type(client: AsyncClient, db_session: AsyncSession):
    payload = {
        "event_id": "evt_unsupported_01",
        "event_type": "post.liked",
        "sent_at": "2026-08-10T09:14:22.481Z",
        "data": {"post_id": "p_123", "user_id": "u_liker"},
    }
    raw = json.dumps(payload).encode("utf-8")
    headers = create_signed_headers(raw)

    resp = await client.post("/webhook", content=raw, headers=headers)
    assert resp.status_code == 200

    event_db = (await db_session.execute(select(WebhookEvent).where(WebhookEvent.event_id == "evt_unsupported_01"))).scalar_one_or_none()
    assert event_db is not None
    assert event_db.status == "IGNORED"

    outboxes = (await db_session.execute(select(DMOutbox))).scalars().all()
    assert len(outboxes) == 0


# ------------------------------------------------------------------------------
# Test Q: Malformed comment.created payload rejected with 422
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_malformed_comment_created_rejected(client: AsyncClient):
    # Missing 'from' field
    payload = {
        "event_id": "evt_malformed_01",
        "event_type": "comment.created",
        "data": {"comment_id": "c_malformed", "text": "hello"},
    }
    raw = json.dumps(payload).encode("utf-8")
    headers = create_signed_headers(raw)

    resp = await client.post("/webhook", content=raw, headers=headers)
    assert resp.status_code == 422


# ------------------------------------------------------------------------------
# Test R: Signature enforcement toggling
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_signature_verification_disabled(client: AsyncClient):
    settings.VERIFY_WEBHOOK_SIGNATURE = False
    payload = {
        "event_id": "evt_no_sig_needed",
        "event_type": "comment.created",
        "data": {"comment_id": "c_no_sig", "text": "test", "from": {"user_id": "u_no_sig"}},
    }
    raw = json.dumps(payload).encode("utf-8")
    # No signature header provided
    resp = await client.post("/webhook", content=raw)
    assert resp.status_code == 200
