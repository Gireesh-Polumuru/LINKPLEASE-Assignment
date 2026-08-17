import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.rule import Rule


@pytest.mark.asyncio
async def test_create_valid_rule(client: AsyncClient, db_session: AsyncSession) -> None:
    """Test creating a valid rule returns HTTP 201 with exact expected fields and persists to DB."""
    payload = {
        "keyword": "PRICE",
        "dm_message": "Here's the price list: https://example.com/pricing",
    }
    response = await client.post("/rules", json=payload)

    # 1. HTTP status is exactly 201 Created
    assert response.status_code == 201

    # 2. Response contains exact required fields
    data = response.json()
    assert "rule_id" in data
    assert "keyword" in data
    assert "dm_message" in data
    assert data["keyword"] == "PRICE"
    assert data["dm_message"] == "Here's the price list: https://example.com/pricing"
    assert len(data["rule_id"]) > 0

    # Ensure no extra unexpected fields
    assert set(data.keys()) == {"rule_id", "keyword", "dm_message"}

    # 3. Rule is actually persisted in the database
    query = select(Rule).where(Rule.rule_id == data["rule_id"])
    result = await db_session.execute(query)
    persisted_rule = result.scalar_one_or_none()

    assert persisted_rule is not None
    assert persisted_rule.keyword == "PRICE"
    assert persisted_rule.dm_message == "Here's the price list: https://example.com/pricing"
    assert persisted_rule.is_active is True


@pytest.mark.asyncio
async def test_create_rule_whitespace_trimming(client: AsyncClient) -> None:
    """Test that surrounding whitespace on keyword and message is trimmed cleanly."""
    payload = {
        "keyword": "  DISCOUNT20  ",
        "dm_message": "  Use code DISCOUNT20 for 20% off!  ",
    }
    response = await client.post("/rules", json=payload)
    assert response.status_code == 201
    data = response.json()
    assert data["keyword"] == "DISCOUNT20"
    assert data["dm_message"] == "Use code DISCOUNT20 for 20% off!"


@pytest.mark.asyncio
async def test_reject_empty_keyword(client: AsyncClient) -> None:
    """Test that empty keyword string is rejected with HTTP 422."""
    payload = {
        "keyword": "",
        "dm_message": "Some valid message",
    }
    response = await client.post("/rules", json=payload)
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_reject_whitespace_only_keyword(client: AsyncClient) -> None:
    """Test that whitespace-only keyword is rejected with HTTP 422."""
    payload = {
        "keyword": "    \t  \n  ",
        "dm_message": "Some valid message",
    }
    response = await client.post("/rules", json=payload)
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_reject_empty_dm_message(client: AsyncClient) -> None:
    """Test that empty dm_message string is rejected with HTTP 422."""
    payload = {
        "keyword": "VALID_KEYWORD",
        "dm_message": "",
    }
    response = await client.post("/rules", json=payload)
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_reject_whitespace_only_dm_message(client: AsyncClient) -> None:
    """Test that whitespace-only dm_message is rejected with HTTP 422."""
    payload = {
        "keyword": "VALID_KEYWORD",
        "dm_message": "   \n\t  ",
    }
    response = await client.post("/rules", json=payload)
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_create_multiple_rules_with_unique_ids(
    client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """Test creating multiple rules and verifying all receive distinct unique rule_ids."""
    rules_to_create = [
        {"keyword": "LINK", "dm_message": "Here is the link: https://link.com"},
        {"keyword": "INFO", "dm_message": "Here is more info: https://info.com"},
        {"keyword": "PRICING", "dm_message": "Here is pricing: https://pricing.com"},
    ]

    created_ids = set()
    for item in rules_to_create:
        response = await client.post("/rules", json=item)
        assert response.status_code == 201
        data = response.json()
        assert data["rule_id"] not in created_ids
        created_ids.add(data["rule_id"])

    assert len(created_ids) == 3

    # Check that all 3 exist in the database
    query = select(Rule)
    result = await db_session.execute(query)
    all_rules = result.scalars().all()
    assert len(all_rules) == 3
