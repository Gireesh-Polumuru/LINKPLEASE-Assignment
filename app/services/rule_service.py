import uuid
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.rule import Rule
from app.schemas.rule import RuleCreate


async def create_rule(db: AsyncSession, rule_in: RuleCreate) -> Rule:
    """Creates and persists a new Rule in the database."""
    rule = Rule(
        rule_id=str(uuid.uuid4()),
        keyword=rule_in.keyword,
        dm_message=rule_in.dm_message,
        is_active=True,
    )
    db.add(rule)
    await db.commit()
    await db.refresh(rule)
    return rule
