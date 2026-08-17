from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.schemas.rule import RuleCreate, RuleResponse
from app.services.rule_service import create_rule

router = APIRouter(tags=["Rules"])


@router.post(
    "/rules",
    response_model=RuleResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create automated DM rule",
    description="Registers a new keyword trigger and its associated direct message template.",
)
async def create_new_rule(
    payload: RuleCreate,
    db: AsyncSession = Depends(get_db),
) -> RuleResponse:
    """Creates a new keyword rule and persists it in PostgreSQL."""
    rule = await create_rule(db=db, rule_in=payload)
    return RuleResponse.model_validate(rule)
