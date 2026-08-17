from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.schemas.stats import StatsResponse
from app.services.stats_service import get_system_stats

router = APIRouter(prefix="/stats", tags=["Stats"])


@router.get(
    "",
    response_model=StatsResponse,
    summary="Get real-time system metrics",
    description="Returns accurate, real-time metrics derived directly from durable database state without in-memory drift.",
)
async def get_stats(db: AsyncSession = Depends(get_db)) -> StatsResponse:
    """Calculates and returns current system metrics across events, rules, outbox DMs, and rate limiting."""
    return await get_system_stats(db)
