from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from app.api import rules_router, stats_router, webhook_router
from app.config import settings
from app.database import init_db


from app.workers import dm_worker, reconciliation_worker


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan manager for startup and shutdown routines."""
    # Startup: initialize database schemas and background dispatch & reconciliation workers
    await init_db()
    dm_worker.start()
    reconciliation_worker.start()
    yield
    # Shutdown: clean up background workers/resources if active
    await dm_worker.stop()
    await reconciliation_worker.stop()


app = FastAPI(
    title=settings.APP_NAME,
    description="LinkPlease Tech Intern Assignment Backend Service",
    version="1.0.0",
    lifespan=lifespan,
)

# Mount API Routers
app.include_router(rules_router)
app.include_router(webhook_router)
app.include_router(stats_router)



@app.get("/health", tags=["Health"])
async def health_check() -> JSONResponse:
    """Basic health check endpoint to verify server readiness."""
    return JSONResponse(
        status_code=200,
        content={
            "status": "healthy",
            "app_name": settings.APP_NAME,
            "version": "1.0.0",
        },
    )
