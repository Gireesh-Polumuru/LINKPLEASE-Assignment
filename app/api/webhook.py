import json
import logging
from fastapi import APIRouter, Depends, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.core.security import verify_webhook_signature
from app.database import get_db
from app.schemas.webhook import WebhookPayload, WebhookResponse
from app.services.webhook_service import process_webhook_event

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Webhooks"])


@router.post(
    "/webhook",
    response_model=WebhookResponse,
    status_code=status.HTTP_200_OK,
    summary="Ingest PseudoGram Webhook Event",
    description="Receives, validates HMAC signature, deduplicates, and stages webhook events and outbox DMs.",
)
async def handle_webhook(
    request: Request,
    db: AsyncSession = Depends(get_db),
    x_pseudogram_signature: str | None = Header(default=None, alias="X-PseudoGram-Signature"),
) -> JSONResponse:
    """Ingests incoming webhook payloads from PseudoGram platform."""
    # 1. Read the RAW request body before any parsing for HMAC verification
    raw_body = await request.body()

    # 2. HMAC Signature Verification (if configured)
    if settings.VERIFY_WEBHOOK_SIGNATURE:
        secret = settings.PSEUDOGRAM_API_KEY or settings.WEBHOOK_SECRET
        if not verify_webhook_signature(
            raw_body=raw_body,
            signature_header=x_pseudogram_signature,
            secret=secret,
        ):
            logger.warning("Rejecting webhook request: Invalid or missing X-PseudoGram-Signature.")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or missing webhook signature.",
            )

    # 3. Parse JSON from raw body
    try:
        raw_json = json.loads(raw_body.decode("utf-8"))
    except Exception as exc:
        logger.error("Failed to decode webhook JSON: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Malformed JSON body.",
        )

    # 4. Validate Schema using Pydantic
    try:
        payload = WebhookPayload.model_validate(raw_json)
    except ValidationError as val_err:
        logger.warning("Webhook payload validation failed: %s", val_err)
        raise HTTPException(
            status_code=422,
            detail=val_err.errors(),
        )

    # 5. Process event via Webhook Service
    result = await process_webhook_event(
        db=db,
        payload=payload,
        raw_payload=raw_json,
    )

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=result,
    )
