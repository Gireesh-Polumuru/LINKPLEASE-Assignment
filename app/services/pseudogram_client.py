import logging
from typing import Any, Optional
import httpx

from app.config import settings

logger = logging.getLogger(__name__)


class PseudoGramClientError(Exception):
    """Base exception for all PseudoGram API client failures."""

    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


class PseudoGramBadRequestError(PseudoGramClientError):
    """Raised on HTTP 400 Bad Request (Non-retryable fatal error)."""

    def __init__(self, detail: str):
        super().__init__(f"Bad Request: {detail}", status_code=400)
        self.detail = detail


class PseudoGramRateLimitError(PseudoGramClientError):
    """Raised on HTTP 429 Rate Limited (Retryable, respects Retry-After)."""

    def __init__(self, retry_after: float, detail: str = "Rate limited"):
        super().__init__(f"Rate Limited (Retry-After: {retry_after}s): {detail}", status_code=429)
        self.retry_after = retry_after
        self.detail = detail


class PseudoGramServerError(PseudoGramClientError):
    """Raised on HTTP 500+ Internal Server Error (Retryable)."""

    def __init__(self, status_code: int, detail: str = "Server error"):
        super().__init__(f"Server Error ({status_code}): {detail}", status_code=status_code)
        self.detail = detail


class PseudoGramNetworkError(PseudoGramClientError):
    """Raised on network connection failures or timeouts (Retryable)."""

    def __init__(self, detail: str):
        super().__init__(f"Network/Timeout Error: {detail}")
        self.detail = detail


class PseudoGramClient:
    """Asynchronous HTTP client for interacting with the PseudoGram platform API."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        http_client: Optional[httpx.AsyncClient] = None,
    ):
        self.base_url = (base_url or settings.PSEUDOGRAM_BASE_URL).rstrip("/")
        self.api_key = api_key or settings.PSEUDOGRAM_API_KEY
        self._client = http_client

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is not None:
            return self._client
        return httpx.AsyncClient(timeout=10.0)

    async def send_dm(
        self,
        recipient_user_id: str,
        message: str,
        comment_id: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        """Dispatches a direct message via POST /v1/dm/send on PseudoGram API.
        
        Args:
            recipient_user_id: Authoritative user ID of the DM recipient.
            message: DM text content to send.
            comment_id: ID of the triggering comment.
            idempotency_key: Stable delivery-specific idempotency key.
            
        Returns:
            dict containing {"dm_id": "...", "status": "queued"} on HTTP 202 Accepted.
            
        Raises:
            PseudoGramBadRequestError: On HTTP 400 (non-retryable).
            PseudoGramRateLimitError: On HTTP 429 (retryable with Retry-After).
            PseudoGramServerError: On HTTP 500+ (retryable).
            PseudoGramNetworkError: On connection drop or timeout (retryable).
        """
        url = f"{self.base_url}/v1/dm/send"
        headers = {
            "X-API-Key": self.api_key,
            "Idempotency-Key": idempotency_key,
            "Content-Type": "application/json",
        }
        payload = {
            "recipient_user_id": recipient_user_id,
            "message": message,
            "comment_id": comment_id,
        }

        # Safe logging without exposing secret API key
        logger.info(
            "[PseudoGramClient] Sending DM to recipient='%s', comment_id='%s', idempotency_key='%s'",
            recipient_user_id,
            comment_id,
            idempotency_key,
        )

        close_after = False
        client = self._client
        if client is None:
            client = httpx.AsyncClient(timeout=10.0)
            close_after = True

        try:
            response = await client.post(url, json=payload, headers=headers)
        except (httpx.TimeoutException, httpx.NetworkError, httpx.RequestError) as net_err:
            logger.warning("[PseudoGramClient] Network or timeout error during send: %s", net_err)
            raise PseudoGramNetworkError(detail=str(net_err)) from net_err
        finally:
            if close_after:
                await client.aclose()

        # Handle Responses
        if response.status_code == 202:
            try:
                data = response.json()
            except Exception:
                data = {}
            dm_id = data.get("dm_id")
            logger.info("[PseudoGramClient] HTTP 202 Accepted: dm_id='%s'", dm_id)
            return {"dm_id": dm_id, "status": data.get("status", "queued")}

        elif response.status_code == 400:
            try:
                err_data = response.json()
                detail = err_data.get("detail", err_data.get("error", "invalid_request"))
            except Exception:
                detail = response.text
            logger.warning("[PseudoGramClient] HTTP 400 Bad Request (Non-retryable): %s", detail)
            raise PseudoGramBadRequestError(detail=detail)

        elif response.status_code == 429:
            # Parse Retry-After safely with fallback
            retry_after_hdr = response.headers.get("Retry-After")
            retry_after_val = 5.0  # Safe default fallback if missing or malformed
            if retry_after_hdr:
                try:
                    retry_after_val = max(1.0, float(retry_after_hdr.strip()))
                except ValueError:
                    logger.warning("[PseudoGramClient] Malformed Retry-After header: '%s'. Using fallback 5.0s", retry_after_hdr)
                    retry_after_val = 5.0

            try:
                err_data = response.json()
                detail = err_data.get("detail", err_data.get("error", "rate_limited"))
            except Exception:
                detail = response.text
            logger.warning("[PseudoGramClient] HTTP 429 Rate Limited (Retry-After: %ss): %s", retry_after_val, detail)
            raise PseudoGramRateLimitError(retry_after=retry_after_val, detail=detail)

        elif response.status_code >= 500:
            try:
                err_data = response.json()
                detail = err_data.get("detail", err_data.get("error", "internal_error"))
            except Exception:
                detail = response.text
            logger.warning("[PseudoGramClient] HTTP %s Server Error (Retryable): %s", response.status_code, detail)
            raise PseudoGramServerError(status_code=response.status_code, detail=detail)

        else:
            # Unexpected HTTP code
            logger.warning("[PseudoGramClient] Unexpected HTTP %s response: %s", response.status_code, response.text)
            raise PseudoGramServerError(status_code=response.status_code, detail=f"Unexpected status {response.status_code}")

    async def get_dm_status(self, dm_id: str) -> dict[str, Any]:
        """Queries delivery status via GET /v1/dm/{dm_id} on PseudoGram API.
        
        IMPORTANT: Read requests are unrestricted and do NOT consume the POST /v1/dm/send rate-limit budget.
        """
        url = f"{self.base_url}/v1/dm/{dm_id}"
        headers = {
            "X-API-Key": self.api_key,
        }
        close_after = False
        client = self._client
        if client is None:
            client = httpx.AsyncClient(timeout=10.0)
            close_after = True

        try:
            response = await client.get(url, headers=headers)
        except (httpx.TimeoutException, httpx.NetworkError, httpx.RequestError) as net_err:
            logger.warning("[PseudoGramClient] Network or timeout error during status query: %s", net_err)
            raise PseudoGramNetworkError(detail=str(net_err)) from net_err
        finally:
            if close_after:
                await client.aclose()

        if response.status_code == 200:
            try:
                return response.json()
            except Exception:
                return {"dm_id": dm_id, "status": "unknown"}
        elif response.status_code == 400:
            raise PseudoGramBadRequestError(detail=response.text)
        elif response.status_code == 429:
            raise PseudoGramRateLimitError(retry_after=5.0, detail="Rate limited")
        else:
            raise PseudoGramServerError(status_code=response.status_code, detail=response.text)
