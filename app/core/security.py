import hashlib
import hmac
from typing import Optional


def compute_signature(raw_body: bytes, secret: str) -> str:
    """Computes HMAC-SHA256 hex digest for a raw request body given a secret key."""
    return hmac.new(
        key=secret.encode("utf-8"),
        msg=raw_body,
        digestmod=hashlib.sha256,
    ).hexdigest()


def verify_webhook_signature(
    raw_body: bytes,
    signature_header: Optional[str],
    secret: str,
) -> bool:
    """Verifies HMAC-SHA256 signature from the X-PseudoGram-Signature header.
    
    Args:
        raw_body: The exact raw bytes of the incoming request body.
        signature_header: The value of the X-PseudoGram-Signature HTTP header.
                          Expected format: 'sha256=<hex_digest>' (or raw hex).
        secret: The shared secret key (e.g. PSEUDOGRAM_API_KEY).
        
    Returns:
        True if the signature matches via constant-time comparison; False otherwise.
    """
    if not signature_header or not secret:
        return False

    received_sig = signature_header.strip()
    if received_sig.lower().startswith("sha256="):
        received_sig = received_sig[7:].strip()

    expected_sig = compute_signature(raw_body=raw_body, secret=secret)
    return hmac.compare_digest(expected_sig.lower(), received_sig.lower())
