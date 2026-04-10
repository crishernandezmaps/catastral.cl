"""Flow.cl payment gateway client — ported from prediosChile flow.js."""
import hmac
import hashlib
import httpx
from config import FLOW_API_KEY, FLOW_SECRET_KEY, FLOW_BASE_URL


def sign_flow_params(params: dict) -> str:
    """HMAC-SHA256 signature required by Flow.cl."""
    sorted_keys = sorted(params.keys())
    to_sign = "".join(f"{k}{params[k]}" for k in sorted_keys)
    return hmac.new(
        FLOW_SECRET_KEY.encode(), to_sign.encode(), hashlib.sha256
    ).hexdigest()


async def create_payment(payment_params: dict) -> dict:
    """Create a Flow.cl payment intent."""
    params = {"apiKey": FLOW_API_KEY, **payment_params}
    params["s"] = sign_flow_params(params)

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{FLOW_BASE_URL}/payment/create",
            data=params,  # form-encoded, not JSON
            timeout=15,
        )
        result = resp.json()
        if not resp.is_success:
            raise Exception(f"Flow Error: {result.get('message')} (Code: {result.get('code')})")
        return result


async def get_payment_status(token: str) -> dict:
    """Check payment status via Flow.cl API."""
    params = {"apiKey": FLOW_API_KEY, "token": token}
    params["s"] = sign_flow_params(params)

    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{FLOW_BASE_URL}/payment/getStatus",
            params=params,
            timeout=15,
        )
        result = resp.json()
        if not resp.is_success:
            raise Exception(f"Flow status check failed: {result.get('message')}")
        return result
