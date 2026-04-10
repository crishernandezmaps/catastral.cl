"""JWT auth middleware — ported from prediosChile middleware/auth.js."""
import jwt
from fastapi import Request, HTTPException, Depends
from config import JWT_SECRET, ADMIN_EMAIL


def _get_token(request: Request) -> str | None:
    """Extract JWT from httpOnly cookie."""
    return request.cookies.get("tremen_session")


def _decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=["HS256"], issuer="catastro-chile")
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expirado")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Token invalido")


async def get_current_user(request: Request) -> dict:
    """FastAPI dependency: returns user dict or raises 401."""
    token = _get_token(request)
    if not token:
        raise HTTPException(status_code=401, detail="No autenticado")
    payload = _decode_token(token)
    return {
        "id": payload.get("userId"),
        "email": payload.get("email"),
        "role": payload.get("role", "user"),
    }


async def get_admin_user(request: Request) -> dict:
    """FastAPI dependency: returns admin user or raises 403."""
    user = await get_current_user(request)
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Acceso denegado")
    return user
