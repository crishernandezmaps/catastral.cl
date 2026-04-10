"""OTP passwordless auth — ported from prediosChile auth.js."""
import secrets
import logging
import jwt
from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from db import pool
from config import JWT_SECRET, ADMIN_EMAIL, FRONTEND_URL
from lib.email import send_otp_email

logger = logging.getLogger("auth")

router = APIRouter()

# Simple in-memory rate limiting (per IP)
import time
import collections

_otp_request_rate: dict[str, collections.deque] = {}
_otp_verify_rate: dict[str, collections.deque] = {}


def _check_rate(store: dict, ip: str, max_count: int, window: int) -> bool:
    now = time.time()
    if ip not in store:
        store[ip] = collections.deque()
    q = store[ip]
    while q and q[0] < now - window:
        q.popleft()
    if len(q) >= max_count:
        return False
    q.append(now)
    return True


class RequestCodeBody(BaseModel):
    email: str


class VerifyCodeBody(BaseModel):
    email: str
    code: str


@router.post("/auth/request-code")
async def request_code(body: RequestCodeBody, request: Request):
    ip = request.client.host if request.client else "unknown"
    if not _check_rate(_otp_request_rate, ip, 5, 900):  # 5 per 15 min
        return JSONResponse(status_code=429, content={"error": "Demasiados intentos. Espera 15 minutos."})

    email = body.email.lower().strip()
    if not email:
        return JSONResponse(status_code=400, content={"error": "Email requerido"})

    with pool.connection() as conn:
        # Upsert user
        conn.execute(
            "INSERT INTO users (email) VALUES (%s) ON CONFLICT (email) DO NOTHING",
            [email],
        )
        row = conn.execute("SELECT id FROM users WHERE email = %s", [email]).fetchone()
        user_id = row[0]

        # Invalidate previous OTPs
        conn.execute("DELETE FROM otp_codes WHERE email = %s", [email])

        # Generate new OTP
        code = str(secrets.randbelow(900000) + 100000)  # 6 digits
        expires_at = datetime.now(timezone.utc) + timedelta(minutes=10)

        conn.execute(
            "INSERT INTO otp_codes (user_id, email, code, expires_at) VALUES (%s, %s, %s, %s)",
            [user_id, email, code, expires_at],
        )

    try:
        await send_otp_email(email, code)
    except Exception as e:
        logger.error(f"Failed to send OTP email: {e}")

    return {"success": True, "message": "Codigo enviado si el correo existe."}


@router.post("/auth/verify-code")
async def verify_code(body: VerifyCodeBody, request: Request, response: Response):
    ip = request.client.host if request.client else "unknown"
    if not _check_rate(_otp_verify_rate, ip, 10, 900):  # 10 per 15 min
        return JSONResponse(status_code=429, content={"error": "Demasiados intentos de verificacion. Espera 15 minutos."})

    email = body.email.lower().strip()
    code = body.code.strip()

    if not email or not code:
        return JSONResponse(status_code=400, content={"error": "Faltan parametros"})

    with pool.connection() as conn:
        row = conn.execute(
            """SELECT id FROM otp_codes
               WHERE email = %s AND code = %s AND expires_at > NOW()
               ORDER BY created_at DESC LIMIT 1""",
            [email, code],
        ).fetchone()

        if not row:
            return JSONResponse(status_code=401, content={"error": "Codigo invalido o expirado"})

        # Delete all OTPs for this email (single-use)
        conn.execute("DELETE FROM otp_codes WHERE email = %s", [email])

        user = conn.execute("SELECT id FROM users WHERE email = %s", [email]).fetchone()

    is_admin = email == ADMIN_EMAIL
    token = jwt.encode(
        {
            "userId": user[0],
            "email": email,
            "role": "admin" if is_admin else "user",
            "exp": datetime.now(timezone.utc) + timedelta(hours=24),
            "iss": "catastro-chile",
        },
        JWT_SECRET,
        algorithm="HS256",
    )

    is_prod = FRONTEND_URL.startswith("https")
    response.set_cookie(
        key="tremen_session",
        value=token,
        httponly=True,
        secure=is_prod,
        samesite="strict" if is_prod else "lax",
        max_age=24 * 60 * 60,
        path="/",
    )

    return {"success": True, "token": token, "user": {"email": email, "role": "admin" if is_admin else "user"}}


@router.post("/auth/logout")
async def logout(response: Response):
    response.delete_cookie("tremen_session", path="/")
    return {"success": True}
