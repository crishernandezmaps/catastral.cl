"""Anonymous LinkedIn share-to-download — no login required.

Flow:
1. User shares on LinkedIn, pastes post URL
2. Backend validates URL, extracts LinkedIn username, creates share_token
3. Sets httpOnly cookie `tremen_share` (1 year)
4. Downloads check this cookie

Recovery: user who cleared cookies provides their LinkedIn username → we return their existing token.
"""
import logging
import re
import secrets
from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from db import pool

logger = logging.getLogger("sharing")

router = APIRouter()

# LinkedIn post URL patterns
LINKEDIN_URL_PATTERN = re.compile(
    r"^https?://(www\.)?linkedin\.com/"
    r"(posts/[^/\s]+|feed/update/urn:li:activity:\d+|pulse/[^/\s]+)",
    re.IGNORECASE,
)

# Extract username from /posts/USERNAME_... URL format
USERNAME_PATTERN = re.compile(r"linkedin\.com/posts/([^_/]+)_", re.IGNORECASE)

COOKIE_NAME = "tremen_share"
COOKIE_MAX_AGE = 365 * 24 * 60 * 60  # 1 year


def _set_share_cookie(response: Response, token: str):
    response.set_cookie(
        key=COOKIE_NAME,
        value=token,
        max_age=COOKIE_MAX_AGE,
        httponly=True,
        secure=True,
        samesite="lax",
        path="/",
    )


def _extract_username(post_url: str) -> str | None:
    m = USERNAME_PATTERN.search(post_url)
    return m.group(1).lower() if m else None


def _strip_tracking(url: str) -> str:
    """Remove UTM/tracking params from LinkedIn URLs for dedup."""
    return url.split("?")[0].rstrip("/")


class ConfirmShareBody(BaseModel):
    postUrl: str
    linkedinUsername: str | None = None


class RecoverBody(BaseModel):
    linkedinUsername: str


@router.post("/share/confirm")
def confirm_share(body: ConfirmShareBody, response: Response):
    """Validate LinkedIn post URL and grant share token (public, no auth)."""
    post_url = _strip_tracking(body.postUrl.strip())

    if not LINKEDIN_URL_PATTERN.match(post_url):
        return JSONResponse(
            status_code=400,
            content={
                "error": "URL invalido",
                "detail": "El URL debe ser de un post de LinkedIn. Ejemplo: https://www.linkedin.com/posts/tu-usuario_...",
            },
        )

    # Try to extract username from URL; otherwise use the one provided by user (optional)
    username = _extract_username(post_url)
    if not username and body.linkedinUsername:
        raw = body.linkedinUsername.strip().lower().lstrip("@")
        m = re.search(r"linkedin\.com/in/([^/\s?]+)", raw, re.IGNORECASE)
        username = m.group(1).lower() if m else raw

    # Username is optional — the post URL is the primary identifier
    if not username:
        username = None

    with pool.connection() as conn:
        # Check if this post URL already has a token
        existing = conn.execute(
            "SELECT token, linkedin_username FROM share_tokens WHERE post_url = %s",
            [post_url],
        ).fetchone()

        if existing:
            token = existing[0]
            logger.info(f"Existing share token reused for {post_url}")
        else:
            token = secrets.token_urlsafe(32)
            conn.execute(
                "INSERT INTO share_tokens (token, post_url, linkedin_username) VALUES (%s, %s, %s)",
                [token, post_url, username],
            )
            logger.info(f"New share token created: username={username} url={post_url}")

    _set_share_cookie(response, token)
    return {"success": True, "username": username}


@router.post("/share/recover")
def recover_share(body: RecoverBody, response: Response):
    """Recover share token by LinkedIn username (for users who cleared cookies)."""
    username = body.linkedinUsername.strip().lower().lstrip("@")

    # Allow URL-like input: https://linkedin.com/in/username
    m = re.search(r"linkedin\.com/in/([^/\s?]+)", username, re.IGNORECASE)
    if m:
        username = m.group(1).lower()

    if not username or len(username) < 2:
        return JSONResponse(
            status_code=400,
            content={"error": "Username invalido"},
        )

    with pool.connection() as conn:
        row = conn.execute(
            "SELECT token, post_url FROM share_tokens WHERE linkedin_username = %s ORDER BY created_at DESC LIMIT 1",
            [username],
        ).fetchone()

    if not row:
        return JSONResponse(
            status_code=404,
            content={"error": "No encontramos un registro con ese usuario. Comparte en LinkedIn para desbloquear."},
        )

    _set_share_cookie(response, row[0])
    return {"success": True, "post_url": row[1]}


@router.post("/share/trust")
def trust_share(response: Response):
    """Honor-based bypass for returning users who already shared."""
    token = secrets.token_urlsafe(32)

    with pool.connection() as conn:
        conn.execute(
            "INSERT INTO share_tokens (token, post_url, linkedin_username) VALUES (%s, %s, %s)",
            [token, "trust://returning-user", None],
        )

    _set_share_cookie(response, token)
    logger.info("Trust-based share token created (returning user)")
    return {"success": True}


@router.get("/share/status")
def share_status(request: Request):
    """Check if the browser has a valid share token."""
    token = request.cookies.get(COOKIE_NAME)
    if not token:
        return {"shared": False}

    with pool.connection() as conn:
        row = conn.execute(
            "SELECT linkedin_username FROM share_tokens WHERE token = %s",
            [token],
        ).fetchone()

    if not row:
        return {"shared": False}

    return {"shared": True, "username": row[0]}


def get_share_token_id(request: Request) -> int | None:
    """Helper for other routers to check if current request has share access.

    Returns share_tokens.id if valid, None otherwise.
    """
    token = request.cookies.get(COOKIE_NAME)
    if not token:
        return None

    with pool.connection() as conn:
        row = conn.execute(
            "SELECT id FROM share_tokens WHERE token = %s", [token]
        ).fetchone()

    return row[0] if row else None
