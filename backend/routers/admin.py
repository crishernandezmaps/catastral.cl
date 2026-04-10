"""Admin routes — ported from prediosChile admin.js."""
import logging
from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional
from db import pool
from lib.comunas import find_comuna
from lib.email import send_grant_notification
from middleware.auth import get_admin_user

logger = logging.getLogger("admin")

router = APIRouter()


class GrantBody(BaseModel):
    email: str
    bundleId: str
    durationDays: Optional[str] = None


class DomainGrantBody(BaseModel):
    domain: str
    durationDays: Optional[str] = None


# ── Grants ──

@router.get("/admin/grants")
def list_grants(admin: dict = Depends(get_admin_user)):
    with pool.connection() as conn:
        rows = conn.execute(
            """SELECT p.id, u.email, p.external_reference, p.created_at, p.expires_at
               FROM purchases p
               JOIN users u ON p.user_id = u.id
               WHERE p.payment_status = 'granted'
               ORDER BY p.created_at DESC"""
        ).fetchall()
    return {
        "success": True,
        "grants": [
            {"id": r[0], "email": r[1], "external_reference": r[2],
             "created_at": r[3].isoformat() if r[3] else None,
             "expires_at": r[4].isoformat() if r[4] else None}
            for r in rows
        ],
    }


@router.post("/admin/grants")
async def create_grant(body: GrantBody, admin: dict = Depends(get_admin_user)):
    email = body.email.lower().strip()
    if not email or not body.bundleId:
        return JSONResponse(status_code=400, content={"error": "Faltan parametros"})

    expires_at = None
    if body.durationDays and body.durationDays != "always":
        try:
            days = int(body.durationDays)
            expires_at = datetime.now(timezone.utc) + timedelta(days=days)
        except ValueError:
            pass

    with pool.connection() as conn:
        conn.execute("INSERT INTO users (email) VALUES (%s) ON CONFLICT (email) DO NOTHING", [email])
        user = conn.execute("SELECT id FROM users WHERE email = %s", [email]).fetchone()

        import time, random
        dummy_pref = f"GRANT_{int(time.time())}_{random.randint(0, 999)}"

        conn.execute(
            """INSERT INTO purchases (user_id, preference_id, external_reference, payment_status, expires_at)
               VALUES (%s, %s, %s, 'granted', %s)""",
            [user[0], dummy_pref, body.bundleId, expires_at],
        )

    matched = find_comuna(body.bundleId)
    package_name = matched["nombre"] if matched else body.bundleId

    duration_map = {"1": "24 Horas", "7": "7 Dias", "30": "30 Dias", "always": "Acceso Permanente"}
    duration_text = duration_map.get(body.durationDays, f"{body.durationDays} Dias")

    try:
        await send_grant_notification(email, package_name, duration_text)
    except Exception as e:
        logger.error(f"Failed to send grant notification: {e}")

    return {"success": True, "message": f"Acceso otorgado a {email}"}


@router.delete("/admin/grants/{grant_id}")
def revoke_grant(grant_id: int, admin: dict = Depends(get_admin_user)):
    with pool.connection() as conn:
        cur = conn.execute(
            "DELETE FROM purchases WHERE id = %s AND payment_status = 'granted'",
            [grant_id],
        )
        if cur.rowcount == 0:
            return JSONResponse(status_code=404, content={"error": "Acceso manual no encontrado"})
    return {"success": True, "message": "Acceso revocado"}


# ── Purchases ──

@router.get("/admin/purchases")
def list_purchases(admin: dict = Depends(get_admin_user)):
    with pool.connection() as conn:
        rows = conn.execute(
            """SELECT p.id, u.email, p.external_reference, p.created_at, p.preference_id, p.amount, p.payer_rut
               FROM purchases p
               JOIN users u ON p.user_id = u.id
               WHERE p.payment_status = 'approved'
               ORDER BY p.created_at DESC"""
        ).fetchall()
    return {
        "success": True,
        "purchases": [
            {"id": r[0], "email": r[1], "external_reference": r[2],
             "created_at": r[3].isoformat() if r[3] else None,
             "preference_id": r[4], "amount": r[5], "payer_rut": r[6]}
            for r in rows
        ],
    }


# ── Users ──

@router.get("/admin/users")
def list_users(admin: dict = Depends(get_admin_user)):
    with pool.connection() as conn:
        rows = conn.execute(
            """SELECT u.id, u.email, u.created_at,
                  (SELECT COUNT(*) FROM purchases WHERE user_id = u.id AND payment_status = 'approved') AS purchase_count,
                  (SELECT COUNT(*) FROM purchases WHERE user_id = u.id AND payment_status = 'granted') AS grant_count
               FROM users u
               ORDER BY u.created_at DESC"""
        ).fetchall()
    return {
        "success": True,
        "users": [
            {"id": r[0], "email": r[1],
             "created_at": r[2].isoformat() if r[2] else None,
             "purchase_count": r[3], "grant_count": r[4]}
            for r in rows
        ],
    }


@router.delete("/admin/users/{user_id}")
def delete_user(user_id: int, admin: dict = Depends(get_admin_user)):
    with pool.connection() as conn:
        user = conn.execute("SELECT id, email FROM users WHERE id = %s", [user_id]).fetchone()
        if not user:
            return JSONResponse(status_code=404, content={"error": "Usuario no encontrado"})
        if user[1] == admin["email"]:
            return JSONResponse(status_code=400, content={"error": "No puedes eliminarte a ti mismo"})
        conn.execute("DELETE FROM users WHERE id = %s", [user_id])
    return {"success": True, "message": f"Usuario {user[1]} eliminado"}


# ── Domain Grants ──

@router.get("/admin/domain-grants")
def list_domain_grants(admin: dict = Depends(get_admin_user)):
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT id, domain, created_by, expires_at, created_at FROM domain_grants ORDER BY created_at DESC"
        ).fetchall()
    return {
        "success": True,
        "domainGrants": [
            {"id": r[0], "domain": r[1], "created_by": r[2],
             "expires_at": r[3].isoformat() if r[3] else None,
             "created_at": r[4].isoformat() if r[4] else None}
            for r in rows
        ],
    }


@router.post("/admin/domain-grants")
def create_domain_grant(body: DomainGrantBody, admin: dict = Depends(get_admin_user)):
    import re
    domain = body.domain.lower().strip().lstrip("@")
    if not re.match(r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z]{2,})+$", domain):
        return JSONResponse(status_code=400, content={"error": "Formato de dominio invalido"})

    expires_at = None
    if body.durationDays and body.durationDays != "always":
        try:
            days = int(body.durationDays)
            expires_at = datetime.now(timezone.utc) + timedelta(days=days)
        except ValueError:
            pass

    with pool.connection() as conn:
        existing = conn.execute("SELECT id FROM domain_grants WHERE domain = %s", [domain]).fetchone()
        if existing:
            return JSONResponse(status_code=409, content={"error": f"El dominio {domain} ya esta autorizado"})

        conn.execute(
            "INSERT INTO domain_grants (domain, created_by, expires_at) VALUES (%s, %s, %s)",
            [domain, admin["email"], expires_at],
        )

    return {"success": True, "message": f"Dominio @{domain} autorizado"}


@router.delete("/admin/domain-grants/{grant_id}")
def revoke_domain_grant(grant_id: int, admin: dict = Depends(get_admin_user)):
    with pool.connection() as conn:
        cur = conn.execute("DELETE FROM domain_grants WHERE id = %s", [grant_id])
        if cur.rowcount == 0:
            return JSONResponse(status_code=404, content={"error": "Dominio autorizado no encontrado"})
    return {"success": True, "message": "Acceso de dominio revocado"}


# ── LinkedIn Shares ──

@router.get("/admin/shares")
def list_shares(admin: dict = Depends(get_admin_user)):
    """List all LinkedIn shares with their URLs, usernames and download counts."""
    with pool.connection() as conn:
        rows = conn.execute(
            """SELECT id, post_url, linkedin_username, downloads_count, created_at
               FROM share_tokens
               ORDER BY created_at DESC"""
        ).fetchall()

        stats = conn.execute(
            """SELECT COUNT(*) as total_shares, COALESCE(SUM(downloads_count), 0) as total_downloads
               FROM share_tokens"""
        ).fetchone()

    return {
        "success": True,
        "stats": {
            "total_shares": stats[0],
            "total_downloads": stats[1],
        },
        "shares": [
            {
                "id": r[0],
                "post_url": r[1],
                "linkedin_username": r[2],
                "downloads_count": r[3],
                "created_at": r[4].isoformat() if r[4] else None,
            }
            for r in rows
        ],
    }


@router.delete("/admin/shares/{share_id}")
def revoke_share(share_id: int, admin: dict = Depends(get_admin_user)):
    """Revoke a share token (removes access for that browser)."""
    with pool.connection() as conn:
        cur = conn.execute("DELETE FROM share_tokens WHERE id = %s", [share_id])
        if cur.rowcount == 0:
            return JSONResponse(status_code=404, content={"error": "Share no encontrado"})
    return {"success": True, "message": "Share revocado"}
