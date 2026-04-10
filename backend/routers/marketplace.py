"""Marketplace routes — availability, secure downloads."""
import logging
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from db import pool
from lib.s3 import get_available_comunas, get_links_for_comuna, _catalog_by_code
from lib.comunas import get_comunas
from routers.sharing import get_share_token_id

logger = logging.getLogger("marketplace")

router = APIRouter()


@router.get("/catalog")
def catalog():
    """Public product catalog."""
    return get_comunas()


@router.get("/comuna-stats")
def comuna_stats():
    """Per-comuna coverage stats from fase4v2 catalog."""
    result = {}
    for code, entry in _catalog_by_code.items():
        predios = entry.get("predios", {})
        total = predios.get("total_csv", 0)
        poligonos = predios.get("total_poligonos", 0)
        result[code] = {
            "total": total,
            "cobertura_pct": round(poligonos * 100 / total, 1) if total else 0,
        }
    return {"success": True, "stats": result}


@router.get("/availability")
def availability():
    """Public: which comunas have data in S3."""
    try:
        return get_available_comunas()
    except Exception as e:
        logger.error(f"Availability check error: {e}")
        return {"available": [], "stats": {}}


@router.get("/secure-download/{comuna_id}")
def secure_download(comuna_id: str, request: Request):
    """Get S3 presigned URLs — requires share_token cookie or admin session."""
    import jwt
    from config import JWT_SECRET
    is_admin = False
    try:
        token = request.cookies.get("tremen_session")
        if token:
            payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"], issuer="catastro-chile")
            is_admin = payload.get("role") == "admin"
    except Exception:
        pass

    share_token_id = get_share_token_id(request)
    if share_token_id is None and not is_admin:
        return JSONResponse(
            status_code=403,
            content={"error": "Comparte en LinkedIn para desbloquear las descargas"},
        )

    if share_token_id:
        with pool.connection() as conn:
            conn.execute(
                "UPDATE share_tokens SET downloads_count = downloads_count + 1 WHERE id = %s",
                [share_token_id],
            )

    links = get_links_for_comuna(comuna_id)
    return {"success": True, "links": links}
