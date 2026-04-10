import logging
import httpx
import time
import json
import pathlib
import threading
import collections
from collections import OrderedDict
from fastapi import APIRouter, Query, Request, Header
from fastapi.responses import JSONResponse
from config import HERE_API_KEY, STATS_TOKEN

logger = logging.getLogger("geocode")

router = APIRouter()

# Rate limits
RATE_PER_MIN = 10
DAILY_PER_IP = 200
MONTHLY_GLOBAL = 25000

# Shared state file for cross-worker persistence
_STATE_FILE = pathlib.Path("/tmp/geocode_state.json")
_lock = threading.Lock()


def _load_state():
    try:
        if _STATE_FILE.exists():
            data = json.loads(_STATE_FILE.read_text())
            return data
    except Exception:
        pass
    return {"month": "", "month_count": 0, "daily": {}, "rate": {}}


def _save_state(state):
    try:
        _STATE_FILE.write_text(json.dumps(state))
    except Exception:
        pass


def _get_client_ip(request: Request) -> str:
    return request.client.host if request.client else "unknown"


def _check_rate_limit(ip: str) -> str | None:
    now = time.time()
    today = time.strftime("%Y-%m-%d")
    month = time.strftime("%Y-%m")

    with _lock:
        state = _load_state()

        if state["month"] != month:
            state["month"] = month
            state["month_count"] = 0
        if state["month_count"] >= MONTHLY_GLOBAL:
            return "Limite mensual de geocoding alcanzado. Intenta el proximo mes."

        daily = state.get("daily", {})
        daily = {k: v for k, v in daily.items() if v.get("day") == today}
        if ip not in daily:
            daily[ip] = {"day": today, "count": 0}
        if daily[ip]["count"] >= DAILY_PER_IP:
            return "Limite diario alcanzado. Maximo 200 busquedas por dia."

        rate = state.get("rate", {})
        if ip not in rate:
            rate[ip] = []
        rate[ip] = [t for t in rate[ip] if t > now - 60]
        if len(rate[ip]) >= RATE_PER_MIN:
            return "Demasiadas solicitudes. Espera un momento."

        state["daily"] = daily
        state["rate"] = rate
        _save_state(state)

    return None


def _count_request(ip: str):
    now = time.time()
    with _lock:
        state = _load_state()
        state["month_count"] = state.get("month_count", 0) + 1
        daily = state.get("daily", {})
        if ip in daily:
            daily[ip]["count"] = daily[ip].get("count", 0) + 1
        rate = state.get("rate", {})
        if ip not in rate:
            rate[ip] = []
        rate[ip].append(now)
        state["daily"] = daily
        state["rate"] = rate
        _save_state(state)


# Cache (in-memory per worker, bounded)
MAX_CACHE = 2000
CACHE_TTL = 3600


class BoundedCache:
    def __init__(self, maxsize=MAX_CACHE):
        self._data = OrderedDict()
        self._maxsize = maxsize

    def get(self, key):
        if key in self._data:
            val, ts = self._data[key]
            if time.time() - ts < CACHE_TTL:
                self._data.move_to_end(key)
                return val
            del self._data[key]
        return None

    def set(self, key, val):
        self._data[key] = (val, time.time())
        if len(self._data) > self._maxsize:
            self._data.popitem(last=False)


_geocode_cache = BoundedCache()
_revgeo_cache = BoundedCache()


def _grid_key(lat: float, lon: float) -> str:
    return f"{round(lat, 4)},{round(lon, 4)}"


def _parse_here_item(item: dict) -> dict:
    addr = item.get("address", {})
    return {
        "display_name": item.get("title", ""),
        "address": {
            "road": addr.get("street", ""),
            "house_number": addr.get("houseNumber", ""),
            "city": addr.get("city", ""),
            "town": addr.get("city", ""),
            "suburb": addr.get("district", ""),
        },
    }


@router.get("/geocode")
async def geocode(request: Request, q: str = Query(..., min_length=2), limit: int = Query(6, ge=1, le=10)):
    ip = _get_client_ip(request)
    err = _check_rate_limit(ip)
    if err:
        return JSONResponse(status_code=429, content={"error": err})

    cache_key = q.strip().lower()
    cached = _geocode_cache.get(cache_key)
    if cached is not None:
        return cached

    async with httpx.AsyncClient() as client:
        resp = await client.get(
            "https://geocode.search.hereapi.com/v1/geocode",
            params={"q": q + ", Chile", "limit": limit, "lang": "es", "in": "countryCode:CHL", "apiKey": HERE_API_KEY},
            timeout=10,
        )
        if resp.status_code != 200:
            logger.error(f"HERE geocode error: {resp.status_code} {resp.text[:200]}")
            return JSONResponse(status_code=502, content={"error": "Error en servicio de geocoding"})
        data = resp.json()

    _count_request(ip)

    results = []
    for item in data.get("items", []):
        parsed = _parse_here_item(item)
        parsed["lat"] = str(item["position"]["lat"])
        parsed["lon"] = str(item["position"]["lng"])
        results.append(parsed)

    _geocode_cache.set(cache_key, results)
    return results


@router.get("/revgeocode")
async def revgeocode(request: Request, lat: float = Query(...), lon: float = Query(...)):
    ip = _get_client_ip(request)
    err = _check_rate_limit(ip)
    if err:
        return JSONResponse(status_code=429, content={"error": err})

    gk = _grid_key(lat, lon)
    cached = _revgeo_cache.get(gk)
    if cached is not None:
        return cached

    async with httpx.AsyncClient() as client:
        resp = await client.get(
            "https://revgeocode.search.hereapi.com/v1/revgeocode",
            params={"at": f"{lat},{lon}", "lang": "es", "apiKey": HERE_API_KEY},
            timeout=10,
        )
        if resp.status_code != 200:
            logger.error(f"HERE revgeocode error: {resp.status_code} {resp.text[:200]}")
            return JSONResponse(status_code=502, content={"error": "Error en servicio de geocoding"})
        data = resp.json()

    _count_request(ip)

    item = (data.get("items") or [None])[0]
    if not item:
        return None

    result = _parse_here_item(item)
    _revgeo_cache.set(gk, result)
    return result


@router.get("/geocode/stats")
async def geocode_stats(authorization: str = Header(None)):
    """Usage stats — requires Bearer token."""
    expected = f"Bearer {STATS_TOKEN}"
    if not authorization or authorization != expected:
        return JSONResponse(status_code=401, content={"error": "No autorizado"})

    state = _load_state()
    return {
        "month": state.get("month", ""),
        "monthly_used": state.get("month_count", 0),
        "monthly_limit": MONTHLY_GLOBAL,
        "remaining": MONTHLY_GLOBAL - state.get("month_count", 0),
        "active_ips": len(state.get("daily", {})),
    }
