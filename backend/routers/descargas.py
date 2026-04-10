import time
import collections
import boto3
from botocore.config import Config
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from config import S3_ENDPOINT, S3_REGION, S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET

router = APIRouter()

# Rate limit: max 10 presigned URL generations per hour per IP
_dl_rate: dict[str, collections.deque] = {}
DL_LIMIT = 10
DL_WINDOW = 3600

def _check_dl_rate(request: Request) -> str | None:
    ip = request.client.host if request.client else "unknown"
    now = time.time()
    if ip not in _dl_rate:
        _dl_rate[ip] = collections.deque()
    q = _dl_rate[ip]
    while q and q[0] < now - DL_WINDOW:
        q.popleft()
    if len(q) >= DL_LIMIT:
        return "Limite de descargas alcanzado. Maximo 10 por hora."
    q.append(now)
    return None

S3_PREFIX = "catastro_historico/output"

PERIODS = [
    {"id": "2018_1", "anio": 2018, "semestre": 1},
    {"id": "2018_2", "anio": 2018, "semestre": 2},
    {"id": "2019_1", "anio": 2019, "semestre": 1},
    {"id": "2019_2", "anio": 2019, "semestre": 2},
    {"id": "2020_1", "anio": 2020, "semestre": 1},
    {"id": "2020_2", "anio": 2020, "semestre": 2},
    {"id": "2021_1", "anio": 2021, "semestre": 1},
    {"id": "2021_2", "anio": 2021, "semestre": 2},
    {"id": "2022_1", "anio": 2022, "semestre": 1},
    {"id": "2022_2", "anio": 2022, "semestre": 2},
    {"id": "2023_1", "anio": 2023, "semestre": 1},
    {"id": "2023_2", "anio": 2023, "semestre": 2},
    {"id": "2024_1", "anio": 2024, "semestre": 1},
    {"id": "2024_2", "anio": 2024, "semestre": 2},
    {"id": "2025_1", "anio": 2025, "semestre": 1},
    {"id": "2025_2", "anio": 2025, "semestre": 2},
]

FILE_SIZES = {
    "2018_1": 1248887127, "2018_2": 1280417663,
    "2019_1": 1307587568, "2019_2": 1323492400,
    "2020_1": 1346509235, "2020_2": 1363190503,
    "2021_1": 1382834402, "2021_2": 1396730010,
    "2022_1": 1421312914, "2022_2": 1438153057,
    "2023_1": 1474148471, "2023_2": 1488339448,
    "2024_1": 1552287504, "2024_2": 1567654285,
    "2025_1": 1596637172, "2025_2": 1609124099,
}

ROW_COUNTS = {
    "2018_1": 7610930, "2018_2": 7786110,
    "2019_1": 7941222, "2019_2": 8028165,
    "2020_1": 8156246, "2020_2": 8247999,
    "2021_1": 8360299, "2021_2": 8437439,
    "2022_1": 8565453, "2022_2": 8656530,
    "2023_1": 8862697, "2023_2": 8940358,
    "2024_1": 9103135, "2024_2": 9183865,
    "2025_1": 9342943, "2025_2": 9407339,
}

_s3 = None
def get_s3():
    global _s3
    if _s3 is None:
        _s3 = boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT,
            region_name=S3_REGION,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
            config=Config(signature_version="s3v4"),
        )
    return _s3


@router.get("/descargas")
def list_descargas():
    """List all available downloads with metadata."""
    total_size = sum(FILE_SIZES.values())
    total_rows = sum(ROW_COUNTS.values())
    return {
        "total_archivos": len(PERIODS),
        "total_size_gb": round(total_size / 1e9, 1),
        "total_registros": total_rows,
        "columnas": 39,
        "formato": "CSV (UTF-8, comma-separated)",
        "archivos": [
            {
                "periodo": f"{p['anio']}S{p['semestre']}",
                "id": p["id"],
                "anio": p["anio"],
                "semestre": p["semestre"],
                "size_bytes": FILE_SIZES.get(p["id"], 0),
                "size_mb": round(FILE_SIZES.get(p["id"], 0) / 1e6, 0),
                "registros": ROW_COUNTS.get(p["id"], 0),
            }
            for p in PERIODS
        ],
    }


@router.get("/descargas/{periodo_id}/url")
def get_download_url(periodo_id: str, request: Request):
    """Generate a presigned URL for downloading a specific period CSV."""
    err = _check_dl_rate(request)
    if err:
        return JSONResponse(status_code=429, content={"error": err})
    valid_ids = {p["id"] for p in PERIODS}
    if periodo_id not in valid_ids:
        return {"error": "Periodo no valido"}

    s3 = get_s3()
    key = f"{S3_PREFIX}/catastro_{periodo_id}.csv"

    url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": S3_BUCKET, "Key": key},
        ExpiresIn=900,
    )

    return {
        "periodo": periodo_id,
        "url": url,
        "expires_in": 900,
        "filename": f"catastro_{periodo_id}.csv",
    }
