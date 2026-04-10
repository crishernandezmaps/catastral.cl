"""S3 client for marketplace downloads — fase4v2 catalog."""
import json
import logging
import boto3
from botocore.config import Config as BotoConfig
from config import S3_ENDPOINT, S3_REGION, S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET
from lib.comunas import find_comuna
from pathlib import Path

logger = logging.getLogger("s3")

_DATA_DIR = Path(__file__).parent.parent / "data"

# Fase4v2 catalog — loaded once at import
_catalog = json.loads((_DATA_DIR / "catalog_fase4v2.json").read_text())
_catalog_by_code: dict[str, dict] = {c["codigo"]: c for c in _catalog["comunas"]}
_available_codes: list[str] = sorted(_catalog_by_code.keys())

# Lazy S3 client
_s3 = None


def _get_s3():
    global _s3
    if _s3 is None:
        _s3 = boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT,
            region_name=S3_REGION,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
            config=BotoConfig(signature_version="s3v4"),
        )
    return _s3


def get_signed_url(key: str, filename: str | None = None, expires_in: int = 900) -> str:
    params = {"Bucket": S3_BUCKET, "Key": key}
    if filename:
        params["ResponseContentDisposition"] = f'attachment; filename="{filename}"'
    return _get_s3().generate_presigned_url(
        "get_object", Params=params, ExpiresIn=expires_in
    )


def get_available_comunas() -> dict:
    """Return all comunas with data in fase4v2."""
    return {"available": _available_codes, "stats": {}}


def get_links_for_comuna(comuna_id: str) -> dict:
    """Generate presigned URLs for a comuna's fase4v2 files (CSV, CSV raw, GPKG)."""
    entry = _catalog_by_code.get(comuna_id)
    if not entry:
        return {}

    comuna = find_comuna(comuna_id)
    friendly = comuna["nombre"].replace(" ", "_") if comuna else comuna_id

    archivos = entry["archivos"]
    links = {}

    if archivos.get("csv"):
        links["csv"] = [{
            "url": get_signed_url(archivos["csv"]["key"], f"{friendly}_{comuna_id}.csv"),
        }]

    if archivos.get("csv_raw"):
        links["csv_raw"] = [{
            "url": get_signed_url(archivos["csv_raw"]["key"], f"{friendly}_{comuna_id}_raw.csv"),
        }]

    if archivos.get("gpkg"):
        links["gpkg"] = [{
            "url": get_signed_url(archivos["gpkg"]["key"], f"{friendly}_{comuna_id}.gpkg"),
        }]

    return links
