#!/usr/bin/env python3
"""Load polygon geometries from S3 GeoJSON files into catastro_actual.geom column.

Uses ijson for streaming JSON parsing — handles multi-GB files without loading
everything into memory.

Usage:
    python 09_load_geometries.py              # all comunas
    python 09_load_geometries.py 13101 13102  # specific comunas (SII codes)
"""

import json
import os
import sys
import time
from decimal import Decimal
import boto3
from boto3.s3.transfer import TransferConfig
import ijson
import psycopg
from pathlib import Path
from config import (
    S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET,
    DB_DSN,
)

CATALOG_PATH = Path(__file__).parent.parent / "backend" / "data" / "fase6_catalogo.json"
BATCH_SIZE = 2000


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )


def load_catalog():
    with open(CATALOG_PATH) as f:
        return json.load(f)


STAGING_DIR = "/tmp/geojson_staging"


def _json_default(obj):
    """Handle Decimal from ijson."""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def download_to_disk(s3, key):
    """Download S3 file to local disk with automatic retries."""
    os.makedirs(STAGING_DIR, exist_ok=True)
    local_path = os.path.join(STAGING_DIR, os.path.basename(key))
    s3.download_file(S3_BUCKET, key, local_path)
    return local_path


def stream_geometries(s3, key):
    """Download GeoJSON to disk, then stream-parse with ijson.

    Yields (geojson_str, comuna_int, manzana_int, predio_int) tuples.
    """
    local_path = download_to_disk(s3, key)
    try:
        with open(local_path, "rb") as f:
            for feature in ijson.items(f, "features.item"):
                geom = feature.get("geometry")
                if not geom:
                    continue
                props = feature.get("properties", {})
                v = props.get("v", "")
                parts = v.split("|")
                if len(parts) != 3:
                    continue
                try:
                    comuna = int(parts[0])
                    manzana = int(parts[1])
                    predio = int(parts[2])
                except ValueError:
                    continue

                yield (json.dumps(geom, default=_json_default), comuna, manzana, predio)
    finally:
        os.remove(local_path)


def update_batch(conn, batch):
    with conn.cursor() as cur:
        cur.executemany(
            "UPDATE catastro_actual SET geom = ST_GeomFromGeoJSON(%s) "
            "WHERE comuna = %s AND manzana = %s AND predio = %s",
            batch,
        )


def ensure_geom_column(conn):
    """Add geom column if it doesn't exist."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'catastro_actual' AND column_name = 'geom'
        """)
        if not cur.fetchone():
            print("Adding geom column to catastro_actual...")
            cur.execute("ALTER TABLE catastro_actual ADD COLUMN geom geometry(Geometry, 4326)")
            conn.commit()
            print("Column added.")
        else:
            print("geom column already exists.")


def main():
    specific_codes = set(sys.argv[1:]) if len(sys.argv) > 1 else None

    catalog = load_catalog()
    comunas = catalog["comunas"]

    if specific_codes:
        comunas = [c for c in comunas if str(c["cod"]) in specific_codes]
        print(f"Processing {len(comunas)} specific comunas: {specific_codes}")
    else:
        print(f"Processing all {len(comunas)} comunas")

    s3 = get_s3_client()
    total_updated = 0
    total_skipped = 0
    t0 = time.time()

    with psycopg.connect(DB_DSN) as conn:
        ensure_geom_column(conn)

        for i, entry in enumerate(comunas, 1):
            code = entry["cod"]
            name = entry["nombre"]
            s3_uri = entry["geojson"]
            key = s3_uri.replace(f"s3://{S3_BUCKET}/", "")

            try:
                head = s3.head_object(Bucket=S3_BUCKET, Key=key)
                size_mb = head["ContentLength"] / 1024 / 1024

                # Skip if this comuna already has geometries loaded
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) FROM catastro_actual WHERE comuna = %s AND geom IS NOT NULL LIMIT 1",
                        [code],
                    )
                    existing = cur.fetchone()[0]
                if existing > 0:
                    print(f"  [{i}/{len(comunas)}] {name} ({code}) — already has {existing:,} geoms, skipping")
                    continue

                print(f"  [{i}/{len(comunas)}] {name} ({code}) — {size_mb:.0f} MB — downloading...", end="", flush=True)

                batch = []
                comuna_count = 0
                for row in stream_geometries(s3, key):
                    batch.append(row)
                    if len(batch) >= BATCH_SIZE:
                        update_batch(conn, batch)
                        comuna_count += len(batch)
                        batch = []

                if batch:
                    update_batch(conn, batch)
                    comuna_count += len(batch)

                conn.commit()
                total_updated += comuna_count

                elapsed = time.time() - t0
                rate = total_updated / elapsed if elapsed > 0 else 0
                print(f" — {comuna_count:,} geoms | Total: {total_updated:,} | {rate:.0f}/s")

            except Exception as e:
                print(f" — ERROR: {e}")
                conn.rollback()
                total_skipped += 1
                continue

        # Build spatial index
        print("\nBuilding spatial index on geom column...")
        with conn.cursor() as cur:
            cur.execute("DROP INDEX IF EXISTS idx_catastro_actual_geom")
            cur.execute(
                "CREATE INDEX idx_catastro_actual_geom ON catastro_actual USING GIST (geom) "
                "WHERE geom IS NOT NULL"
            )
        conn.commit()
        print("Index created.")

    elapsed = time.time() - t0
    print(f"\nDone. {total_updated:,} geometries loaded in {elapsed:.1f}s")
    if total_skipped:
        print(f"Skipped {total_skipped} comunas due to errors")


if __name__ == "__main__":
    main()
