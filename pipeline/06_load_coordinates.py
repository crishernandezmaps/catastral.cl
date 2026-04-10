#!/usr/bin/env python3
"""Load lat/lon coordinates from S3 clean CSVs into catastro_actual."""

import csv
import io
import sys
import time
import boto3
import psycopg
from config import (
    S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET,
    DB_DSN,
)

S3_PREFIX = "2025ss_bcn/sii_data/"
BATCH_SIZE = 5000


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )


def list_csv_files(s3):
    """List all comuna CSV files in the clean data prefix."""
    paginator = s3.get_paginator("list_objects_v2")
    files = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".csv") and "comuna=" in obj["Key"]:
                files.append(obj["Key"])
    return sorted(files)


def parse_csv_coords(s3, key):
    """Download a CSV and extract (comuna, manzana, predio, lat, lon) tuples."""
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    text = obj["Body"].read().decode("utf-8")
    reader = csv.DictReader(io.StringIO(text))

    rows = []
    for row in reader:
        try:
            lat = row.get("lat", "").strip()
            lon = row.get("lon", "").strip()
            if not lat or not lon:
                continue
            lat_f = float(lat)
            lon_f = float(lon)
            if lat_f == 0 or lon_f == 0:
                continue
            comuna = int(row["comuna"])
            manzana = int(row["manzana"])
            predio = int(row["predio"])
            rows.append((lat_f, lon_f, comuna, manzana, predio))
        except (ValueError, KeyError):
            continue
    return rows


def update_batch(conn, batch):
    """Update lat/lon for a batch of predios."""
    with conn.cursor() as cur:
        cur.executemany(
            "UPDATE catastro_actual SET lat = %s, lon = %s "
            "WHERE comuna = %s AND manzana = %s AND predio = %s",
            batch,
        )


def main():
    s3 = get_s3_client()
    files = list_csv_files(s3)
    print(f"Found {len(files)} CSV files in {S3_PREFIX}")

    total_updated = 0
    total_rows = 0
    t0 = time.time()

    with psycopg.connect(DB_DSN) as conn:
        for i, key in enumerate(files, 1):
            try:
                rows = parse_csv_coords(s3, key)
                total_rows += len(rows)

                for start in range(0, len(rows), BATCH_SIZE):
                    batch = rows[start : start + BATCH_SIZE]
                    update_batch(conn, batch)
                    total_updated += len(batch)

                conn.commit()

                elapsed = time.time() - t0
                rate = total_updated / elapsed if elapsed > 0 else 0
                print(
                    f"  [{i}/{len(files)}] {key.split('/')[-1]}: "
                    f"{len(rows)} coords | "
                    f"Total: {total_updated:,} | "
                    f"{rate:.0f} rows/s"
                )
            except Exception as e:
                print(f"  ERROR processing {key}: {e}", file=sys.stderr)
                conn.rollback()
                continue

    elapsed = time.time() - t0
    print(f"\nDone. {total_updated:,} coordinates loaded in {elapsed:.1f}s")
    print(f"Parsed {total_rows:,} rows from {len(files)} files")


if __name__ == "__main__":
    main()
