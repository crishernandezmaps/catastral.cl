#!/usr/bin/env python3
"""Fix shifted coordinates: some CSVs have lat in direccion_sii, lon in lat field."""

import csv
import io
import sys
import time
import boto3
import psycopg
from config import S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET, DB_DSN

S3_PREFIX = "2025ss_bcn/sii_data/"
BATCH_SIZE = 5000


def main():
    s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT, aws_access_key_id=S3_ACCESS_KEY, aws_secret_access_key=S3_SECRET_KEY)

    paginator = s3.get_paginator("list_objects_v2")
    files = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".csv") and "comuna=" in obj["Key"]:
                files.append(obj["Key"])
    files.sort()
    print(f"Scanning {len(files)} CSV files for shifted coordinates...")

    total_fixed = 0
    t0 = time.time()

    with psycopg.connect(DB_DSN) as conn:
        for fi, key in enumerate(files, 1):
            try:
                obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
                text = obj["Body"].read().decode("utf-8", errors="replace")
                reader = csv.DictReader(io.StringIO(text))

                batch = []
                for row in reader:
                    lat_raw = row.get("lat", "").strip()
                    if not lat_raw:
                        continue
                    try:
                        lat_f = float(lat_raw)
                        lon_f = float(row.get("lon", "0").strip())
                    except (ValueError, TypeError):
                        continue

                    # Already valid
                    if -56 <= lat_f <= -17 and -76 <= lon_f <= -66:
                        continue

                    # Check if shifted: lat field has lon, direccion has lat
                    try:
                        dir_f = float(row.get("direccion_sii", "").strip())
                    except (ValueError, TypeError):
                        continue

                    if -56 <= dir_f <= -17 and -76 <= lat_f <= -66:
                        real_lat = dir_f
                        real_lon = lat_f
                        comuna = int(row["comuna"])
                        manzana = int(row["manzana"])
                        predio = int(row["predio"])
                        batch.append((real_lat, real_lon, comuna, manzana, predio))

                if batch:
                    for start in range(0, len(batch), BATCH_SIZE):
                        chunk = batch[start:start + BATCH_SIZE]
                        with conn.cursor() as cur:
                            cur.executemany(
                                "UPDATE catastro_actual SET lat = %s, lon = %s "
                                "WHERE comuna = %s AND manzana = %s AND predio = %s AND lat IS NULL",
                                chunk,
                            )
                    conn.commit()
                    total_fixed += len(batch)
                    elapsed = time.time() - t0
                    print(f"  [{fi}/{len(files)}] {key.split('/')[-1]}: {len(batch)} fixed | Total: {total_fixed:,} | {elapsed:.0f}s")

            except Exception as e:
                print(f"  ERROR {key}: {e}", file=sys.stderr)
                conn.rollback()

    elapsed = time.time() - t0
    print(f"\nDone. {total_fixed:,} shifted coordinates fixed in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
