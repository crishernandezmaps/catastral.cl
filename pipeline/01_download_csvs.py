#!/usr/bin/env python3
"""Download catastro CSVs from S3 to local staging directory."""
import os
import sys
import boto3
from config import (
    S3_ENDPOINT, S3_REGION, S3_ACCESS_KEY, S3_SECRET_KEY,
    S3_BUCKET, S3_PREFIX, STAGING_DIR, PERIODS,
)

def main(periods=None):
    periods = periods or PERIODS
    os.makedirs(STAGING_DIR, exist_ok=True)

    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        region_name=S3_REGION,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )

    for period in periods:
        key = f"{S3_PREFIX}/catastro_{period}.csv"
        local = os.path.join(STAGING_DIR, f"catastro_{period}.csv")

        if os.path.exists(local):
            local_size = os.path.getsize(local)
            remote = s3.head_object(Bucket=S3_BUCKET, Key=key)
            if local_size == remote["ContentLength"]:
                print(f"  SKIP {period} (already downloaded, {local_size/1e9:.1f} GB)")
                continue

        print(f"  Downloading {period}...", end=" ", flush=True)
        s3.download_file(S3_BUCKET, key, local)
        size = os.path.getsize(local) / 1e9
        print(f"{size:.2f} GB")

    print("Download complete.")

if __name__ == "__main__":
    target = sys.argv[1:] if len(sys.argv) > 1 else None
    main(target)
