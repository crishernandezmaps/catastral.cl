#!/usr/bin/env python3
"""Fix shifted columns in S3 CSVs: some rows have lat in direccion_sii, lon in lat field."""

import csv
import io
import sys
import time
import boto3
from config import S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET

S3_PREFIX = "2025ss_bcn/sii_data/"

# The 30 comunas with shifted data
COMUNAS_TO_FIX = [
    "10111", "10112", "10203", "10401", "10403", "10404", "10502",
    "11303", "11401", "12205", "12302", "14202", "14602", "14605",
    "16403", "7202", "7301", "7304", "7402", "8104", "8107",
    "8203", "8210", "8211", "8306", "8401", "8411", "8412",
    "9204", "9208",
]


def main():
    s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT, aws_access_key_id=S3_ACCESS_KEY, aws_secret_access_key=S3_SECRET_KEY)

    for ci, comuna in enumerate(COMUNAS_TO_FIX, 1):
        key = f"{S3_PREFIX}comuna={comuna}.csv"
        print(f"[{ci}/{len(COMUNAS_TO_FIX)}] Processing {key}...")

        try:
            obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
            text = obj["Body"].read().decode("utf-8", errors="replace")
        except Exception as e:
            print(f"  ERROR downloading: {e}")
            continue

        reader = csv.DictReader(io.StringIO(text))
        fieldnames = reader.fieldnames
        rows = []
        fixed = 0

        for row in reader:
            lat_raw = row.get("lat", "").strip()
            lon_raw = row.get("lon", "").strip()
            dir_raw = row.get("direccion_sii", "").strip()

            if lat_raw:
                try:
                    lat_f = float(lat_raw)
                    lon_f = float(lon_raw) if lon_raw else 0
                except (ValueError, TypeError):
                    rows.append(row)
                    continue

                # Check if shifted
                if not (-56 <= lat_f <= -17 and -76 <= lon_f <= -66):
                    try:
                        dir_f = float(dir_raw)
                        if -56 <= dir_f <= -17 and -76 <= lat_f <= -66:
                            # Fix: swap back to correct positions
                            row["lat"] = str(dir_f)
                            row["lon"] = str(lat_f)
                            # direccion_sii was overwritten with lat, restore from valorTotal context
                            # We can't recover the original direccion, but at least coords are fixed
                            row["direccion_sii"] = ""
                            fixed += 1
                    except (ValueError, TypeError):
                        pass

            rows.append(row)

        if fixed == 0:
            print(f"  No shifted rows found, skipping upload")
            continue

        # Write corrected CSV
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

        # Upload back to S3
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=output.getvalue().encode("utf-8"),
        )
        print(f"  Fixed {fixed:,} rows, uploaded back to S3")

    print("\nDone.")


if __name__ == "__main__":
    main()
