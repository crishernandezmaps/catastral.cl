#!/usr/bin/env python3
"""Load the latest period CSV into catastro_actual using COPY protocol."""
import os
import io
import csv
import time
import psycopg
from config import DB_DSN, STAGING_DIR, LATEST_PERIOD, CSV_COLUMNS

CHUNK_SIZE = 500_000
DB_COLUMNS = CSV_COLUMNS  # same order

def main():
    csv_path = os.path.join(STAGING_DIR, f"catastro_{LATEST_PERIOD}.csv")
    if not os.path.exists(csv_path):
        print(f"ERROR: {csv_path} not found. Run 01_download_csvs.py first.")
        return

    print(f"Loading {LATEST_PERIOD} into catastro_actual...")
    t0 = time.time()

    with psycopg.connect(DB_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE catastro_actual RESTART IDENTITY")
            # Drop indexes for faster bulk load
            cur.execute("DROP INDEX IF EXISTS idx_actual_rol")
            cur.execute("DROP INDEX IF EXISTS idx_actual_comuna")
            cur.execute("DROP INDEX IF EXISTS idx_actual_destino")
            cur.execute("DROP INDEX IF EXISTS idx_actual_sup")
            cur.execute("DROP INDEX IF EXISTS idx_actual_avaluo")
            cur.execute("DROP INDEX IF EXISTS idx_actual_direccion")
        conn.commit()

        total = 0
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)  # skip header

            cols = ", ".join(DB_COLUMNS)
            placeholders = ", ".join(["%s"] * len(DB_COLUMNS))

            with conn.cursor() as cur:
                with cur.copy(f"COPY catastro_actual ({cols}) FROM STDIN") as copy:
                    for row in reader:
                        # Convert empty strings to None
                        cleaned = [None if v == "" else v for v in row]
                        copy.write_row(cleaned)
                        total += 1
                        if total % 500_000 == 0:
                            print(f"  {total:,} rows...", flush=True)

        conn.commit()

    elapsed = time.time() - t0
    print(f"Loaded {total:,} rows in {elapsed:.0f}s ({total/elapsed:,.0f} rows/s)")

if __name__ == "__main__":
    main()
