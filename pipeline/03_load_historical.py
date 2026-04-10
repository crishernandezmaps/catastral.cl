#!/usr/bin/env python3
"""Load slim historical data from all 16 CSVs into catastro_historico."""
import os
import csv
import time
import psycopg
from config import DB_DSN, STAGING_DIR, PERIODS, HIST_COLUMNS, CSV_COLUMNS

# Map CSV column index for each historical column we need
HIST_CSV_INDICES = {col: CSV_COLUMNS.index(col) for col in HIST_COLUMNS}

def main():
    print("Loading historical data into catastro_historico...")
    t0 = time.time()

    with psycopg.connect(DB_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE catastro_historico")
        conn.commit()

        grand_total = 0
        cols = ", ".join(HIST_COLUMNS)

        for period in PERIODS:
            csv_path = os.path.join(STAGING_DIR, f"catastro_{period}.csv")
            if not os.path.exists(csv_path):
                print(f"  SKIP {period} (file not found)")
                continue

            pt0 = time.time()
            count = 0

            with open(csv_path, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                next(reader)  # skip header

                with conn.cursor() as cur:
                    with cur.copy(f"COPY catastro_historico ({cols}) FROM STDIN") as copy:
                        for row in reader:
                            slim = []
                            for col in HIST_COLUMNS:
                                idx = HIST_CSV_INDICES[col]
                                v = row[idx] if idx < len(row) else ""
                                slim.append(None if v == "" else v)
                            copy.write_row(slim)
                            count += 1

            conn.commit()
            elapsed = time.time() - pt0
            grand_total += count
            print(f"  {period}: {count:,} rows in {elapsed:.0f}s")

    total_elapsed = time.time() - t0
    print(f"Total: {grand_total:,} rows in {total_elapsed:.0f}s")

if __name__ == "__main__":
    main()
