#!/usr/bin/env python3
"""Run the full pipeline: download, load latest, load historical, build indexes."""
import time
import sys

def main():
    skip_download = "--skip-download" in sys.argv
    only_latest = "--only-latest" in sys.argv

    t0 = time.time()

    if not skip_download:
        print("=" * 60)
        print("STEP 1: Download CSVs from S3")
        print("=" * 60)
        from importlib import import_module
        dl = import_module("01_download_csvs")
        dl.main()
    else:
        print("Skipping download (--skip-download)")

    print()
    print("=" * 60)
    print("STEP 2: Load latest period into catastro_actual")
    print("=" * 60)
    from importlib import import_module
    load = import_module("02_load_latest")
    load.main()

    if not only_latest:
        print()
        print("=" * 60)
        print("STEP 3: Load historical data into catastro_historico")
        print("=" * 60)
        hist = import_module("03_load_historical")
        hist.main()

    print()
    print("=" * 60)
    print("STEP 4: Build indexes")
    print("=" * 60)
    idx = import_module("04_build_indexes")
    idx.main()

    total = time.time() - t0
    print()
    print(f"Pipeline complete in {total/60:.1f} minutes")

if __name__ == "__main__":
    main()
