#!/usr/bin/env python3
"""
batch_etapa2.py — Etapa 2: Procesamiento paralelo (sin túneles).

Procesa comunas ya descargadas en Etapa 1. NO usa internet.
Para cada comuna:
  1. Vectorización bloques 16384px con overlap
  2. Merge urbanos S3 + agrícolas
  3. Match PIP/nearest + huérfanos
  4. Recovery UTM (sin OCR — da 0 resultados y es muy lento)
  5. Optimize (simplify geometrías)
  6. Upload a S3 sii_extractor/

Corre N comunas en paralelo (default 3).

Usage:
    python3 batch_etapa2.py --workers 3
    python3 batch_etapa2.py --comunas 7208,7207 --workers 2
"""

import argparse
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import boto3
from botocore.config import Config

VENV = "/root/carto_predios/venv/bin/python3"
PIPELINE_DIR = "/root/carto_predios/sii_vectorizer/pipeline_clean"
WORKDIR_BASE = "/tmp/fase0_v2"
STATUS_FILE = "/tmp/batch_etapa2_status.json"

S3_ENDPOINT = "https://nbg1.your-objectstorage.com"
S3_BUCKET = "siipredios"
S3_ACCESS_KEY = "YOUR_ACCESS_KEY"
S3_SECRET_KEY = "YOUR_SECRET_KEY"

MAULE_COMUNAS = [
    (7401, "CAUQUENES", "Cauquenes"),
    (7403, "CHANCO", "Chanco"),
    (7303, "COLBUN", "Colbun"),
    (7208, "CONSTITUCION", "Constitucion"),
    (7207, "CUREPTO", "Curepto"),
    (7101, "CURICO", "Curico"),
    (7209, "EMPEDRADO", "Empedrado"),
    (7107, "HUALANE", "Hualane"),
    (7105, "LICANTEN", "Licanten"),
    (7301, "LINARES", "Linares"),
    (7304, "LONGAVI", "Longavi"),
    (7206, "MAULE", "Maule"),
    (7108, "MOLINA", "Molina"),
    (7305, "PARRAL", "Parral"),
    (7203, "PELARCO", "Pelarco"),
    (7402, "PELLUHUE", "Pelluhue"),
    (7205, "PENCAHUE", "Pencahue"),
    (7104, "RAUCO", "Rauco"),
    (7306, "RETIRO", "Retiro"),
    (7103, "ROMERAL", "Romeral"),
    (7204, "RIO_CLARO", "Rio_Claro"),
    (7109, "SAGRADA_FAMILIA", "Sagrada_Familia"),
    (7202, "SAN_CLEMENTE", "San_Clemente"),
    (7310, "SAN_JAVIER", "San_Javier"),
    (7210, "SAN_RAFAEL", "San_Rafael"),
    (7201, "TALCA", "Talca"),
    (7102, "TENO", "Teno"),
]


def update_status(data):
    with open(STATUS_FILE, "w") as f:
        json.dump(data, f)


def is_download_complete(cod):
    """Check if Etapa 1 data exists."""
    wdir = os.path.join(WORKDIR_BASE, str(cod))
    csv_path = os.path.join(wdir, f"comuna={cod}.csv")
    tiles_dir = os.path.join(wdir, "tiles_z19")
    has_csv = os.path.exists(csv_path)
    has_tiles = os.path.exists(tiles_dir) and len(
        [f for f in os.listdir(tiles_dir) if f.startswith("sc_")]
    ) > 100
    return has_csv and has_tiles


def is_in_s3(cod, wms):
    """Check if already uploaded to sii_extractor."""
    try:
        s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT,
            aws_access_key_id=S3_ACCESS_KEY, aws_secret_access_key=S3_SECRET_KEY,
            config=Config(signature_version="s3v4"))
        s3.head_object(Bucket=S3_BUCKET,
            Key=f"sii_extractor/{wms}_{cod}/comuna={cod}.parquet")
        return True
    except Exception:
        return False


def process_comuna(cod, wms, nombre):
    """Process a single comuna (vectorize + match + optimize + upload). Returns (cod, ok, msg, elapsed)."""
    t0 = time.time()
    wdir = os.path.join(WORKDIR_BASE, str(cod))
    log_file = f"/tmp/batch_e2_{cod}.log"
    log = open(log_file, "w")

    try:
        # 1. Vectorize + match via orchestrator
        # The orchestrator will skip download (data already exists) and go to vectorize
        result = subprocess.run(
            [VENV, "-u", os.path.join(PIPELINE_DIR, "fase0_orchestrator.py"),
             "--comuna", str(cod), "--tunnels", "70", "--skip-s3-check"],
            cwd=PIPELINE_DIR,
            stdout=log, stderr=subprocess.STDOUT,
            timeout=7200,  # 2h max
        )
        log.close()

        if result.returncode != 0:
            return (cod, False, "orchestrator failed", time.time() - t0)

        # 2. Optimize
        s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT,
            aws_access_key_id=S3_ACCESS_KEY, aws_secret_access_key=S3_SECRET_KEY,
            config=Config(signature_version="s3v4"))

        raw_parquet = f"/tmp/batch_e2_{cod}_raw.parquet"
        opt_parquet = f"/tmp/batch_e2_{cod}_opt.parquet"

        s3.download_file(S3_BUCKET, f"2025ss_bcn/F0/comuna={cod}.parquet", raw_parquet)

        result = subprocess.run(
            [VENV, os.path.join(PIPELINE_DIR, "optimize_parquet.py"),
             "--input", raw_parquet, "--output", opt_parquet],
            cwd=PIPELINE_DIR, capture_output=True, text=True, timeout=1800,
        )

        if result.returncode != 0 or not os.path.exists(opt_parquet):
            return (cod, False, "optimize failed", time.time() - t0)

        # 3. Upload to sii_extractor/
        s3_prefix = f"sii_extractor/{wms}_{cod}"
        s3.upload_file(opt_parquet, S3_BUCKET, f"{s3_prefix}/comuna={cod}.parquet")
        s3.copy_object(Bucket=S3_BUCKET,
            CopySource=f"{S3_BUCKET}/2025ss_bcn/F0/comuna={cod}.csv",
            Key=f"{s3_prefix}/comuna={cod}.csv")

        # 4. Cleanup temp files
        for f in [raw_parquet, opt_parquet]:
            if os.path.exists(f):
                os.remove(f)

        # 5. Cleanup workdir (tiles + data)
        import shutil
        shutil.rmtree(wdir, ignore_errors=True)

        elapsed = time.time() - t0
        return (cod, True, f"OK", elapsed)

    except Exception as e:
        return (cod, False, str(e)[:100], time.time() - t0)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", type=int, default=3,
                        help="Parallel workers (default 3)")
    parser.add_argument("--comunas", default=None,
                        help="Comma-separated SII codes (default: all Maule)")
    args = parser.parse_args()

    if args.comunas:
        codes = [int(c.strip()) for c in args.comunas.split(",")]
        comunas = [(cod, wms, name) for cod, wms, name in MAULE_COMUNAS if cod in codes]
    else:
        comunas = MAULE_COMUNAS

    # Filter: only those with complete downloads and not already in S3
    to_process = []
    for cod, wms, nombre in comunas:
        if is_in_s3(cod, wms):
            print(f"  {nombre} ({cod}) — already in S3, skip", flush=True)
            continue
        if not is_download_complete(cod):
            print(f"  {nombre} ({cod}) — download incomplete, skip (run Etapa 1 first)",
                  flush=True)
            continue
        to_process.append((cod, wms, nombre))

    total = len(to_process)
    if total == 0:
        print("Nothing to process.", flush=True)
        return

    t0 = time.time()
    done = 0
    failed = []

    print(f"\n{'='*60}", flush=True)
    print(f"ETAPA 2 — Procesamiento — {total} comunas — {args.workers} workers",
          flush=True)
    print(f"{'='*60}", flush=True)

    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        futures = {}
        for cod, wms, nombre in to_process:
            fut = pool.submit(process_comuna, cod, wms, nombre)
            futures[fut] = (cod, wms, nombre)

        for fut in as_completed(futures):
            cod, wms, nombre = futures[fut]
            try:
                cod_r, ok, msg, elapsed = fut.result()
            except Exception as e:
                ok, msg, elapsed = False, str(e)[:100], 0

            if ok:
                done += 1
                print(f"  DONE {nombre} ({cod}) in {elapsed/60:.1f} min — "
                      f"{done}/{total}", flush=True)
            else:
                failed.append((cod, nombre, msg))
                print(f"  FAIL {nombre} ({cod}): {msg}", flush=True)

            update_status({
                "done": done, "total": total, "failed": len(failed),
                "elapsed_min": (time.time() - t0) / 60,
                "last": nombre,
            })

    elapsed_total = (time.time() - t0) / 60
    print(f"\n{'='*60}", flush=True)
    print(f"ETAPA 2 COMPLETE — {done}/{total} OK, {len(failed)} failed — "
          f"{elapsed_total:.0f} min", flush=True)
    if failed:
        print(f"Failed: {failed}", flush=True)
    print(f"{'='*60}", flush=True)


if __name__ == "__main__":
    main()
