#!/usr/bin/env python3
"""
batch_join_v2.py — Batch: corre 3_join_mejorado.py para las 347 comunas.

Descarga F0 CSV + F2 GPKG de S3, corre el join, sube resultado a S3.
Skip si ya existe en S3.

Uso:
    python3 batch_join_v2.py [--with-fi]
    # --with-fi: activa getFeatureInfo para manzanas huérfanas (requiere tunnels)
"""

import argparse
import os
import subprocess
import sys
import time

import boto3

S3_ENDPOINT = "https://nbg1.your-objectstorage.com"
S3_BUCKET = "siipredios"
AWS_ACCESS = "YOUR_ACCESS_KEY"
AWS_SECRET = "YOUR_SECRET_KEY"
S3_F0 = "2025ss_bcn/sii_data"
S3_F2 = "2025ss_bcn/vectors"
S3_OUT = "2025ss_bcn/fase3v2"

BASEDIR = os.path.dirname(os.path.abspath(__file__))
VENV = "/root/carto_predios/venv/bin/python3"
JOIN_SCRIPT = os.path.join(BASEDIR, "3_join_mejorado.py")
WORKDIR = "/tmp/fase3v2"

# Nombre WMS por comuna (para --with-fi)
# Solo necesario si se activa FI
COMUNAS_PY = os.path.join(BASEDIR, "comunas.py")


def get_s3():
    return boto3.client("s3", endpoint_url=S3_ENDPOINT,
                        aws_access_key_id=AWS_ACCESS,
                        aws_secret_access_key=AWS_SECRET)


def s3_exists(s3, key):
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except Exception:
        return False


def s3_download(s3, key, local):
    try:
        s3.download_file(S3_BUCKET, key, local)
        return True
    except Exception:
        return False


def s3_upload(s3, local, key):
    try:
        s3.upload_file(local, S3_BUCKET, key)
        return True
    except Exception as e:
        print(f"  [ERROR] Upload {key}: {e}")
        return False


def list_comunas_in_s3(s3, prefix):
    """Lista códigos de comuna disponibles en un prefix de S3."""
    codes = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix + "/"):
        for obj in page.get("Contents", []):
            fname = obj["Key"].split("/")[-1]
            if fname.startswith("comuna=") and fname.endswith(".csv"):
                cod = fname.replace("comuna=", "").replace(".csv", "")
                codes.add(cod)
            elif fname.endswith(".gpkg") and "comuna=" in fname:
                cod = fname.split("comuna=")[-1].replace(".gpkg", "")
                codes.add(cod)
    return sorted(codes)


def get_wms_name(cod):
    """Intenta obtener nombre WMS desde comunas.py."""
    try:
        spec = {}
        exec(open(COMUNAS_PY).read(), spec)
        normalizar = spec.get("normalizar")
        excepciones = spec.get("EXCEPCIONES_WMS", {})
        if cod in excepciones:
            return excepciones[cod]
        # Fallback: buscar en la lista de comunas
        comunas = spec.get("COMUNAS", {})
        nombre = comunas.get(cod, comunas.get(int(cod), ""))
        if nombre and normalizar:
            return normalizar(nombre)
    except Exception:
        pass
    return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--with-fi", action="store_true",
                        help="Activar getFeatureInfo para manzanas huérfanas")
    args = parser.parse_args()

    s3 = get_s3()

    # Listar comunas con F0 CSV disponible
    print("Listando comunas con F0 CSV en S3...")
    f0_codes = list_comunas_in_s3(s3, S3_F0)
    print(f"  F0 CSVs: {len(f0_codes)}")

    # Listar comunas con F2 GPKG disponible
    print("Listando comunas con F2 GPKG en S3...")
    f2_codes = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_F2 + "/"):
        for obj in page.get("Contents", []):
            fname = obj["Key"].split("/")[-1]
            if fname.startswith("comuna=") and fname.endswith(".gpkg"):
                cod = fname.replace("comuna=", "").replace(".gpkg", "")
                f2_codes.add(cod)
    print(f"  F2 GPKGs: {len(f2_codes)}")

    # Intersección: comunas con ambos inputs
    both = sorted(set(f0_codes) & f2_codes)
    print(f"  Comunas procesables: {len(both)}")

    # Ya completadas
    done_codes = set()
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_OUT + "/"):
        for obj in page.get("Contents", []):
            fname = obj["Key"].split("/")[-1]
            if fname.startswith("comuna=") and fname.endswith(".csv"):
                cod = fname.replace("comuna=", "").replace(".csv", "")
                done_codes.add(cod)
    print(f"  Ya completadas: {len(done_codes)}")

    pending = [c for c in both if c not in done_codes]
    print(f"  Pendientes: {len(pending)}")
    print()

    os.makedirs(WORKDIR, exist_ok=True)
    t_start = time.time()
    n_ok = 0
    n_fail = 0

    for i, cod in enumerate(pending):
        t0 = time.time()
        print(f"[{i+1}/{len(pending)}] Comuna {cod}...")

        # Paths locales
        f0_local = os.path.join(WORKDIR, f"f0_{cod}.csv")
        f2_local = os.path.join(WORKDIR, f"f2_{cod}.gpkg")
        out_dir = os.path.join(WORKDIR, f"out_{cod}")
        os.makedirs(out_dir, exist_ok=True)

        # Descargar inputs
        if not os.path.exists(f0_local):
            if not s3_download(s3, f"{S3_F0}/comuna={cod}.csv", f0_local):
                print(f"  [SKIP] No se pudo descargar F0")
                n_fail += 1
                continue

        if not os.path.exists(f2_local):
            if not s3_download(s3, f"{S3_F2}/comuna={cod}.gpkg", f2_local):
                print(f"  [SKIP] No se pudo descargar F2")
                n_fail += 1
                # Limpiar F0
                if os.path.exists(f0_local):
                    os.remove(f0_local)
                continue

        # Construir comando
        cmd = [VENV, JOIN_SCRIPT,
               "--csv", f0_local,
               "--gpkg", f2_local,
               "--output", out_dir,
               "--cod", cod]

        if args.with_fi:
            nombre = get_wms_name(cod)
            if nombre:
                cmd += ["--nombre", nombre]

        # Ejecutar
        result = subprocess.run(cmd, capture_output=True, text=True,
                                timeout=3600)

        if result.returncode != 0:
            print(f"  [FAIL] {result.stderr[-300:]}")
            n_fail += 1
        else:
            # Extraer stats del output
            for line in result.stdout.split("\n"):
                if "TOTAL:" in line or "URBANA:" in line or "Hoyos" in line:
                    print(f"  {line.strip()}")

            # Upload
            for ext in [".csv", ".gpkg"]:
                local = os.path.join(out_dir, f"comuna={cod}{ext}")
                if os.path.exists(local):
                    s3_upload(s3, local, f"{S3_OUT}/comuna={cod}{ext}")

            n_ok += 1
            elapsed = time.time() - t0
            print(f"  [OK] {elapsed:.1f}s")

        # Cleanup
        for f in [f0_local, f2_local]:
            if os.path.exists(f):
                os.remove(f)
        import shutil
        if os.path.exists(out_dir):
            shutil.rmtree(out_dir, ignore_errors=True)

    elapsed_total = time.time() - t_start
    print()
    print(f"=== BATCH COMPLETADO ===")
    print(f"OK: {n_ok}, FAIL: {n_fail}, Total: {elapsed_total/60:.1f} min")


if __name__ == "__main__":
    main()
