#!/usr/bin/env python3
"""
5_generate_catalog.py — Genera JSON catálogo para Fase 4v2

Por cada comuna en S3 fase4v2, genera un JSON con:
  - Código y nombre de comuna
  - Rutas y pesos de archivos (CSV crudo, CSV procesado, GPKG)
  - Conteos: total filas CSV, polígonos GPKG, predios con datos, huérfanos, catastro-only
  - Predios con lat/lon

Output:
  - s3://siipredios/2025ss_bcn/fase4v2/catalog.json  (JSON con todas las comunas)
"""

import json
import os
import io
import geopandas as gpd
import pandas as pd
import boto3
from time import time
from datetime import datetime, timezone

# ── S3 config ──────────────────────────────────────────────────────────────
S3_ENDPOINT = "https://nbg1.your-objectstorage.com"
S3_BUCKET   = "siipredios"
AWS_ACCESS  = "YOUR_ACCESS_KEY"
AWS_SECRET  = "YOUR_SECRET_KEY"
S3_PREFIX   = "2025ss_bcn/fase4v2"

# ── Nombres de comunas (from catastro or F3v2) ────────────────────────────
def get_s3():
    return boto3.client("s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_ACCESS,
        aws_secret_access_key=AWS_SECRET)


def s3_size(s3, key):
    try:
        resp = s3.head_object(Bucket=S3_BUCKET, Key=key)
        return resp["ContentLength"]
    except Exception:
        return 0


def main():
    s3 = get_s3()
    tmp_dir = "/tmp/fase4v2_catalog"
    os.makedirs(tmp_dir, exist_ok=True)

    # List all comunas
    codigos = []
    kwargs = dict(Bucket=S3_BUCKET, Prefix=f"{S3_PREFIX}/", MaxKeys=1000)
    while True:
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            k = obj["Key"]
            if k.endswith(".gpkg"):
                cod = k.split("comuna=")[1].replace(".gpkg", "")
                codigos.append(cod)
        if not resp.get("IsTruncated"):
            break
        kwargs["ContinuationToken"] = resp["NextContinuationToken"]
    codigos.sort()
    print(f"Generating catalog for {len(codigos)} comunas...")

    catalog = {
        "fuente": "SII Segundo Semestre 2025 + Catastro Semestral",
        "pipeline": "Fase 4 v2: F3v2 enriquecido con catastro_2025_2.csv",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total_comunas": len(codigos),
        "comunas": []
    }

    totals = {
        "csv_rows": 0, "gpkg_rows": 0, "con_datos_y_poligono": 0,
        "solo_poligono": 0, "catastro_only": 0, "con_latlon": 0
    }

    for i, cod in enumerate(codigos):
        t0 = time()

        # File sizes
        gpkg_key = f"{S3_PREFIX}/comuna={cod}.gpkg"
        csv_key  = f"{S3_PREFIX}/comuna={cod}.csv"
        raw_key  = f"{S3_PREFIX}/comuna={cod}_raw.csv"

        gpkg_size = s3_size(s3, gpkg_key)
        csv_size  = s3_size(s3, csv_key)
        raw_size  = s3_size(s3, raw_key)

        # Read CSV to get counts
        try:
            resp = s3.get_object(Bucket=S3_BUCKET, Key=csv_key)
        except Exception as e:
            print(f"[{i+1}/{len(codigos)}] {cod} — CSV not found, skipping")
            continue
        df = pd.read_csv(io.BytesIO(resp["Body"].read()), dtype=str, low_memory=False)

        has_rol  = df["manzana"].notna() & (df["manzana"] != "") & (df["manzana"] != "nan")
        has_lat  = df["lat"].notna() & (df["lat"] != "") & (df["lat"] != "nan")
        has_poly = df["pol_area_m2"].notna() & (df["pol_area_m2"] != "") & (df["pol_area_m2"] != "nan")
        has_val  = df["valorTotal"].notna() & (df["valorTotal"] != "") & (df["valorTotal"] != "nan")

        con_datos_y_poly = int((has_rol & has_poly).sum())
        solo_poly        = int((has_poly & ~has_rol).sum())
        catastro_only    = int((has_rol & ~has_poly).sum())
        con_latlon       = int(has_lat.sum())
        total_poly       = int(has_poly.sum())

        # Get nombre from CSV
        nombre_col = df["nombreComuna"].dropna()
        nombre_col = nombre_col[nombre_col != ""]
        nombre_col = nombre_col[nombre_col != "nan"]
        nombre = nombre_col.iloc[0] if len(nombre_col) > 0 else cod

        entry = {
            "codigo": cod,
            "nombre": nombre,
            "predios": {
                "total_csv": len(df),
                "total_poligonos": total_poly,
                "con_datos_y_poligono": con_datos_y_poly,
                "solo_poligono": solo_poly,
                "catastro_only": catastro_only,
                "con_latlon": con_latlon,
            },
            "archivos": {
                "gpkg": {
                    "key": gpkg_key,
                    "tamano_mb": round(gpkg_size / 1048576, 1)
                },
                "csv": {
                    "key": csv_key,
                    "tamano_mb": round(csv_size / 1048576, 1)
                },
                "csv_raw": {
                    "key": raw_key,
                    "tamano_mb": round(raw_size / 1048576, 1)
                }
            }
        }

        catalog["comunas"].append(entry)

        totals["csv_rows"]             += len(df)
        totals["gpkg_rows"]            += total_poly
        totals["con_datos_y_poligono"] += con_datos_y_poly
        totals["solo_poligono"]        += solo_poly
        totals["catastro_only"]        += catastro_only
        totals["con_latlon"]           += con_latlon

        elapsed = time() - t0
        print(f"[{i+1}/{len(codigos)}] {cod} {nombre} — CSV:{len(df):,} GPKG:{total_poly:,} [{elapsed:.1f}s]")

    catalog["totales"] = totals

    # Upload catalog
    catalog_json = json.dumps(catalog, indent=2, ensure_ascii=False)
    local_path = os.path.join(tmp_dir, "catalog.json")
    with open(local_path, "w") as f:
        f.write(catalog_json)
    s3.upload_file(local_path, S3_BUCKET, f"{S3_PREFIX}/catalog.json")

    print()
    print("=" * 60)
    print(f"Catalog uploaded: s3://{S3_BUCKET}/{S3_PREFIX}/catalog.json")
    print(f"Total CSV rows:             {totals['csv_rows']:>12,}")
    print(f"Total polígonos (GPKG):     {totals['gpkg_rows']:>12,}")
    print(f"Con datos + polígono:       {totals['con_datos_y_poligono']:>12,}")
    print(f"Solo polígono (huérfanos):  {totals['solo_poligono']:>12,}")
    print(f"Catastro-only (sin poly):   {totals['catastro_only']:>12,}")
    print(f"Con lat/lon:                {totals['con_latlon']:>12,}")
    print("=" * 60)


if __name__ == "__main__":
    main()
