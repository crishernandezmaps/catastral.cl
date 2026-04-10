#!/usr/bin/env python3
"""
4_enrich_catastro.py — Fase 4 v2: Enriquecer Fase 3v2 con datos del catastro semestral

Agrega columnas de edificación/construcción del CSV semestral del SII
(catastro_2025_2.csv) al output de Fase 3v2.

Lógica:
  1. Predios con match en F3v2 → se agregan columnas nuevas del catastro
  2. Predios en catastro sin match en F3v2 → se agregan como filas sin geometría
  3. Polígonos huérfanos (sin rol) → se preservan intactos, columnas nuevas en null
  → SIEMPRE se preservan todas las geometrías

Input:
  - s3://siipredios/2025ss_bcn/fase3v2/comuna={cod}.gpkg
  - s3://siipredios/catastro_historico/output/catastro_2025_2.csv

Output (por comuna):
  - s3://siipredios/2025ss_bcn/fase4v2/comuna={cod}.gpkg      (enriquecido + geometría)
  - s3://siipredios/2025ss_bcn/fase4v2/comuna={cod}.csv       (enriquecido sin geometría)
  - s3://siipredios/2025ss_bcn/fase4v2/comuna={cod}_raw.csv   (slice crudo del catastro)

Join key: comuna + manzana + predio
  F3v2 usa zero-padded strings: manzana='00397', predio='00013', comuna='10102'
  Catastro usa enteros: manzana=308, predio=61, comuna=1101
  → Se normalizan ambos a entero para el join
"""

import argparse
import os
import sys
import geopandas as gpd
import pandas as pd
import boto3
from time import time

# ── S3 config ──────────────────────────────────────────────────────────────
S3_ENDPOINT = "https://nbg1.your-objectstorage.com"
S3_BUCKET   = "siipredios"
AWS_ACCESS  = "YOUR_ACCESS_KEY"
AWS_SECRET  = "YOUR_SECRET_KEY"

S3_F3V2_PREFIX   = "2025ss_bcn/fase3v2"
S3_CATASTRO_KEY  = "catastro_historico/output/catastro_2025_2.csv"
S3_OUTPUT_PREFIX = "2025ss_bcn/fase4v2"

# ── Columnas nuevas del catastro (no presentes en F3v2) ────────────────────
CATASTRO_NEW_COLS = [
    "dc_contribucion_semestral",
    "dc_cod_destino",
    "dc_avaluo_fiscal",
    "dc_avaluo_exento",
    "dc_sup_terreno",
    "dc_cod_ubicacion",
    "dc_direccion",
    "dc_bc1_comuna", "dc_bc1_manzana", "dc_bc1_predio",
    "dc_bc2_comuna", "dc_bc2_manzana", "dc_bc2_predio",
    "dc_padre_comuna", "dc_padre_manzana", "dc_padre_predio",
    "n_lineas_construccion", "sup_construida_total",
    "anio_construccion_min", "anio_construccion_max",
    "materiales", "calidades", "pisos_max",
    "serie",
]

# Todas las columnas del catastro (para predios catastro-only)
CATASTRO_ALL_COLS = [
    "periodo", "anio", "semestre",
    "rc_direccion", "rc_serie", "rc_ind_aseo", "rc_cuota_trimestral",
    "rc_avaluo_total", "rc_avaluo_exento", "rc_anio_term_exencion",
    "rc_cod_ubicacion", "rc_cod_destino",
] + CATASTRO_NEW_COLS


def get_s3():
    return boto3.client("s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_ACCESS,
        aws_secret_access_key=AWS_SECRET)


def s3_exists(s3, key):
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except Exception:
        return False


def s3_download(s3, key, local_path):
    s3.download_file(S3_BUCKET, key, local_path)


def s3_upload(s3, local_path, key):
    s3.upload_file(local_path, S3_BUCKET, key)


def list_f3v2_comunas(s3):
    """List all comuna codes that have a GPKG in fase3v2."""
    codigos = []
    kwargs = dict(Bucket=S3_BUCKET, Prefix=f"{S3_F3V2_PREFIX}/", MaxKeys=1000)
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
    return sorted(codigos)


def load_catastro(s3, tmp_dir):
    """Download and load full catastro CSV. Index by comuna for fast filtering."""
    local = os.path.join(tmp_dir, "catastro_2025_2.csv")
    if not os.path.exists(local):
        print("  Downloading catastro_2025_2.csv (1.6 GB)...")
        s3_download(s3, S3_CATASTRO_KEY, local)
    print("  Loading catastro CSV...")
    t0 = time()
    df = pd.read_csv(local, dtype=str, low_memory=False)
    print(f"  Loaded {len(df):,} rows, {len(df.columns)} cols in {time()-t0:.1f}s")
    # Normalize keys to int-string for matching
    df["_jk_comuna"]  = pd.to_numeric(df["comuna"],  errors="coerce").astype("Int64").astype(str)
    df["_jk_manzana"] = pd.to_numeric(df["manzana"], errors="coerce").astype("Int64").astype(str)
    df["_jk_predio"]  = pd.to_numeric(df["predio"],  errors="coerce").astype("Int64").astype(str)
    return df


def normalize_key(s):
    """'00397' → '397', '10102' → '10102', NaN → None"""
    try:
        return str(int(s))
    except (ValueError, TypeError):
        return None


def process_comuna(cod, gdf_f3, df_cat_comuna, s3, tmp_dir):
    """Enrich one comuna with catastro data. Returns (n_f3, n_out, n_catastro_only)."""

    # ── Normalize F3v2 join keys ───────────────────────────────────────────
    gdf_f3 = gdf_f3.copy()
    gdf_f3["_jk_mz"] = gdf_f3["manzana"].map(normalize_key)
    gdf_f3["_jk_pr"] = gdf_f3["predio"].map(normalize_key)
    has_key = gdf_f3["_jk_mz"].notna() & gdf_f3["_jk_pr"].notna()
    gdf_f3["_jk"] = None
    gdf_f3.loc[has_key, "_jk"] = gdf_f3.loc[has_key, "_jk_mz"] + "-" + gdf_f3.loc[has_key, "_jk_pr"]

    # ── Normalize catastro join keys ───────────────────────────────────────
    df_cat = df_cat_comuna.copy()
    df_cat["_jk"] = df_cat["_jk_manzana"] + "-" + df_cat["_jk_predio"]

    # ── Determine which catastro cols to add ───────────────────────────────
    existing_cols = set(gdf_f3.columns)
    new_cols = [c for c in CATASTRO_NEW_COLS if c in df_cat.columns and c not in existing_cols]

    # ── LEFT JOIN: F3v2 ← catastro new cols ────────────────────────────────
    cat_slim = df_cat[["_jk"] + new_cols].drop_duplicates(subset="_jk", keep="first")
    n_before = len(gdf_f3)
    gdf_merged = gdf_f3.merge(cat_slim, on="_jk", how="left")
    assert len(gdf_merged) == n_before, f"Merge inflated rows: {n_before} → {len(gdf_merged)}"

    # ── Find catastro-only predios (not in F3v2) ──────────────────────────
    f3_keys = set(gdf_f3.loc[has_key, "_jk"].dropna().unique())
    cat_keys = set(df_cat["_jk"].unique())
    missing_keys = cat_keys - f3_keys

    n_catastro_only = 0
    if missing_keys:
        df_new = df_cat[df_cat["_jk"].isin(missing_keys)].copy()
        n_catastro_only = len(df_new)

        # Map catastro columns to F3v2 column names where possible
        df_new["geometry"] = None
        df_new["_match_method"] = "catastro_only"
        # Map basic identifiers
        df_new["comuna"]  = cod
        # manzana/predio: restore zero-padded format
        df_new["manzana"] = df_new["manzana"].apply(lambda x: str(x).zfill(5) if pd.notna(x) else x)
        df_new["predio"]  = df_new["predio"].apply(lambda x: str(x).zfill(5) if pd.notna(x) else x)
        df_new["rol"]     = df_new["manzana"] + "-" + df_new["predio"]

        # Align columns: add missing cols as NaN
        for col in gdf_merged.columns:
            if col not in df_new.columns:
                df_new[col] = None
        df_new = df_new[gdf_merged.columns]

        gdf_merged = pd.concat(
            [gdf_merged, gpd.GeoDataFrame(df_new, geometry="geometry", crs=gdf_merged.crs)],
            ignore_index=True
        )

    # ── Clean up temp columns ──────────────────────────────────────────────
    drop_cols = [c for c in ("_jk", "_jk_mz", "_jk_pr") if c in gdf_merged.columns]
    gdf_merged.drop(columns=drop_cols, inplace=True)

    # ── Export ─────────────────────────────────────────────────────────────
    gpkg_path = os.path.join(tmp_dir, f"comuna={cod}.gpkg")
    csv_path  = os.path.join(tmp_dir, f"comuna={cod}.csv")
    raw_path  = os.path.join(tmp_dir, f"comuna={cod}_raw.csv")

    # GPKG (only rows with geometry)
    gdf_with_geom = gdf_merged[gdf_merged.geometry.notna()].copy()
    gdf_with_geom.to_file(gpkg_path, driver="GPKG")
    # CSV (without geometry)
    gdf_merged.drop(columns=["geometry"]).to_csv(csv_path, index=False)
    # Raw catastro slice
    drop_jk = [c for c in ("_jk", "_jk_comuna", "_jk_manzana", "_jk_predio") if c in df_cat.columns]
    df_cat.drop(columns=drop_jk).to_csv(raw_path, index=False)

    # Upload to S3
    for local, key in [
        (gpkg_path, f"{S3_OUTPUT_PREFIX}/comuna={cod}.gpkg"),
        (csv_path,  f"{S3_OUTPUT_PREFIX}/comuna={cod}.csv"),
        (raw_path,  f"{S3_OUTPUT_PREFIX}/comuna={cod}_raw.csv"),
    ]:
        s3_upload(s3, local, key)
        os.remove(local)

    return n_before, len(gdf_merged), n_catastro_only


def main():
    parser = argparse.ArgumentParser(description="Fase 4 v2: Enrich F3v2 with catastro semestral")
    parser.add_argument("--cod", help="Process single comuna code (e.g. 10102)")
    parser.add_argument("--skip-existing", action="store_true", help="Skip if output already in S3")
    args = parser.parse_args()

    s3 = get_s3()
    tmp_dir = "/tmp/fase4v2"
    os.makedirs(tmp_dir, exist_ok=True)

    # Load catastro (once, ~1.6 GB)
    print("=" * 60)
    print("FASE 4 v2: Enriquecer con catastro semestral")
    print("=" * 60)
    df_cat = load_catastro(s3, tmp_dir)

    # Index catastro by normalized comuna code for fast lookup
    cat_by_comuna = {}
    for cod_cat, grp in df_cat.groupby("_jk_comuna"):
        cat_by_comuna[cod_cat] = grp

    # Get comunas to process
    if args.cod:
        codigos = [args.cod]
    else:
        codigos = list_f3v2_comunas(s3)

    print(f"\n  {len(codigos)} comunas to process")
    print(f"  Catastro new cols: {len(CATASTRO_NEW_COLS)}")
    print()

    total_f3 = 0
    total_out = 0
    total_new = 0
    skipped = 0

    for i, cod in enumerate(codigos):
        tag = f"[{i+1}/{len(codigos)}]"

        # Skip existing?
        if args.skip_existing:
            out_key = f"{S3_OUTPUT_PREFIX}/comuna={cod}.gpkg"
            if s3_exists(s3, out_key):
                print(f"{tag} {cod} — exists, skip")
                skipped += 1
                continue

        # Download F3v2 GPKG
        f3_key = f"{S3_F3V2_PREFIX}/comuna={cod}.gpkg"
        local_gpkg = os.path.join(tmp_dir, f"f3_{cod}.gpkg")
        try:
            s3_download(s3, f3_key, local_gpkg)
        except Exception as e:
            print(f"{tag} {cod} — F3v2 not found: {e}")
            continue

        t0 = time()
        gdf = gpd.read_file(local_gpkg)
        os.remove(local_gpkg)

        # Get catastro for this comuna (normalize cod: '10102' → '10102', '01101' → '1101')
        cod_norm = str(int(cod))
        df_cat_c = cat_by_comuna.get(cod_norm, pd.DataFrame())

        if df_cat_c.empty:
            print(f"{tag} {cod} — WARNING: no catastro data for comuna {cod_norm}")
            # Still process (just no enrichment) to maintain consistency
            # Add empty new cols
            for col in CATASTRO_NEW_COLS:
                if col not in gdf.columns:
                    gdf[col] = None
            gpkg_path = os.path.join(tmp_dir, f"comuna={cod}.gpkg")
            csv_path  = os.path.join(tmp_dir, f"comuna={cod}.csv")
            raw_path  = os.path.join(tmp_dir, f"comuna={cod}_raw.csv")
            gdf.to_file(gpkg_path, driver="GPKG")
            gdf.drop(columns=["geometry"]).to_csv(csv_path, index=False)
            pd.DataFrame().to_csv(raw_path, index=False)
            for local, key in [
                (gpkg_path, f"{S3_OUTPUT_PREFIX}/comuna={cod}.gpkg"),
                (csv_path,  f"{S3_OUTPUT_PREFIX}/comuna={cod}.csv"),
                (raw_path,  f"{S3_OUTPUT_PREFIX}/comuna={cod}_raw.csv"),
            ]:
                s3_upload(s3, local, key)
                os.remove(local)
            print(f"{tag} {cod} — F3:{len(gdf):,} → out:{len(gdf):,} (no catastro) [{time()-t0:.1f}s]")
            continue

        n_f3, n_out, n_new = process_comuna(cod, gdf, df_cat_c, s3, tmp_dir)
        elapsed = time() - t0

        total_f3  += n_f3
        total_out += n_out
        total_new += n_new

        print(f"{tag} {cod} — F3:{n_f3:,} → out:{n_out:,} (+{n_new} catastro-only) [{elapsed:.1f}s]")

    print()
    print("=" * 60)
    print(f"DONE. F3:{total_f3:,} → out:{total_out:,} (+{total_new:,} catastro-only) skipped:{skipped}")
    print(f"Output: s3://{S3_BUCKET}/{S3_OUTPUT_PREFIX}/")
    print("=" * 60)


if __name__ == "__main__":
    main()
