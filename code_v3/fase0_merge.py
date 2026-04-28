#!/usr/bin/env python3
"""
fase0_merge.py — Merge JSONs individuales → CSV final por comuna.

1. Carga JSONs de getPredioNacional
2. Enriquece con catastro semestral (dc_direccion, dc_bc1_*, dc_padre_*, etc.)
3. Herencia por dirección usando dc_direccion (más completa que direccion_sii)
4. Stats + guardar CSV

Usage:
    python3 fase0_merge.py --comuna 15105 --datadir /tmp/fase0/15105/data
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path

import pandas as pd

from fase0_config import CATASTRO_CSV


def normalize_address(s):
    """Normaliza dirección quitando sufijos de departamento/bloque."""
    if pd.isna(s) or not s:
        return None
    s = str(s).upper().strip()
    s = re.sub(r"\s+(A|B|C|D|E)?DP\s+\d+\w*.*$", "", s)
    s = re.sub(r"\s+BX\s+\d+.*$", "", s)
    s = re.sub(r"\s+BD\s+\d+.*$", "", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--comuna", type=int, required=True)
    parser.add_argument("--datadir", required=True)
    parser.add_argument("--output", default=None)
    parser.add_argument("--catastro", default=CATASTRO_CSV)
    args = parser.parse_args()

    cod = args.comuna
    data_dir = Path(args.datadir)
    out_path = args.output or str(data_dir.parent / f"comuna={cod}.csv")

    # ── 1. Cargar JSONs ──────────────────────────────────────────────────────
    print("[1] Cargando JSONs desde %s..." % data_dir, flush=True)
    rows = []
    for f in sorted(data_dir.glob("*.json")):
        try:
            with open(f) as fh:
                rows.append(json.load(fh))
        except (json.JSONDecodeError, FileNotFoundError):
            pass

    print("    %s predios cargados" % f"{len(rows):,}", flush=True)
    if not rows:
        print("    Sin datos. Abortando.", flush=True)
        return

    df = pd.DataFrame(rows)
    df["manzana"] = df["manzana"].astype(str).str.strip().str.zfill(5)
    df["predio"] = df["predio"].astype(str).str.strip().str.zfill(5)
    df = df.sort_values(["manzana", "predio"]).reset_index(drop=True)

    # ── 2. Enriquecer con catastro semestral ─────────────────────────────────
    catastro_cols = [
        "dc_direccion", "dc_contribucion_semestral", "dc_cod_destino",
        "dc_avaluo_fiscal", "dc_avaluo_exento", "dc_sup_terreno",
        "dc_cod_ubicacion",
        "dc_bc1_comuna", "dc_bc1_manzana", "dc_bc1_predio",
        "dc_bc2_comuna", "dc_bc2_manzana", "dc_bc2_predio",
        "dc_padre_comuna", "dc_padre_manzana", "dc_padre_predio",
        "n_lineas_construccion", "sup_construida_total",
        "anio_construccion_min", "anio_construccion_max",
        "materiales", "calidades", "pisos_max", "serie",
    ]

    if os.path.exists(args.catastro):
        print("[2] Enriqueciendo con catastro semestral...", flush=True)
        # Read catastro for this comuna only
        cat_rows = []
        for chunk in pd.read_csv(args.catastro, dtype=str, chunksize=100_000,
                                 usecols=["comuna", "manzana", "predio"] + catastro_cols):
            mask = chunk["comuna"].astype(int) == cod
            if mask.any():
                cat_rows.append(chunk[mask])
        if cat_rows:
            cat = pd.concat(cat_rows, ignore_index=True)
            cat["_key"] = (
                cat["manzana"].astype(int).astype(str).str.zfill(5) + "_" +
                cat["predio"].astype(int).astype(str).str.zfill(5)
            )
            cat = cat.drop_duplicates("_key").set_index("_key")
            cat = cat[catastro_cols]

            df["_key"] = df["manzana"] + "_" + df["predio"]
            before_cols = len(df.columns)
            df = df.join(cat, on="_key", rsuffix="_cat")
            df.drop(columns=["_key"], inplace=True, errors="ignore")

            # Count enriched
            enriched = df["dc_direccion"].notna().sum()
            print("    Catastro join: %s predios con dc_direccion" % f"{enriched:,}", flush=True)
        else:
            print("    Sin datos catastro para comuna %d" % cod, flush=True)
    else:
        print("[2] Catastro CSV no encontrado (%s), skip" % args.catastro, flush=True)

    # ── 3. Herencia por dirección (usando dc_direccion del catastro) ─────────
    print("[3] Herencia por direccion...", flush=True)

    # Usar dc_direccion (catastro, más completa) con fallback a direccion_sii
    addr_col = "dc_direccion" if "dc_direccion" in df.columns else "direccion_sii"
    df["_addr_base"] = df[addr_col].apply(normalize_address)

    has_coords = df["lat"].notna() & (df["lat"] != "None")
    con = df[has_coords & df["_addr_base"].notna()]
    sin = df[~has_coords & df["_addr_base"].notna()]

    inherited = 0
    if len(sin) > 0 and len(con) > 0:
        addr_idx = {}
        for idx, r in con.iterrows():
            key = (r["manzana"], r["_addr_base"])
            if key not in addr_idx:
                addr_idx[key] = {
                    "lat": r["lat"], "lon": r["lon"],
                    "predioPublicado_predio": r.get("predioPublicado_predio"),
                    "predioPublicado_utm_x": r.get("predioPublicado_utm_x"),
                    "predioPublicado_utm_y": r.get("predioPublicado_utm_y"),
                }

        for idx, r in sin.iterrows():
            key = (r["manzana"], r["_addr_base"])
            donor = addr_idx.get(key)
            if donor:
                for col, val in donor.items():
                    if val is not None and (col not in df.columns or pd.isna(df.at[idx, col])):
                        df.at[idx, col] = val
                inherited += 1

    print("    Heredados: %s (de %s sin coords, usando %s)" % (
        f"{inherited:,}", f"{len(sin):,}", addr_col), flush=True)

    df.drop(columns=["_addr_base"], inplace=True, errors="ignore")

    # ── 4. Stats ─────────────────────────────────────────────────────────────
    print("[4] Stats:", flush=True)
    total = len(df)
    with_coords = df["lat"].notna().sum() - (df["lat"] == "None").sum()
    with_pp = df["predioPublicado_predio"].notna().sum()
    ok = (df["_ok"] == True).sum()
    print("    Total:           %s" % f"{total:,}", flush=True)
    print("    _ok=True:        %s (%s%%)" % (f"{ok:,}", f"{ok/total*100:.1f}"), flush=True)
    print("    Con lat/lon:     %s (%s%%)" % (f"{with_coords:,}", f"{with_coords/total*100:.1f}"), flush=True)
    print("    Con predioPubl:  %s (%s%%)" % (f"{with_pp:,}", f"{with_pp/total*100:.1f}"), flush=True)

    # ── 5. Guardar CSV (pipe-separated) ─────────────────────────────────────
    print("[5] Guardando %s..." % out_path, flush=True)
    df.to_csv(out_path, index=False, sep="|")
    size_mb = os.path.getsize(out_path) / 1024 / 1024
    print("    %s MB" % f"{size_mb:.0f}", flush=True)
    print("Done.", flush=True)


if __name__ == "__main__":
    main()
