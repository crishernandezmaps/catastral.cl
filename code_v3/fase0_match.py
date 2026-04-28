#!/usr/bin/env python3
"""
fase0_match.py — Match Phase 0 coords → vectorized polygons → GPKG.

Methods (in order):
  1. Point-in-polygon
  2. Nearest ≤10m
  3. Nearest ≤50m
  4. Coord inheritance (apartments sharing exact lat/lon)
  5. Address inheritance (apartments sharing building address)

Generates GPKG with all predios (geometry when matched, null when not).
Appends match metrics to metrics.json.

Usage:
    python3 fase0_match.py --comuna 15105 --csv data.csv --vectors vectors.gpkg --output out.gpkg
"""

import argparse
import json
import os
import re
import time

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from shapely.strtree import STRtree


def normalize_address(s):
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
    parser.add_argument("--csv", required=True)
    parser.add_argument("--vectors", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--metrics", default=None)
    args = parser.parse_args()

    t0_total = time.time()

    # ── 1. Cargar ────────────────────────────────────────────────────────────
    print("  [match] Cargando...", flush=True)
    polys = gpd.read_file(args.vectors, engine="pyogrio")
    df = pd.read_csv(args.csv, dtype=str, low_memory=False, sep="|")
    print("    Poligonos: %d  |  Predios: %d" % (len(polys), len(df)), flush=True)

    if polys.crs and polys.crs.to_epsg() != 4326:
        polys = polys.to_crs("EPSG:4326")

    tree = STRtree(polys.geometry.values)
    total = len(df)

    # Prepare columns
    df["_lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["_lon"] = pd.to_numeric(df["lon"], errors="coerce")
    valid = df["_lat"].notna() & (df["_lat"] >= -62) & (df["_lat"] <= -17) & (df["_lon"] >= -80) & (df["_lon"] <= -64)
    with_coords = df[valid].index

    df["_poly_idx"] = -1
    df["_match_method"] = ""

    # ── 2. PIP ───────────────────────────────────────────────────────────────
    print("  [match] PIP...", flush=True)
    df["_match_dist_m"] = None
    t0 = time.time()
    pip = 0
    for idx in with_coords:
        pt = Point(df.at[idx, "_lon"], df.at[idx, "_lat"])
        for c in tree.query(pt):
            if polys.geometry.iloc[c].contains(pt):
                df.at[idx, "_poly_idx"] = c
                df.at[idx, "_match_method"] = "point_in_polygon"
                df.at[idx, "_match_dist_m"] = 0.0
                pip += 1
                break
    print("    PIP: %d (%.0fs)" % (pip, time.time() - t0), flush=True)

    # ── 3. Nearest ≤10m (drift típico SII ~0.2m, máx ~3m) ──────────────────
    print("  [match] Nearest ≤10m...", flush=True)
    t0 = time.time()
    n10 = 0
    unmatched = df[valid & (df["_poly_idx"] == -1)].index
    for idx in unmatched:
        pt = Point(df.at[idx, "_lon"], df.at[idx, "_lat"])
        ni = tree.nearest(pt)
        dist_m = pt.distance(polys.geometry.iloc[ni]) * 111000
        if dist_m <= 10:
            df.at[idx, "_poly_idx"] = ni
            df.at[idx, "_match_method"] = "nearest_10m"
            df.at[idx, "_match_dist_m"] = round(dist_m, 1)
            n10 += 1
    print("    ≤10m: %d (%.0fs)" % (n10, time.time() - t0), flush=True)

    # ── 4. Coord inheritance ─────────────────────────────────────────────────
    print("  [match] Coord inheritance...", flush=True)
    t0 = time.time()
    matched_df = df[df["_poly_idx"] >= 0]
    coord_map = {}
    for idx, row in matched_df.iterrows():
        key = (round(row["_lat"], 6), round(row["_lon"], 6))
        if key not in coord_map:
            coord_map[key] = int(row["_poly_idx"])

    ci = 0
    for idx in df[valid & (df["_poly_idx"] == -1)].index:
        key = (round(df.at[idx, "_lat"], 6), round(df.at[idx, "_lon"], 6))
        if key in coord_map:
            df.at[idx, "_poly_idx"] = coord_map[key]
            df.at[idx, "_match_method"] = "coord_inheritance"
            ci += 1
    print("    Coord inherit: %d (%.0fs)" % (ci, time.time() - t0), flush=True)

    # ── 5. Address inheritance (usa dc_direccion del catastro, más completa) ─
    print("  [match] Address inheritance...", flush=True)
    t0 = time.time()
    addr_col = "dc_direccion" if "dc_direccion" in df.columns else "direccion_sii"
    df["_addr"] = df[addr_col].apply(normalize_address)
    df["_mz"] = df["manzana"].astype(str).str.strip().str.zfill(5)

    addr_map = {}
    for idx, row in df[(df["_poly_idx"] >= 0) & df["_addr"].notna()].iterrows():
        key = (row["_mz"], row["_addr"])
        if key not in addr_map:
            addr_map[key] = int(row["_poly_idx"])

    ai = 0
    for idx, row in df[(df["_poly_idx"] == -1) & df["_addr"].notna()].iterrows():
        key = (row["_mz"], row["_addr"])
        if key in addr_map:
            df.at[idx, "_poly_idx"] = addr_map[key]
            df.at[idx, "_match_method"] = "address_inheritance"
            ai += 1
    print("    Addr inherit: %d (%.0fs)" % (ai, time.time() - t0), flush=True)

    # ── 6. Build GPKG ────────────────────────────────────────────────────────
    print("  [match] Construyendo GPKG...", flush=True)
    geometries = []
    areas = []
    for _, row in df.iterrows():
        pi = int(row["_poly_idx"])
        if pi >= 0:
            geometries.append(polys.geometry.iloc[pi])
            areas.append(polys.geometry.iloc[pi].area * 111000 * 111000)
        else:
            geometries.append(None)
            areas.append(None)

    df["pol_area_m2"] = areas
    drop_cols = ["_lat", "_lon", "_addr", "_mz"]
    df.drop(columns=drop_cols, inplace=True, errors="ignore")

    gdf = gpd.GeoDataFrame(df, geometry=geometries, crs="EPSG:4326")

    # ── 6b. Append unmatched polygons ────────────────────────────────────
    used_poly_idxs = set(df[df["_poly_idx"] >= 0]["_poly_idx"].astype(int).unique())
    all_poly_idxs = set(range(len(polys)))
    orphan_idxs = sorted(all_poly_idxs - used_poly_idxs)
    if orphan_idxs:
        orphan_geoms = [polys.geometry.iloc[i] for i in orphan_idxs]
        orphan_areas = [polys.geometry.iloc[i].area * 111000 * 111000 for i in orphan_idxs]
        orphan_df = pd.DataFrame({
            col: pd.NA for col in gdf.columns if col != "geometry"
        }, index=range(len(orphan_idxs)))
        orphan_df["_poly_idx"] = -2
        orphan_df["_match_method"] = "unmatched_polygon"
        orphan_df["pol_area_m2"] = orphan_areas
        orphan_gdf = gpd.GeoDataFrame(orphan_df, geometry=orphan_geoms, crs="EPSG:4326")
        gdf = pd.concat([gdf, orphan_gdf], ignore_index=True)
        gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs="EPSG:4326")
        print("    Polygons sin match (huerfanos): %d" % len(orphan_idxs), flush=True)

    print("  [match] Guardando GeoParquet...", flush=True)
    gdf.to_parquet(args.output)
    out_mb = os.path.getsize(args.output) / 1024 / 1024
    print("    GeoParquet: %.0f MB" % out_mb, flush=True)

    # ── 7. Metrics ───────────────────────────────────────────────────────────
    total_matched = (df["_poly_idx"] >= 0).sum()
    sin_coords = total - len(with_coords)

    n_orphan = len(orphan_idxs) if orphan_idxs else 0
    metrics = {
        "poligonos_disponibles": len(polys),
        "poligonos_huerfanos": n_orphan,
        "predios_con_coords": len(with_coords),
        "metodo_pip": pip,
        "metodo_nearest_10m": n10,
        "metodo_coord_inherit": ci,
        "metodo_addr_inherit": ai,
        "metodo_nearest_50m": 0,
        "total_matched": int(total_matched),
        "sin_match": int(total - total_matched),
        "sin_coords": int(sin_coords),
        "cobertura_pct": round(total_matched / total * 100, 2),
        "tiempo_s": round(time.time() - t0_total),
    }

    if args.metrics:
        existing = {}
        if os.path.exists(args.metrics):
            try:
                with open(args.metrics) as f:
                    existing = json.load(f)
            except Exception:
                pass
        existing["match"] = metrics
        with open(args.metrics, "w") as f:
            json.dump(existing, f, indent=2)

    print("", flush=True)
    sep = "=" * 60
    print("  " + sep, flush=True)
    print("  Total: %s | Con pol: %s (%.2f%%) | Sin pol: %s" % (
        f"{total:,}", f"{total_matched:,}", total_matched/total*100,
        f"{total-total_matched:,}"), flush=True)
    print("  PIP=%s  N10=%s  CI=%s  AI=%s" % (
        f"{pip:,}", f"{n10:,}", f"{ci:,}", f"{ai:,}"), flush=True)
    print("  " + sep, flush=True)


if __name__ == "__main__":
    main()
