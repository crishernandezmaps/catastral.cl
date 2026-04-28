#!/usr/bin/env python3
"""
fase0_recovery.py — Recuperación de predios sin polígono post-PIP.

Corre DESPUÉS de fase0_match.py. Aplica métodos adicionales para
predios que no matchearon en PIP ni nearest ≤10m:

  1. AH UTM → PIP: predios con ah_utm_x/y (coords del Área Homogénea)
     convertidas de EPSG:32719 a WGS84, point-in-polygon
  2. AH UTM → nearest: si no cae dentro, nearest sin límite
  3. Nearest unlimited: predios con lat/lon que quedaron sin match
     (coords SII muy desplazadas) → nearest sin límite de distancia
  4. OCR huérfanos: polígonos sin predio asignado, extraer número
     via Tesseract, deducir manzana del vecino → match
  5. Address inheritance: predios sin coords que comparten
     (manzana, dirección base) con un predio CON polígono

Usage:
    python3 fase0_recovery.py --comuna 15105 \
        --gpkg /tmp/fase0_v2/15105/comuna=15105.gpkg \
        --vectors /tmp/fase3_test/15105/vectors_15105.gpkg \
        --tif /tmp/fase3_test/15105/tif_15105.tif \
        --csv /tmp/fase0_v2/15105/comuna=15105.csv
"""

import argparse
import json
import math
import os
import re
import time

import geopandas as gpd
import numpy as np
import pandas as pd
import pytesseract
import rasterio
from PIL import Image
from pyproj import Transformer
from rasterio.features import geometry_mask
from rasterio.windows import from_bounds
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


def ocr_polygon(src, geom):
    """Extract number from polygon via Tesseract OCR."""
    try:
        minx, miny, maxx, maxy = geom.bounds
        win = from_bounds(minx, miny, maxx, maxy, src.transform)
        win = win.intersection(rasterio.windows.Window(0, 0, src.width, src.height))
        if win.width < 5 or win.height < 5:
            return None
        data = src.read(window=win)
        r, a = data[0], data[3]
        win_transform = rasterio.windows.transform(win, src.transform)
        mask = geometry_mask([geom], out_shape=(int(win.height), int(win.width)),
                            transform=win_transform, invert=True)
        text_mask = (r < 120) & mask & (a > 0)
        if text_mask.sum() < 5:
            return None
        h, w = text_mask.shape
        ocr_img = np.ones((h, w), dtype=np.uint8) * 255
        ocr_img[text_mask] = 0
        pil = Image.fromarray(ocr_img, "L").resize((w * 3, h * 3), Image.NEAREST)
        padded = Image.new("L", (pil.width + 20, pil.height + 20), 255)
        padded.paste(pil, (10, 10))
        text = pytesseract.image_to_string(
            padded, config="--psm 6 -c tessedit_char_whitelist=0123456789").strip()
        for n in text.split():
            if n.isdigit() and 0 < int(n) < 90000:
                return int(n)
    except Exception:
        pass
    return None


def main():
    parser = argparse.ArgumentParser(description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--comuna", type=int, required=True)
    parser.add_argument("--gpkg", required=True, help="GPKG de fase0_match.py")
    parser.add_argument("--vectors", required=True, help="Vectores originales")
    parser.add_argument("--tif", required=True, help="TIF original")
    parser.add_argument("--csv", required=True, help="CSV de fase0_merge.py")
    parser.add_argument("--agri-vectors", default="", help="GPKG con polígonos agrícolas nuevos")
    parser.add_argument("--metrics", default=None)
    parser.add_argument("--min-area-ocr", type=int, default=200)
    args = parser.parse_args()

    t0_total = time.time()

    # ── Cargar datos ─────────────────────────────────────────────────────────
    print("[recovery] Cargando datos...", flush=True)
    gdf = gpd.read_parquet(args.gpkg)
    polys = gpd.read_file(args.vectors, engine="pyogrio")
    df = pd.read_csv(args.csv, dtype=str, low_memory=False, sep="|")

    if polys.crs and polys.crs.to_epsg() != 4326:
        polys_4326 = polys.to_crs("EPSG:4326")
    else:
        polys_4326 = polys

    # Merge agricultural polygons if provided
    if args.agri_vectors and os.path.exists(args.agri_vectors):
        agri = gpd.read_file(args.agri_vectors, engine="pyogrio")
        if agri.crs and agri.crs.to_epsg() != 4326:
            agri = agri.to_crs("EPSG:4326")
        polys_4326 = pd.concat([polys_4326, agri], ignore_index=True)
        print("    Polígonos: %d urbanos + %d agrícolas = %d total" % (
            len(polys) - len(agri) if len(polys) > len(agri) else len(polys),
            len(agri), len(polys_4326)), flush=True)

    tree = STRtree(polys_4326.geometry.values)
    tr = Transformer.from_crs("EPSG:32719", "EPSG:4326", always_xy=True)

    gdf["_key"] = (gdf["manzana"].astype(str).str.strip().str.zfill(5) + "_" +
                   gdf["predio"].astype(str).str.strip().str.zfill(5))
    has_geom = gdf.geometry.notna() & ~gdf.geometry.is_empty
    before = has_geom.sum()
    total = len(gdf)

    # Keys sin polígono
    sin_keys = set(gdf.loc[~has_geom, "_key"])
    print("    Total: %d  Con pol: %d  Sin pol: %d" % (total, before, len(sin_keys)), flush=True)

    stats = {}

    # ── 1. AH/CSA UTM → PIP / nearest ──────────────────────────────────────
    print("[recovery] 1. AH/CSA UTM coords...", flush=True)
    ah_pip = ah_near = 0
    for idx in gdf[~has_geom].index:
        # Try AH UTM first (urban), then CSA UTM (agricultural)
        utm_x = None
        utm_y = None
        source = None
        if "ah_utm_x" in gdf.columns:
            ax = gdf.at[idx, "ah_utm_x"]
            ay = gdf.at[idx, "ah_utm_y"]
            if not pd.isna(ax) and str(ax) not in ("", "None", "nan"):
                utm_x, utm_y, source = ax, ay, "ah"
        if utm_x is None and "csa_utm_x" in gdf.columns:
            cx = gdf.at[idx, "csa_utm_x"]
            cy = gdf.at[idx, "csa_utm_y"]
            if not pd.isna(cx) and str(cx) not in ("", "None", "nan"):
                utm_x, utm_y, source = cx, cy, "csa"
        if utm_x is None:
            continue
        try:
            lon, lat = tr.transform(float(utm_x), float(utm_y))
        except (ValueError, TypeError):
            continue
        pt = Point(lon, lat)
        # PIP
        found = False
        for c in tree.query(pt):
            if polys_4326.geometry.iloc[c].contains(pt):
                gdf.at[idx, "geometry"] = polys_4326.geometry.iloc[c]
                gdf.at[idx, "_match_method"] = "%s_utm_pip" % source
                gdf.at[idx, "_match_dist_m"] = 0.0
                ah_pip += 1
                found = True
                break
        if not found:
            ni = tree.nearest(pt)
            dist_m = pt.distance(polys_4326.geometry.iloc[ni]) * 111000
            gdf.at[idx, "geometry"] = polys_4326.geometry.iloc[ni]
            gdf.at[idx, "_match_method"] = "%s_utm_nearest" % source
            gdf.at[idx, "_match_dist_m"] = round(dist_m, 1)
            ah_near += 1
    print("    AH UTM PIP: %d  |  nearest: %d" % (ah_pip, ah_near), flush=True)
    stats["ah_utm_pip"] = ah_pip
    stats["ah_utm_nearest"] = ah_near

    # ── 2. Nearest unlimited (lat/lon lejos de polígonos) ────────────────────
    print("[recovery] 2. Nearest unlimited...", flush=True)
    has_geom = gdf.geometry.notna() & ~gdf.geometry.is_empty  # refresh
    n_unlimited = 0
    for idx in gdf[~has_geom].index:
        lat_v = gdf.at[idx, "lat"] if "lat" in gdf.columns else None
        lon_v = gdf.at[idx, "lon"] if "lon" in gdf.columns else None
        if pd.isna(lat_v) or str(lat_v) in ("", "nan", "None"):
            continue
        try:
            lat_f, lon_f = float(lat_v), float(lon_v)
            if not (-62 <= lat_f <= -17 and -80 <= lon_f <= -64):
                continue
        except (ValueError, TypeError):
            continue
        pt = Point(lon_f, lat_f)
        ni = tree.nearest(pt)
        dist_m = pt.distance(polys_4326.geometry.iloc[ni]) * 111000
        gdf.at[idx, "geometry"] = polys_4326.geometry.iloc[ni]
        gdf.at[idx, "_match_method"] = "nearest_unlimited"
        gdf.at[idx, "_match_dist_m"] = round(dist_m, 1)
        n_unlimited += 1
    print("    Nearest unlimited: %d" % n_unlimited, flush=True)
    stats["nearest_unlimited"] = n_unlimited

    # ── 3. OCR huérfanos ─────────────────────────────────────────────────────
    print("[recovery] 3. OCR huerfanos...", flush=True)
    has_geom = gdf.geometry.notna() & ~gdf.geometry.is_empty
    used_polys = set()
    matched_centroids = gdf[has_geom].geometry.centroid
    matched_tree = STRtree(matched_centroids.values)
    matched_gdf = gdf[has_geom].copy()
    matched_gdf["_mz"] = matched_gdf["manzana"].astype(str).str.strip().str.zfill(5)

    # Identify orphan polygon indices
    for idx in gdf[has_geom].index:
        g = gdf.at[idx, "geometry"]
        for c in tree.query(g.centroid):
            inter = polys_4326.geometry.iloc[c].intersection(g).area
            if inter / max(g.area, 1e-12) > 0.9:
                used_polys.add(c)
                break

    orphan_idxs = [i for i in range(len(polys))
                   if i not in used_polys and polys.geometry.iloc[i].area >= args.min_area_ocr]
    print("    Huerfanos >= %d m2: %d" % (args.min_area_ocr, len(orphan_idxs)), flush=True)

    n_ocr = 0
    if orphan_idxs and os.path.exists(args.tif):
        sin_geom_keys = set(gdf.loc[~(gdf.geometry.notna() & ~gdf.geometry.is_empty), "_key"])
        with rasterio.open(args.tif) as src:
            for pi in orphan_idxs:
                geom_3857 = polys.geometry.iloc[pi]
                predio_num = ocr_polygon(src, geom_3857)
                if predio_num is None:
                    continue

                # Deduce manzana from nearest matched polygon
                centroid_4326 = polys_4326.geometry.iloc[pi].centroid
                ni = matched_tree.nearest(centroid_4326)
                mz = matched_gdf.iloc[ni]["_mz"]
                predio_str = str(predio_num).zfill(5)
                target_key = mz + "_" + predio_str

                if target_key in sin_geom_keys:
                    target_idx = gdf.index[gdf["_key"] == target_key]
                    if len(target_idx) > 0:
                        gdf.at[target_idx[0], "geometry"] = polys_4326.geometry.iloc[pi]
                        gdf.at[target_idx[0], "_match_method"] = "ocr_orphan"
                        sin_geom_keys.discard(target_key)
                        n_ocr += 1
    print("    OCR matched: %d" % n_ocr, flush=True)
    stats["ocr_orphan"] = n_ocr

    # ── 4. Address inheritance ───────────────────────────────────────────────
    print("[recovery] 4. Address inheritance...", flush=True)
    has_geom = gdf.geometry.notna() & ~gdf.geometry.is_empty
    addr_col = "dc_direccion" if "dc_direccion" in gdf.columns else "direccion_sii"
    gdf["_addr"] = gdf[addr_col].apply(normalize_address) if addr_col in gdf.columns else None
    gdf["_mz"] = gdf["manzana"].astype(str).str.strip().str.zfill(5)

    addr_idx = {}
    for idx in gdf[has_geom & gdf["_addr"].notna()].index:
        key = (gdf.at[idx, "_mz"], gdf.at[idx, "_addr"])
        if key not in addr_idx:
            addr_idx[key] = idx

    n_addr = 0
    for idx in gdf[~has_geom & gdf["_addr"].notna()].index:
        key = (gdf.at[idx, "_mz"], gdf.at[idx, "_addr"])
        donor_idx = addr_idx.get(key)
        if donor_idx is not None:
            gdf.at[idx, "geometry"] = gdf.at[donor_idx, "geometry"]
            gdf.at[idx, "_match_method"] = "address_inheritance"
            n_addr += 1
    print("    Address inheritance: %d" % n_addr, flush=True)
    stats["address_inheritance"] = n_addr

    # ── Marcar predios sin resultado ────────────────────────────────────────
    has_geom_final = gdf.geometry.notna() & ~gdf.geometry.is_empty
    no_result = ~has_geom_final & ((gdf["_match_method"] == "") | gdf["_match_method"].isna())
    gdf.loc[no_result, "_match_method"] = "sin_resultado"
    print("[recovery] Predios sin resultado (ningún método): %d" % no_result.sum(), flush=True)

    # ── Guardar ──────────────────────────────────────────────────────────────
    after = has_geom_final.sum()
    recovered = after - before
    gdf.drop(columns=["_key", "_addr", "_mz"], errors="ignore", inplace=True)

    print("[recovery] Guardando GeoParquet...", flush=True)
    gdf.to_parquet(args.gpkg)

    elapsed = time.time() - t0_total
    print("", flush=True)
    print("=" * 60, flush=True)
    print("  Recovery: +%d predios en %.0fs" % (recovered, elapsed), flush=True)
    print("    AH UTM PIP:          %d" % ah_pip, flush=True)
    print("    AH UTM nearest:      %d" % ah_near, flush=True)
    print("    Nearest unlimited:   %d" % n_unlimited, flush=True)
    print("    OCR huerfanos:       %d" % n_ocr, flush=True)
    print("    Address inheritance: %d" % n_addr, flush=True)
    print("  Total: %d → %d (%.2f%%)" % (before, after, after / total * 100), flush=True)
    print("  Sin poligono: %d (%.2f%%)" % (total - after, (total - after) / total * 100), flush=True)
    print("=" * 60, flush=True)

    # Update metrics
    if args.metrics and os.path.exists(args.metrics):
        m = json.load(open(args.metrics))
        m["recovery"] = stats
        m["recovery"]["total_recovered"] = recovered
        m["recovery"]["cobertura_post_recovery"] = round(after / total * 100, 2)
        m["recovery"]["tiempo_s"] = round(elapsed)
        with open(args.metrics, "w") as f:
            json.dump(m, f, indent=2)


if __name__ == "__main__":
    main()
