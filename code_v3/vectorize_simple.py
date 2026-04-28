#!/usr/bin/env python3
"""
vectorize_simple.py — Vectoriza un TIF en bloques grandes con overlap.

Lee el TIF en bloques de 16384×16384 px (~5×5 km a z19, ~4 GB RAM cada uno).
Cada bloque se vectoriza como una imagen continua (sin artefactos internos).
Solo las costuras entre bloques necesitan merge (buffer trick).

Para un TIF de 96K×96K px: 36 bloques, ~36 costuras. Producción limpia.

Usage:
    python3 vectorize_simple.py --tif /tmp/14202.tif --output /tmp/14202_vectors.gpkg
"""

import argparse
import os
import time

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.features import shapes
from rasterio.windows import Window
from scipy import ndimage
from shapely.geometry import shape, Polygon, MultiPolygon
from shapely.ops import unary_union
from shapely.validation import make_valid

# Thresholds
URBAN_DN_MIN = 160
URBAN_DN_MAX = 200
URBAN_MIN_AREA = 1
URBAN_MAX_AREA = 50_000
AGRI_ALPHA_FILL = 179
AGRI_BORDER_DILATION = 3
AGRI_MIN_AREA_M2 = 5_000
HOLE_MAX_URBAN = 50
HOLE_MAX_AGRI = 500

BLOCK_SIZE = 16384  # ~5 km at z19 resolution, ~4 GB RAM per block
OVERLAP = 128       # overlap between blocks to avoid cut polygons


def vectorize_block(data, transform):
    """Vectorize a single block (urban + agricultural)."""
    r = data[0]
    alpha = data[3]
    results = []

    # ── Urban ────────────────────────────────────────────────────────────
    urban_fill = (r >= URBAN_DN_MIN) & (r <= URBAN_DN_MAX) & (alpha == 255)
    urban_border = (r < URBAN_DN_MIN) & (alpha == 255)
    if urban_fill.sum() > 50:
        dilated = ndimage.binary_dilation(urban_border, iterations=1)
        interior = urban_fill & ~dilated
        labeled, n = ndimage.label(interior)
        if n > 0:
            for geom_dict, val in shapes(labeled.astype(np.int32),
                                         mask=labeled > 0, connectivity=8,
                                         transform=transform):
                if val == 0:
                    continue
                poly = shape(geom_dict)
                if not poly.is_valid:
                    poly = make_valid(poly)
                if not poly.is_valid:
                    continue
                for p in (list(poly.geoms) if isinstance(poly, MultiPolygon)
                          else [poly] if isinstance(poly, Polygon) else []):
                    area = p.area
                    if area < URBAN_MIN_AREA or area > URBAN_MAX_AREA:
                        continue
                    if p.interiors:
                        kept = [h for h in p.interiors if Polygon(h).area >= HOLE_MAX_URBAN]
                        if len(kept) != len(list(p.interiors)):
                            p = Polygon(p.exterior, kept)
                    results.append(p)

    # ── Agricultural ─────────────────────────────────────────────────────
    agri_fill = (alpha == AGRI_ALPHA_FILL).astype(np.uint8)
    agri_border = alpha > AGRI_ALPHA_FILL
    if agri_fill.sum() > 100 and agri_border.sum() > 5:
        dilated = ndimage.binary_dilation(agri_border, iterations=AGRI_BORDER_DILATION)
        interior = agri_fill & ~dilated.astype(np.uint8)
        labeled, n = ndimage.label(interior)
        if n > 0:
            for geom_dict, val in shapes(labeled.astype(np.int32),
                                         mask=labeled > 0, connectivity=8,
                                         transform=transform):
                if val == 0:
                    continue
                poly = shape(geom_dict)
                if not poly.is_valid:
                    poly = make_valid(poly)
                if not poly.is_valid:
                    continue
                for p in (list(poly.geoms) if isinstance(poly, MultiPolygon)
                          else [poly] if isinstance(poly, Polygon) else []):
                    if p.area < AGRI_MIN_AREA_M2:
                        continue
                    if p.interiors:
                        kept = [h for h in p.interiors if Polygon(h).area >= HOLE_MAX_AGRI]
                        if len(kept) != len(list(p.interiors)):
                            p = Polygon(p.exterior, kept)
                    results.append(p)

    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--tif", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    t0 = time.time()

    with rasterio.open(args.tif) as src:
        width, height = src.width, src.height
        full_transform = src.transform
        crs = src.crs
        print(f"[vectorize] TIF: {width}x{height} px, CRS: {crs}", flush=True)

        # Calculate blocks
        step = BLOCK_SIZE - OVERLAP
        blocks = []
        for row in range(0, height, step):
            for col in range(0, width, step):
                w = min(BLOCK_SIZE, width - col)
                h = min(BLOCK_SIZE, height - row)
                if w < 256 or h < 256:
                    continue
                blocks.append(Window(col, row, w, h))

        print(f"    {len(blocks)} blocks of {BLOCK_SIZE}px "
              f"(~{BLOCK_SIZE * 0.3 / 1000:.1f} km, overlap={OVERLAP}px)",
              flush=True)

        all_polys = []
        for i, win in enumerate(blocks):
            data = src.read(window=win)
            block_transform = rasterio.windows.transform(win, full_transform)

            # Skip empty blocks
            alpha = data[3]
            if (alpha > 0).sum() < 100:
                if (i + 1) % 10 == 0 or i + 1 == len(blocks):
                    print(f"    Block {i+1}/{len(blocks)}: skip (empty) | "
                          f"total: {len(all_polys):,} polys", flush=True)
                continue

            polys = vectorize_block(data, block_transform)
            all_polys.extend(polys)

            if (i + 1) % 10 == 0 or i + 1 == len(blocks):
                print(f"    Block {i+1}/{len(blocks)}: +{len(polys)} | "
                      f"total: {len(all_polys):,} polys", flush=True)

            del data  # free memory

    print(f"    Raw polygons: {len(all_polys):,}", flush=True)

    if not all_polys:
        print("[vectorize] No polygons found!", flush=True)
        return

    # ── Merge block boundaries ──────────────────────────────────────────
    # Overlap between blocks ensures polygons at boundaries already overlap,
    # so a simple unary_union merges them without needing buffer tricks.
    print("[vectorize] Merging block boundaries (unary_union)...", flush=True)
    merged = unary_union(all_polys)

    if merged.geom_type == "MultiPolygon":
        final_polys = list(merged.geoms)
    elif merged.geom_type == "Polygon":
        final_polys = [merged]
    else:
        final_polys = []

    # Final hole-fill
    cleaned = []
    for p in final_polys:
        if p.area < 500:
            continue
        if p.interiors:
            kept = [h for h in p.interiors if Polygon(h).area >= HOLE_MAX_AGRI]
            if len(kept) != len(list(p.interiors)):
                p = Polygon(p.exterior, kept)
        cleaned.append(p)

    print(f"    After merge + hole-fill: {len(cleaned):,} polygons", flush=True)

    # ── Save ────────────────────────────────────────────────────────────
    print(f"[vectorize] Saving...", flush=True)
    gdf = gpd.GeoDataFrame(geometry=cleaned, crs=crs)
    gdf.to_file(args.output, engine="pyogrio")

    elapsed = time.time() - t0
    out_mb = os.path.getsize(args.output) / 1e6
    print(f"[vectorize] Done in {elapsed:.0f}s — {len(cleaned):,} polygons, "
          f"{out_mb:.0f} MB", flush=True)


if __name__ == "__main__":
    main()
