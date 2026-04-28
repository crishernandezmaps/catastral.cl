#!/usr/bin/env python3
"""
optimize_parquet.py — Post-proceso: suaviza geometrías del pipeline fase0.

Aplica simplify diferenciado:
  - Predios matcheados: ~1m tolerancia (suaviza pixel staircases)
  - Polígonos huérfanos: ~5m tolerancia (reduce mega-polígonos agrícolas)

No modifica el pipeline — es un paso independiente.

Usage:
    python3 optimize_parquet.py --input comuna=7301.parquet --output comuna=7301_opt.parquet
"""

import argparse
import os
import time

import geopandas as gpd
from shapely.validation import make_valid


def optimize(gdf):
    """Simplify all geometries to remove pixel staircases."""
    has_geom = gdf.geometry.notnull() & ~gdf.geometry.is_empty
    is_orphan = gdf["_match_method"] == "unmatched_polygon"

    matched_mask = has_geom & ~is_orphan
    orphan_mask = has_geom & is_orphan

    n_matched = matched_mask.sum()
    n_orphan = orphan_mask.sum()

    # Matched predios: gentle simplify (~1m) to smooth pixel edges
    if n_matched > 0:
        gdf.loc[matched_mask, "geometry"] = gdf.loc[matched_mask, "geometry"].simplify(
            0.00001, preserve_topology=True
        )

    # Orphan polygons: stronger simplify (~5m) to reduce vertex count
    if n_orphan > 0:
        gdf.loc[orphan_mask, "geometry"] = gdf.loc[orphan_mask, "geometry"].simplify(
            0.00005, preserve_topology=True
        )

    # Fix any invalid geometries
    for idx in gdf[has_geom].index:
        g = gdf.at[idx, "geometry"]
        if g is not None and not g.is_empty and not g.is_valid:
            gdf.at[idx, "geometry"] = make_valid(g)

    print(f"  Matched: {n_matched:,} (simplify ~1m)", flush=True)
    print(f"  Orphans: {n_orphan:,} (simplify ~5m)", flush=True)

    return gdf


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    t0 = time.time()
    print(f"Loading {args.input}...", flush=True)
    gdf = gpd.read_parquet(args.input)
    print(f"  {len(gdf):,} rows", flush=True)

    print("Optimizing...", flush=True)
    gdf = optimize(gdf)

    print(f"Saving {args.output}...", flush=True)
    gdf.to_parquet(args.output)

    in_mb = os.path.getsize(args.input) / 1e6
    out_mb = os.path.getsize(args.output) / 1e6
    print(f"Done in {time.time()-t0:.0f}s: {in_mb:.0f} MB → {out_mb:.0f} MB", flush=True)


if __name__ == "__main__":
    main()
