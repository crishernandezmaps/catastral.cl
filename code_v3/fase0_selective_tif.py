#!/usr/bin/env python3
"""
fase0_selective_tif.py — Descarga supercells WMS z19 y vectoriza por bloques.

Descarga supercells de 1024×1024 px (4×4 tiles z19) del polígono BCN
con buffer de 1.5 km, luego vectoriza en bloques de 16384×16384 px
(idéntico a vectorize_simple.py).

Pasos:
  1. Calcula supercells z19 dentro del polígono BCN buffereado
  2. Descarga supercells en paralelo usando túneles VPN (70 túneles × 2 threads)
  3. Agrupa supercells en bloques de 16×16 supercells (16384×16384 px)
  4. Vectoriza cada bloque (urbano + agrícola)
  5. Merge entre bloques con unary_union + fill holes

Usage:
    python3 fase0_selective_tif.py --comuna 7301 --wms-name LINARES \\
        --csv /tmp/fase0_v2/7301/comuna=7301.csv \\
        --outdir /tmp/fase0_v2/7301 --tunnels 70
"""

import argparse
import json
import math
import os
import shutil
import subprocess
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
from PIL import Image

# ─── Constants ───────────────────────────────────────────────────────────────

ZOOM = 19
SUPER_SIZE = 4          # 4×4 tiles = 1024×1024 px per WMS request
SUPER_PX = SUPER_SIZE * 256  # 1024
BLOCK_SUPERS = 16       # 16×16 supercells per vectorization block
BLOCK_PX = BLOCK_SUPERS * SUPER_PX  # 16384
BLOCK_TILES = BLOCK_SUPERS * SUPER_SIZE  # 64 tiles per block axis
OVERLAP_SUPERS = 1      # 1 supercell overlap between blocks (1024px)
OVERLAP_PX = OVERLAP_SUPERS * SUPER_PX  # 1024
STEP_SUPERS = BLOCK_SUPERS - OVERLAP_SUPERS  # 15 supercells step
THREADS_PER_TUNNEL = 2
BUFFER_KM = 1.5

# Vectorization thresholds (same as vectorize_simple.py)
URBAN_DN_MIN = 160
URBAN_DN_MAX = 200
URBAN_MIN_AREA = 1
URBAN_MAX_AREA = 50_000
AGRI_ALPHA_FILL = 179
AGRI_BORDER_DILATION = 3
AGRI_MIN_AREA_M2 = 5_000
HOLE_MAX_URBAN = 50
HOLE_MAX_AGRI = 500

EPSG3857_HALF = 20037508.34
TILE_SIZE_M = (EPSG3857_HALF * 2) / (2 ** ZOOM)

COMUNAS_SHP = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "Comunas", "comunas.shp"
)

WMS_URL = "https://www4.sii.cl/mapasui/services/ui/wmsProxyService/call"


# ─── Geometry helpers ────────────────────────────────────────────────────────

def supercell_bbox_3857(sc_x, sc_y):
    """EPSG:3857 bbox for a supercell at tile coords (sc_x, sc_y)."""
    left = -EPSG3857_HALF + sc_x * TILE_SIZE_M
    top = EPSG3857_HALF - sc_y * TILE_SIZE_M
    right = left + SUPER_SIZE * TILE_SIZE_M
    bottom = top - SUPER_SIZE * TILE_SIZE_M
    return left, bottom, right, top


def block_bbox_3857(bx, by):
    """EPSG:3857 bbox for a block at block coords (bx, by)."""
    left = -EPSG3857_HALF + bx * BLOCK_TILES * TILE_SIZE_M
    top = EPSG3857_HALF - by * BLOCK_TILES * TILE_SIZE_M
    right = left + BLOCK_TILES * TILE_SIZE_M
    bottom = top - BLOCK_TILES * TILE_SIZE_M
    return left, bottom, right, top


# ─── Step 1: Calculate supercell set from BCN polygon ────────────────────────

def _normalize(s):
    """Strip accents and uppercase for matching."""
    import unicodedata
    return unicodedata.normalize("NFD", str(s)).encode("ascii", "ignore").decode().upper().strip()


def calc_supercell_set(cod, buffer_km=BUFFER_KM):
    """Calculate supercells covering the BCN commune polygon + buffer."""
    import geopandas as gpd
    from shapely.geometry import box

    if not os.path.exists(COMUNAS_SHP):
        print(f"    ⚠ Comunas shapefile not found: {COMUNAS_SHP}", flush=True)
        return None

    comunas = gpd.read_file(COMUNAS_SHP, engine="pyogrio")
    comunas["_norm"] = comunas["Comuna"].apply(_normalize)

    # Find comuna by WMS name (exact match, accent-insensitive)
    from fase0_config import load_wms_names
    wms_names = load_wms_names()
    comuna_name = wms_names.get(cod, "")
    match = gpd.GeoDataFrame()
    if comuna_name:
        search = _normalize(comuna_name.replace("_", " "))
        match = comunas[comunas["_norm"] == search]
        # Fallback: try first word (handles SANTIAGO_CENTRO → Santiago)
        if len(match) == 0 and " " in search:
            first_word = search.split()[0]
            match = comunas[comunas["_norm"] == first_word]
    if len(match) == 0:
        for fmt in [str(cod), str(cod).zfill(5)]:
            match = comunas[comunas["cod_comuna"].astype(str).str.strip() == fmt]
            if len(match) > 0:
                break
    if len(match) == 0:
        print(f"    ⚠ Comuna {cod} ({comuna_name}) not found in shapefile", flush=True)
        return None
    if len(match) > 1:
        print(f"    ⚠ Multiple matches for {cod}: {match['Comuna'].tolist()}", flush=True)
        return None
    print(f"    BCN match: {match.iloc[0]['Comuna']} (cod {match.iloc[0]['cod_comuna']})",
          flush=True)

    # Buffer in EPSG:3857 (meters)
    match_3857 = match.to_crs("EPSG:3857") if comunas.crs.to_epsg() != 3857 else match
    geom = match_3857.iloc[0].geometry.buffer(buffer_km * 1000)
    print(f"    Buffer: {buffer_km} km", flush=True)

    # Supercell grid bounds
    minx, miny, maxx, maxy = geom.bounds
    sc_size = SUPER_SIZE * TILE_SIZE_M

    sx_min = int((minx + EPSG3857_HALF) / TILE_SIZE_M) // SUPER_SIZE * SUPER_SIZE
    sx_max = int((maxx + EPSG3857_HALF) / TILE_SIZE_M) // SUPER_SIZE * SUPER_SIZE
    sy_min = int((EPSG3857_HALF - maxy) / TILE_SIZE_M) // SUPER_SIZE * SUPER_SIZE
    sy_max = int((EPSG3857_HALF - miny) / TILE_SIZE_M) // SUPER_SIZE * SUPER_SIZE

    # Keep supercells that intersect the buffered polygon
    sc_set = set()
    for sy in range(sy_min, sy_max + SUPER_SIZE, SUPER_SIZE):
        for sx in range(sx_min, sx_max + SUPER_SIZE, SUPER_SIZE):
            left = -EPSG3857_HALF + sx * TILE_SIZE_M
            top = EPSG3857_HALF - sy * TILE_SIZE_M
            right = left + sc_size
            bottom = top - sc_size
            if geom.intersects(box(left, bottom, right, top)):
                sc_set.add((sx, sy))

    print(f"    Supercells: {len(sc_set):,} (1024×1024 px each)", flush=True)
    return sc_set


# ─── Step 2: Download supercells via VPN tunnels (flock queue + work stealing) ─

SC_WORKER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sc_worker.py")
STALL_THRESHOLD_S = 60


def _rotate_tunnel(tunnel_id):
    """Rotate a VPN tunnel to a different Mullvad relay."""
    import json as _json
    import random
    from fase0_config import MULLVAD_RELAYS_PATH
    wg = f"wg{tunnel_id}"
    ns = f"vpn{tunnel_id}"
    try:
        with open(MULLVAD_RELAYS_PATH) as f:
            relays = _json.load(f)
        if not relays:
            return
        relay = random.choice(relays)
        old_pk = subprocess.check_output(
            f"ip netns exec {ns} wg show {wg} peers".split(),
            timeout=5).decode().strip().split("\n")[0]
        subprocess.run(
            f"ip netns exec {ns} wg set {wg} peer {old_pk} remove".split(),
            timeout=5)
        subprocess.run(
            f"ip netns exec {ns} wg set {wg} peer {relay['public_key']} "
            f"endpoint {relay['ipv4_addr_in']}:51820 allowed-ips 0.0.0.0/0".split(),
            timeout=5)
        loc = relay.get("hostname", "?")[:6]
        print(f"  [ROTATE] T{tunnel_id} → {loc}", flush=True)
    except Exception:
        pass


def download_supercells(sc_set, out_dir, cod, wms_name, n_tunnels):
    """Download supercells using flock queue + work stealing + IP rotation.

    Same pattern as fase0_orchestrator: shared queue file, workers compete
    for items via flock, stall detection rotates blocked IPs.
    """
    from fase0_config import VENV_PYTHON

    tiles_dir = os.path.join(out_dir, "tiles_z19")
    os.makedirs(tiles_dir, exist_ok=True)

    # Filter cached
    to_download = []
    for sc_x, sc_y in sc_set:
        path = os.path.join(tiles_dir, f"sc_{sc_x}_{sc_y}.png")
        if not (os.path.exists(path) and os.path.getsize(path) > 100):
            to_download.append((sc_x, sc_y))

    cached = len(sc_set) - len(to_download)
    print(f"    {cached:,} cached, {len(to_download):,} to download", flush=True)

    if not to_download:
        return tiles_dir

    # Write queue file
    queue_path = os.path.join(out_dir, "sc_queue.txt")
    with open(queue_path, "w") as f:
        for sc_x, sc_y in to_download:
            bb = supercell_bbox_3857(sc_x, sc_y)
            bbox_str = f"{bb[0]},{bb[1]},{bb[2]},{bb[3]}"
            f.write(f"{sc_x}|{sc_y}|{bbox_str}\n")

    # Launch workers (one per tunnel)
    n_use = min(n_tunnels, len(to_download))
    print(f"    Launching {n_use} workers...", flush=True)

    procs = []
    counter_files = []
    for i in range(n_use):
        counter = os.path.join(out_dir, f"sc_worker_{i}.count")
        with open(counter, "w") as f:
            f.write("0")
        counter_files.append(counter)

        cmd = [
            "ip", "netns", "exec", f"vpn{i}",
            VENV_PYTHON, "-u", SC_WORKER,
            "--tunnel", str(i),
            "--queue", queue_path,
            "--outdir", tiles_dir,
            "--comuna", str(cod),
            "--wms-name", wms_name,
            "--counter", counter,
        ]
        proc = subprocess.Popen(
            cmd, cwd=os.path.dirname(SC_WORKER),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        procs.append(proc)

    # Monitor with stall detection + IP rotation
    t0 = time.time()
    total_pending = len(to_download)
    last_progress = {i: (0, time.time()) for i in range(n_use)}

    while True:
        alive = sum(1 for p in procs if p.poll() is None)
        if alive == 0:
            break

        total_done = 0
        for i, cf in enumerate(counter_files):
            try:
                cur = int(open(cf).read().strip())
            except Exception:
                cur = 0
            total_done += cur

            # Stall detection
            prev_count, prev_time = last_progress[i]
            if cur > prev_count:
                last_progress[i] = (cur, time.time())
            elif procs[i].poll() is None and time.time() - prev_time > STALL_THRESHOLD_S:
                print(f"\n  [STALL] T{i} (stuck at {cur})", flush=True)
                _rotate_tunnel(i)
                last_progress[i] = (cur, time.time())

        elapsed = time.time() - t0
        rate = total_done / max(elapsed, 1)
        remaining = total_pending - total_done
        eta = remaining / max(rate, 0.1)
        print(f"\r    [{cod}] {total_done + cached:,}/{len(sc_set):,} "
              f"({(total_done + cached) / len(sc_set) * 100:.1f}%) | "
              f"{alive} workers | {rate:.1f}/s | ETA {eta / 60:.1f}min",
              end="", flush=True)

        time.sleep(5)

    print(flush=True)
    elapsed = time.time() - t0

    # Final count
    final = sum(1 for f in os.listdir(tiles_dir)
                if f.startswith("sc_") and f.endswith(".png")
                and os.path.getsize(os.path.join(tiles_dir, f)) > 100)
    still_missing = len(sc_set) - final
    print(f"    Done: {final:,}/{len(sc_set):,} supercells in {elapsed:.0f}s "
          f"({final / max(elapsed, 1):.0f}/s)", flush=True)
    if still_missing > 0:
        print(f"    ⚠ {still_missing:,} supercells still missing", flush=True)

    # Cleanup counter files
    for cf in counter_files:
        if os.path.exists(cf):
            os.remove(cf)
    if os.path.exists(queue_path):
        os.remove(queue_path)

    return tiles_dir


# ─── Step 3: Vectorize supercells in large blocks with overlap ───────────────

def vectorize_block_image(block_img, transform):
    """Vectorize a single block image (urban + agricultural). Returns list of polygons."""
    from rasterio.features import shapes
    from shapely.geometry import shape, Polygon, MultiPolygon
    from shapely.validation import make_valid
    from scipy import ndimage

    alpha = block_img[:, :, 3]
    if (alpha > 0).sum() < 100:
        return []

    r_band = block_img[:, :, 0]
    results = []

    # ── Urban ────────────────────────────────────────────────────────────
    urban_fill = (r_band >= URBAN_DN_MIN) & (r_band <= URBAN_DN_MAX) & (alpha == 255)
    urban_border = (r_band < URBAN_DN_MIN) & (alpha == 255)
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
                    if p.area < URBAN_MIN_AREA or p.area > URBAN_MAX_AREA:
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


def vectorize_blocks(tiles_dir, out_vectors):
    """Vectorize supercells in overlapping blocks of 16384×16384 px.

    Blocks overlap by 1 supercell (1024px) on each side, ensuring polygons
    at block boundaries are duplicated. unary_union then merges them cleanly,
    eliminating the visible grid lines from non-overlapping blocks.
    """
    import geopandas as gpd
    from rasterio.transform import Affine
    from shapely.geometry import Polygon
    from shapely.ops import unary_union

    # Index supercells by (sc_x, sc_y) in supercell coordinates
    sc_paths = {}
    for fname in os.listdir(tiles_dir):
        if not fname.startswith("sc_") or not fname.endswith(".png"):
            continue
        fpath = os.path.join(tiles_dir, fname)
        if os.path.getsize(fpath) < 100:
            continue
        parts = fname.replace(".png", "").split("_")
        sc_paths[(int(parts[1]), int(parts[2]))] = fpath

    if not sc_paths:
        print("    No supercells to vectorize!", flush=True)
        return None

    # Find supercell bounds (in supercell-aligned tile coords)
    all_sc_x = [k[0] for k in sc_paths]
    all_sc_y = [k[1] for k in sc_paths]
    min_sc_x, max_sc_x = min(all_sc_x), max(all_sc_x)
    min_sc_y, max_sc_y = min(all_sc_y), max(all_sc_y)

    # Generate overlapping block origins (step = BLOCK_TILES - OVERLAP in tiles)
    step_tiles = STEP_SUPERS * SUPER_SIZE  # 15 * 4 = 60 tiles
    block_origins = []
    oy = min_sc_y
    while oy <= max_sc_y:
        ox = min_sc_x
        while ox <= max_sc_x:
            block_origins.append((ox, oy))
            ox += step_tiles
        oy += step_tiles

    print(f"    {len(sc_paths):,} supercells → {len(block_origins):,} blocks "
          f"({BLOCK_PX}×{BLOCK_PX} px, overlap {OVERLAP_PX}px)", flush=True)

    all_polys = []
    blocks_with_content = 0

    for i, (ox, oy) in enumerate(block_origins):
        # Assemble block image from all supercells in range [ox, ox+BLOCK_TILES)
        block_img = np.zeros((BLOCK_PX, BLOCK_PX, 4), dtype=np.uint8)
        has_content = False

        for sc_x in range(ox, ox + BLOCK_TILES, SUPER_SIZE):
            for sc_y in range(oy, oy + BLOCK_TILES, SUPER_SIZE):
                path = sc_paths.get((sc_x, sc_y))
                if path is None:
                    continue
                try:
                    img = Image.open(path).convert("RGBA")
                    arr = np.array(img)
                    px = (sc_x - ox) * 256
                    py = (sc_y - oy) * 256
                    h, w = arr.shape[:2]
                    block_img[py:py + h, px:px + w] = arr
                    has_content = True
                except Exception:
                    pass

        if not has_content:
            continue

        # Block georeference
        left = -EPSG3857_HALF + ox * TILE_SIZE_M
        top = EPSG3857_HALF - oy * TILE_SIZE_M
        right = left + BLOCK_TILES * TILE_SIZE_M
        bottom = top - BLOCK_TILES * TILE_SIZE_M
        res_x = (right - left) / BLOCK_PX
        res_y = (top - bottom) / BLOCK_PX
        transform = Affine(res_x, 0, left, 0, -res_y, top)

        polys = vectorize_block_image(block_img, transform)
        all_polys.extend(polys)

        if polys:
            blocks_with_content += 1

        if (i + 1) % 10 == 0 or i + 1 == len(block_origins):
            print(f"    Block {i+1}/{len(block_origins)}: "
                  f"{blocks_with_content} with content | "
                  f"{len(all_polys):,} polys", flush=True)

        del block_img

    print(f"    Raw polygons: {len(all_polys):,}", flush=True)

    if not all_polys:
        print("    No polygons found!", flush=True)
        return None

    # Merge overlapping polygons (overlap ensures duplicates at boundaries)
    print("    Merging block boundaries (unary_union)...", flush=True)
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

    # Save as GPKG in EPSG:4326
    gdf = gpd.GeoDataFrame(geometry=cleaned, crs="EPSG:3857")
    gdf = gdf.to_crs("EPSG:4326")
    gdf.to_file(out_vectors, engine="pyogrio")
    vec_mb = os.path.getsize(out_vectors) / 1e6
    print(f"    Saved: {len(gdf):,} polygons ({vec_mb:.0f} MB)", flush=True)

    return out_vectors


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--comuna", type=int, required=True)
    parser.add_argument("--wms-name", required=True)
    parser.add_argument("--csv", required=True, help="CSV pipe-separated")
    parser.add_argument("--outdir", required=True)
    parser.add_argument("--tunnels", type=int, default=70)
    parser.add_argument("--output-vectors", default=None)
    args = parser.parse_args()

    t0 = time.time()
    cod = args.comuna
    out_vectors = args.output_vectors or os.path.join(
        args.outdir, f"vectors_tif_{cod}.gpkg"
    )

    # 1. Calculate supercell set
    print("[selective-z19] Calculating supercell set...", flush=True)
    sc_set = calc_supercell_set(cod)
    if sc_set is None:
        print("    ⚠ Cannot determine supercell set. Skipping.", flush=True)
        return

    # 2. Download supercells
    print("[selective-z19] Downloading supercells (1024×1024 px)...", flush=True)
    tiles_dir = download_supercells(
        sc_set, args.outdir, cod, args.wms_name, args.tunnels
    )

    # 3. Vectorize
    print("[selective-z19] Vectorizing blocks (16384×16384 px)...", flush=True)
    vectorize_blocks(tiles_dir, out_vectors)

    elapsed = time.time() - t0
    print(f"[selective-z19] Done in {elapsed/60:.1f} min", flush=True)


if __name__ == "__main__":
    main()
