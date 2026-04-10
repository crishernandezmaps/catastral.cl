#!/usr/bin/env python3
"""
Descarga un chunk de tiles WMS del SII usando supercells (4x4 tiles por request).

Cada instancia descarga supercells donde (supercell_index % total_chunks == chunk_id).
Las supercells agrupan 4x4 tiles (16 tiles) en un solo request WMS de 1024x1024 px,
reduciendo ~9x el numero de requests HTTP vs tiles individuales de 256x256.

Diseñado para correr dentro de un network namespace WireGuard.
Compatible con batch_tif_30ns.sh (misma interfaz CLI y formato de progress).

Uso:
    python3 download_chunk.py \
        --comuna 13101 --nombre SANTIAGO_CENTRO \
        --bbox "-70.67,-33.46,-70.63,-33.42" --zoom 19 \
        --chunk 0 --total-chunks 30 \
        --out-dir /tmp/tif_13101 --workers 2

Progress se reporta a stderr con prefijo PROGRESS/DONE para el monitor.
"""

import argparse
import io
import math
import os
import sys
import threading
import time

import requests
from requests.adapters import HTTPAdapter
from PIL import Image

BASE_URL = "https://www4.sii.cl/mapasui/services/ui/wmsProxyService/call"

SUPER_SIZE = 4                  # 4x4 tiles per supercell
SUPER_PX = SUPER_SIZE * 256     # 1024 px per side


def ll2t(lat, lon, z):
    lr = math.radians(lat)
    nn = 2 ** z
    return (
        int((lon + 180) / 360 * nn),
        int((1 - math.asinh(math.tan(lr)) / math.pi) / 2 * nn),
    )


def tile_to_bbox_3857(x, y, zoom):
    world_size = 20037508.34 * 2
    tile_size = world_size / (2 ** zoom)
    minx = -20037508.34 + x * tile_size
    maxy = 20037508.34 - y * tile_size
    return (minx, maxy - tile_size, minx + tile_size, maxy)


LAYER_NOT_FOUND = "LAYER_NOT_FOUND"

# Thread-local session for TLS connection reuse
_tls = threading.local()


def _get_session():
    if not hasattr(_tls, "s"):
        _tls.s = requests.Session()
        _tls.s.mount("https://", HTTPAdapter(pool_connections=2, pool_maxsize=2))
    return _tls.s


def download_supercell(cod, nombre, scx, scy, zoom, out_dir,
                       grid_sx, grid_mx, grid_sy, grid_my, retries=3):
    """Download a 4x4 supercell (1024x1024 px) and split into individual 256x256 tiles.

    Returns (n_ok, n_fail, is_layer_not_found)
    """
    session = _get_session()
    tiles_dir = os.path.join(out_dir, "tiles")

    # Determine which tiles in this supercell fall within the grid
    valid_tiles = []
    for dy in range(SUPER_SIZE):
        for dx in range(SUPER_SIZE):
            tx, ty = scx + dx, scy + dy
            if grid_sx <= tx <= grid_mx and grid_sy <= ty <= grid_my:
                valid_tiles.append((tx, ty, dx, dy))

    if not valid_tiles:
        return (0, 0, False)

    # Skip tiles already on disk (work stealing support)
    pending = [(tx, ty, dx, dy) for tx, ty, dx, dy in valid_tiles
               if not os.path.exists(os.path.join(tiles_dir, f"tile_{tx}_{ty}.png"))]
    already_ok = len(valid_tiles) - len(pending)

    if not pending:
        return (already_ok, 0, False)

    # Supercell bbox: top-left of (scx, scy) to bottom-right of (scx+3, scy+3)
    tl = tile_to_bbox_3857(scx, scy, zoom)
    br = tile_to_bbox_3857(scx + SUPER_SIZE - 1, scy + SUPER_SIZE - 1, zoom)
    sc_bbox = f"{tl[0]},{br[1]},{br[2]},{tl[3]}"

    params = {
        "service": "WMS",
        "request": "GetMap",
        "layers": f"sii:BR_CART_{nombre}_WMS",
        "styles": "PREDIOS_WMS_V0",
        "format": "image/png",
        "transparent": "true",
        "version": "1.1.1",
        "comuna": cod,
        "eac": "0",
        "eacano": "0",
        "height": str(SUPER_PX),
        "width": str(SUPER_PX),
        "srs": "EPSG:3857",
        "bbox": sc_bbox,
    }

    for attempt in range(retries):
        try:
            resp = session.get(BASE_URL, params=params, timeout=60)
            if resp.status_code == 200:
                content = resp.content
                if content[:5] == b"<?xml" and b"LayerNotDefined" in content:
                    return (0, len(pending), True)

                img = Image.open(io.BytesIO(content))

                for tx, ty, dx, dy in pending:
                    crop = img.crop((dx * 256, dy * 256, (dx + 1) * 256, (dy + 1) * 256))
                    crop.save(os.path.join(tiles_dir, f"tile_{tx}_{ty}.png"))

                return (already_ok + len(pending), 0, False)
            elif attempt < retries - 1:
                time.sleep(1)
        except Exception:
            if attempt < retries - 1:
                time.sleep(2)

    return (already_ok, len(pending), False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--comuna", required=True)
    parser.add_argument("--nombre", required=True, help="Nombre WMS")
    parser.add_argument("--bbox", required=True, help="min_lon,min_lat,max_lon,max_lat")
    parser.add_argument("--zoom", type=int, default=19)
    parser.add_argument("--chunk", type=int, required=True)
    parser.add_argument("--total-chunks", type=int, required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--progress-every", type=int, default=15)
    args = parser.parse_args()

    bbox = tuple(float(x) for x in args.bbox.split(","))
    min_lon, min_lat, max_lon, max_lat = bbox

    sx, sy = ll2t(max_lat, min_lon, args.zoom)
    mx, my = ll2t(min_lat, max_lon, args.zoom)
    if sx > mx:
        sx, mx = mx, sx
    if sy > my:
        sy, my = my, sy

    # Build supercell grid aligned to SUPER_SIZE boundaries
    aligned_sx = sx - (sx % SUPER_SIZE)
    aligned_sy = sy - (sy % SUPER_SIZE)

    all_supercells = []  # (scx, scy, n_tiles_in_grid)
    for scy in range(aligned_sy, my + 1, SUPER_SIZE):
        for scx in range(aligned_sx, mx + 1, SUPER_SIZE):
            n = 0
            for dy in range(SUPER_SIZE):
                for dx in range(SUPER_SIZE):
                    if sx <= scx + dx <= mx and sy <= scy + dy <= my:
                        n += 1
            if n > 0:
                all_supercells.append((scx, scy, n))

    # Distribute supercells to this chunk
    my_supercells = [(scx, scy, n) for i, (scx, scy, n)
                     in enumerate(all_supercells)
                     if i % args.total_chunks == args.chunk]

    my_total_tiles = sum(n for _, _, n in my_supercells)

    os.makedirs(os.path.join(args.out_dir, "tiles"), exist_ok=True)

    ok = 0
    fail = 0
    t0 = time.time()
    last_report = t0

    from concurrent.futures import ThreadPoolExecutor, as_completed

    # Pre-check: skip supercells where all tiles already on disk
    tiles_dir = os.path.join(args.out_dir, "tiles")
    pending_scs = []
    for scx, scy, n in my_supercells:
        all_exist = True
        for dy in range(SUPER_SIZE):
            for dx in range(SUPER_SIZE):
                tx, ty = scx + dx, scy + dy
                if sx <= tx <= mx and sy <= ty <= my:
                    if not os.path.exists(os.path.join(tiles_dir, f"tile_{tx}_{ty}.png")):
                        all_exist = False
                        break
            if not all_exist:
                break
        if all_exist:
            ok += n
        else:
            pending_scs.append((scx, scy, n))

    skipped = ok
    if skipped > 0:
        print(
            f"PROGRESS chunk={args.chunk} ok={ok} fail={fail} "
            f"total={my_total_tiles} skipped={skipped} rate=0.0/s",
            file=sys.stderr, flush=True,
        )

    sc_lnf = 0  # supercells with LayerNotDefined
    sc_done = 0  # supercells processed

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {}
        for scx, scy, n in pending_scs:
            f = executor.submit(
                download_supercell, args.comuna, args.nombre,
                scx, scy, args.zoom, args.out_dir,
                sx, mx, sy, my,
            )
            futures[f] = (scx, scy, n)

        for f in as_completed(futures):
            scx, scy, n = futures[f]
            sc_done += 1
            try:
                n_ok, n_fail, lnf = f.result()
                ok += n_ok
                fail += n_fail
                if lnf:
                    sc_lnf += 1
            except Exception:
                fail += n

            # Early exit: if first 3 supercells are all LayerNotDefined, abort
            if sc_lnf >= 3 and sc_done == sc_lnf and ok == skipped:
                print(
                    f"LAYER_NOT_FOUND chunk={args.chunk} name={args.nombre}",
                    file=sys.stderr, flush=True,
                )
                executor.shutdown(wait=False, cancel_futures=True)
                sys.exit(10)

            now = time.time()
            if now - last_report >= args.progress_every:
                elapsed = now - t0
                rate = (ok + fail - skipped) / elapsed if elapsed > 0 else 0
                print(
                    f"PROGRESS chunk={args.chunk} ok={ok} fail={fail} "
                    f"total={my_total_tiles} rate={rate:.1f}/s",
                    file=sys.stderr,
                    flush=True,
                )
                last_report = now

    elapsed = time.time() - t0
    rate = (ok + fail - skipped) / elapsed if elapsed > 0 else 0
    print(
        f"DONE chunk={args.chunk} ok={ok} fail={fail} "
        f"total={my_total_tiles} rate={rate:.1f}/s elapsed={elapsed:.0f}s",
        file=sys.stderr,
        flush=True,
    )


if __name__ == "__main__":
    main()
