#!/usr/bin/env python3
"""
sc_worker.py — Worker de descarga de supercells WMS dentro de ip netns exec vpnN.

Mismo patrón que fase0_worker.py: cola compartida con flock + work stealing.
Cada item de la cola es "sc_x|sc_y|bbox_str".

Usage:
    ip netns exec vpn0 python3 -u sc_worker.py \
        --tunnel 0 --queue /tmp/fase0_v2/7301/sc_queue.txt \
        --outdir /tmp/fase0_v2/7301/tiles_z19 \
        --comuna 7301 --wms-name LINARES --counter /tmp/fase0_v2/7301/sc_0.count
"""

import argparse
import fcntl
import io
import os
import sys
import time

import requests
from PIL import Image

WMS_URL = "https://www4.sii.cl/mapasui/services/ui/wmsProxyService/call"


def grab_items(queue_path, batch_size=20):
    """Toma items del final de la cola (LIFO). Todos los workers compiten."""
    items = []
    try:
        with open(queue_path, "r+") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            lines = f.readlines()
            to_take = min(batch_size, len(lines))
            if to_take > 0:
                items = [l.strip() for l in lines[-to_take:]]
                f.seek(0)
                f.writelines(lines[:-to_take])
                f.truncate()
            fcntl.flock(f, fcntl.LOCK_UN)
    except FileNotFoundError:
        pass
    return items


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tunnel", type=int, required=True)
    parser.add_argument("--queue", required=True)
    parser.add_argument("--outdir", required=True)
    parser.add_argument("--comuna", type=int, required=True)
    parser.add_argument("--wms-name", required=True)
    parser.add_argument("--counter", default="")
    args = parser.parse_args()

    t = args.tunnel
    log = lambda m: print(
        f"[{time.strftime('%H:%M:%S')}] T{t:02d} {m}", file=sys.stderr, flush=True
    )
    log("started")

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    ok_count = 0
    errors = 0
    consecutive_fail = 0

    while True:
        items = grab_items(args.queue, batch_size=5)
        if not items:
            break

        for item in items:
            parts = item.split("|")
            if len(parts) != 3:
                continue
            sc_x, sc_y, bbox_str = int(parts[0]), int(parts[1]), parts[2]

            tile_file = os.path.join(args.outdir, f"sc_{sc_x}_{sc_y}.png")
            if os.path.exists(tile_file) and os.path.getsize(tile_file) > 100:
                ok_count += 1
                continue

            params = {
                "service": "WMS", "request": "GetMap",
                "layers": f"sii:BR_CART_{args.wms_name}_WMS",
                "styles": "PREDIOS_WMS_V0", "format": "image/png",
                "transparent": "true", "version": "1.1.1",
                "comuna": args.comuna, "eac": "0", "eacano": "0",
                "height": "1024", "width": "1024",
                "srs": "EPSG:3857", "bbox": bbox_str,
            }

            success = False
            for attempt in range(3):
                try:
                    resp = session.get(WMS_URL, params=params, timeout=30)
                    if resp.status_code == 200 and len(resp.content) > 100:
                        with open(tile_file, "wb") as f:
                            f.write(resp.content)
                        ok_count += 1
                        consecutive_fail = 0
                        success = True
                        break
                    elif resp.status_code == 200:
                        break  # empty tile, don't retry
                except Exception:
                    if attempt < 2:
                        time.sleep(2)

            if not success:
                errors += 1
                consecutive_fail += 1

            # Renovar sesión si muchos fails seguidos
            if consecutive_fail >= 10:
                session = requests.Session()
                session.headers.update({"User-Agent": "Mozilla/5.0"})
                consecutive_fail = 0
                log("session renewed")

        # Update counter
        if args.counter:
            with open(args.counter, "w") as f:
                f.write(str(ok_count))

    # Final counter
    if args.counter:
        with open(args.counter, "w") as f:
            f.write(str(ok_count))

    log(f"DONE ok={ok_count} err={errors}")


if __name__ == "__main__":
    main()
