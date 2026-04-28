#!/usr/bin/env python3
"""
fase0_worker.py — Worker que corre dentro de ip netns exec vpnN.

Procesa predios de una cola compartida (flock + work stealing):
- Toma lotes de 20 items de la cola (atómico via flock)
- Consulta getPredioNacional con servicios correctos
- Normaliza respuesta incluyendo predioPublicado completo
- Escribe JSON por predio + counter por worker
- Renueva sesión tras 20 nulls consecutivos
- Reporta progreso a stderr

Usage:
    ip netns exec vpn0 python3 -u fase0_worker.py \
        --tunnel 0 --queue /tmp/fase0/15105/queue.txt \
        --outdir /tmp/fase0/15105/data --comuna 15105
"""

import argparse
import fcntl
import json
import os
import sys
import time

# Worker corre desde pipeline_clean/, importa módulos locales
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fase0_normalize import make_session, fetch_predio
from fase0_config import SESSION_RENEW_AFTER_NULLS


# ─── Cola compartida con flock (work stealing built-in) ──────────────────────

def grab_items(queue_path: str, batch_size: int = 20) -> list[str]:
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
    parser.add_argument("--wms-name", default="")
    parser.add_argument("--counter", default="")
    args = parser.parse_args()

    t = args.tunnel
    log = lambda m: print(
        f"[{time.strftime('%H:%M:%S')}] T{t:02d} {m}", file=sys.stderr, flush=True
    )
    log("started")

    session = make_session()
    ok_count = 0
    found_coords = 0
    found_pp = 0
    errors = 0
    consecutive_null = 0

    while True:
        items = grab_items(args.queue, batch_size=20)
        if not items:
            break

        for item in items:
            parts = item.split("|")
            if len(parts) != 2:
                continue
            mz, pr = parts[0].zfill(5), parts[1].zfill(5)

            out_file = os.path.join(args.outdir, f"{mz}_{pr}.json")
            if os.path.exists(out_file):
                ok_count += 1
                continue

            result = fetch_predio(session, args.comuna, mz, pr, args.wms_name)

            with open(out_file, "w") as f:
                json.dump(result, f, ensure_ascii=False, default=str)
            ok_count += 1

            # Track stats
            if result.get("lat") is not None:
                found_coords += 1
                consecutive_null = 0
            elif result.get("predioPublicado_predio") is not None:
                found_pp += 1
                consecutive_null = 0
            elif result.get("_ok"):
                consecutive_null += 1
            else:
                errors += 1
                consecutive_null += 1

            # Renovar sesión si muchos nulls seguidos (IP podría estar bloqueada)
            if consecutive_null >= SESSION_RENEW_AFTER_NULLS:
                session = make_session()
                consecutive_null = 0
                log("session renewed")

        # Update counter file (one per worker, no contention)
        if args.counter:
            with open(args.counter, "w") as f:
                f.write(str(ok_count))

        log(f"ok={ok_count} coords={found_coords} pp={found_pp} err={errors}")

    # Final counter update
    if args.counter:
        with open(args.counter, "w") as f:
            f.write(str(ok_count))

    log(f"DONE ok={ok_count} coords={found_coords} pp={found_pp} err={errors}")


if __name__ == "__main__":
    main()
