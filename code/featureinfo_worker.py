#!/usr/bin/env python3
"""
Worker para consultar getFeatureInfo del SII via túnel Mullvad.

Lee polígonos de una cola compartida (flock), consulta getFeatureInfo
por centroide, escribe resultados a directorio de output.

Uso (desde batch):
    ip netns exec vpn0 python3 featureinfo_worker.py \
        --tunnel 0 --layer sii:BR_CART_SANTIAGO_OESTE_WMS \
        --queue /tmp/fi_queue.txt --outdir /tmp/fi_results/
"""

import argparse
import fcntl
import json
import os
import random
import subprocess
import sys
import time

import requests

FEATURE_INFO_URL = "https://www4.sii.cl/mapasui/services/data/mapasFacadeService/getFeatureInfo"
RELAY_JSON = "/tmp/mullvad_relays.json"
SPARE_RELAYS = [
    "gb-man", "de-ber", "nl-ams", "fr-par", "se-sto",
    "jp-tyo", "au-syd", "ie-dub", "es-mad", "it-rom",
    "at-vie", "ch-zur", "no-osl", "dk-cph", "fi-hel",
    "pl-war", "ro-buc", "bg-sof", "hr-zag", "cz-prg",
    "hu-bud", "ca-van", "nz-akl", "us-phx", "us-atl",
    "us-slc", "us-den", "us-bos",
]
_relay_idx = 0


def rotate_tunnel(tunnel_id):
    """Rota el túnel WireGuard a un relay Mullvad fresco."""
    global _relay_idx
    if not os.path.exists(RELAY_JSON):
        return False

    relay_name = SPARE_RELAYS[_relay_idx % len(SPARE_RELAYS)]
    _relay_idx += 1

    try:
        with open(RELAY_JSON) as f:
            relays = json.load(f)["wireguard"]["relays"]
        new_ep = new_pk = None
        for r in relays:
            if r["hostname"].startswith(relay_name):
                new_ep = r["ipv4_addr_in"]
                new_pk = r["public_key"]
                break
        if not new_ep:
            return False

        ns = f"vpn{tunnel_id}"
        wg = f"wg{tunnel_id}"
        result = subprocess.run(
            ["ip", "netns", "exec", ns, "wg", "show", wg, "peers"],
            capture_output=True, text=True,
        )
        old_pk = result.stdout.strip().split("\n")[0] if result.stdout.strip() else None
        if old_pk:
            subprocess.run(
                ["ip", "netns", "exec", ns, "wg", "set", wg, "peer", old_pk, "remove"],
                capture_output=True,
            )
        subprocess.run(
            ["ip", "netns", "exec", ns, "wg", "set", wg, "peer", new_pk,
             "endpoint", f"{new_ep}:51820", "allowed-ips", "0.0.0.0/0",
             "persistent-keepalive", "25"],
            capture_output=True,
        )
        time.sleep(3)
        print(f"T{tunnel_id}: ROTATED -> {relay_name}", file=sys.stderr, flush=True)
        return True
    except Exception as e:
        print(f"T{tunnel_id}: rotate failed: {e}", file=sys.stderr, flush=True)
        return False


def create_session():
    """Crea sesion HTTP con cookie del SII."""
    s = requests.Session()
    s.headers.update({
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": "https://www4.sii.cl",
        "Referer": "https://www4.sii.cl/mapasui/internet/",
        "User-Agent": f"Mozilla/5.0 (fi-worker-{os.getpid()})",
    })
    try:
        s.get("https://www4.sii.cl/mapasui/internet/", timeout=15)
    except Exception:
        pass
    return s


def query_feature_info(session, layer, lat, lon):
    """Consulta getFeatureInfo. Retorna dict, 'RATE_LIMITED', 'BLOCKED', o None."""
    delta_lat = 0.0003
    delta_lng = 0.0004

    payload = {
        "metaData": {
            "namespace": "cl.sii.sdi.lob.bbrr.mapas.data.api.interfaces.MapasFacadeService/getFeatureInfo",
            "conversationId": "UNAUTHENTICATED-CALL",
            "transactionId": f"tx-{int(time.time()*1000)}-{random.randint(1000,9999)}",
        },
        "data": {
            "clickInfo": {
                "x": 128, "y": 128,
                "southwestx": lat - delta_lat,
                "southwesty": lon - delta_lng,
                "northeastx": lat + delta_lat,
                "northeasty": lon + delta_lng,
                "layer": layer,
                "width": 256, "height": 256,
                "servicios": [],
            }
        }
    }

    try:
        r = session.post(FEATURE_INFO_URL, json=payload, timeout=15)
        if r.status_code == 200:
            j = r.json()
            if j and j.get("data"):
                d = j["data"]
                if d.get("existePredio") and d["existePredio"] != -1:
                    return {
                        "manzana": d.get("manzana"),
                        "predio": d.get("predio"),
                        "rol": (d.get("rol") or "").strip(),
                        "direccion": (d.get("direccion") or "").strip(),
                        "destino": (d.get("destinoDescripcion") or "").strip(),
                        "ah": (d.get("ah") or "").strip(),
                        "lat": d.get("ubicacionX"),
                        "lon": d.get("ubicacionY"),
                        "avaluo_total": d.get("valorTotal"),
                        "avaluo_afecto": d.get("valorAfecto"),
                        "avaluo_exento": d.get("valorExento"),
                        "sup_terreno": d.get("supTerreno"),
                        "sup_construida": d.get("supConsMt2"),
                    }
            return None
        elif r.status_code == 429:
            return "RATE_LIMITED"
        elif r.status_code in (403, 503):
            return "BLOCKED"
    except requests.exceptions.Timeout:
        return None
    except Exception:
        pass
    return None


def grab_next_item(queue_path):
    """Atomically grab next item from shared queue."""
    try:
        with open(queue_path, "r+") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            lines = f.readlines()
            if not lines:
                fcntl.flock(f, fcntl.LOCK_UN)
                return None
            item = lines[0].strip()
            f.seek(0)
            f.writelines(lines[1:])
            f.truncate()
            fcntl.flock(f, fcntl.LOCK_UN)
            return item
    except (IOError, ValueError):
        return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tunnel", type=int, required=True)
    parser.add_argument("--layer", required=True, help="WMS layer name")
    parser.add_argument("--queue", required=True, help="Shared queue file")
    parser.add_argument("--outdir", required=True, help="Output directory")
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    session = create_session()
    t_id = args.tunnel
    processed = 0
    found = 0
    blocked_count = 0
    max_rotations = 3
    rotations = 0

    print(f"T{t_id}: started", file=sys.stderr, flush=True)

    while True:
        item = grab_next_item(args.queue)
        if item is None:
            break

        # Parse: poly_idx,lat,lon
        parts = item.split(",")
        if len(parts) != 3:
            continue
        poly_idx, lat, lon = parts[0], float(parts[1]), float(parts[2])

        result = query_feature_info(session, args.layer, lat, lon)

        if result == "RATE_LIMITED":
            time.sleep(3 + random.random() * 2)
            blocked_count += 1
            if blocked_count > 5:
                if rotations < max_rotations and rotate_tunnel(t_id):
                    rotations += 1
                    blocked_count = 0
                    session = create_session()
                else:
                    print(f"T{t_id}: BURNED after {rotations} rotations", file=sys.stderr, flush=True)
                    break
            continue
        elif result == "BLOCKED":
            if rotations < max_rotations and rotate_tunnel(t_id):
                rotations += 1
                blocked_count = 0
                session = create_session()
                continue
            else:
                print(f"T{t_id}: BLOCKED, stopping", file=sys.stderr, flush=True)
                break
        elif result is not None and isinstance(result, dict):
            out_file = os.path.join(args.outdir, f"p_{poly_idx}.json")
            with open(out_file, "w") as f:
                json.dump(result, f)
            found += 1
            blocked_count = 0

        processed += 1
        time.sleep(0.3 + random.random() * 0.2)

        if processed % 100 == 0:
            print(f"T{t_id}: processed={processed} found={found}",
                  file=sys.stderr, flush=True)

    print(f"T{t_id}: done, processed={processed} found={found} rotations={rotations}",
          file=sys.stderr, flush=True)


if __name__ == "__main__":
    main()
