#!/usr/bin/env python3
"""
fase0_orchestrator.py — Orquestador Phase 0 v2.

Descarga datos tabulares de TODOS los predios de Chile usando 40 túneles VPN.
Usa la UNION de roles_split + catastro semestral como fuente.

Features:
  - 40 túneles Mullvad en paralelo (vpn0-vpn39)
  - Cola compartida con flock (work stealing: si un túnel termina, ayuda al resto)
  - Rotación automática de IP si un worker se estanca >60s
  - Renovación de sesión tras 20 nulls consecutivos
  - Resume: skip predios ya descargados (JSONs en disco)
  - Skip comunas ya en S3 (idempotente)
  - Herencia por dirección en merge (fase 9 integrada)
  - predioPublicado completo (rol_base 9xxx + UTM)

Usage:
    cd /root/carto_predios/sii_vectorizer/pipeline_clean
    python3 -u fase0_orchestrator.py --comuna 15105          # una comuna
    python3 -u fase0_orchestrator.py --all                   # todas
    python3 -u fase0_orchestrator.py --queue /tmp/queue.txt  # desde queue
"""

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import boto3
import pandas as pd

from fase0_config import (
    NUM_TUNNELS, VENV_PYTHON, BASE_DIR,
    S3_ENDPOINT, S3_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY, S3_OUTPUT_PREFIX,
    ROLES_SPLIT_DIR, CATASTRO_CSV, CATASTRO_S3_KEY,
    STALL_THRESHOLD_S, MULLVAD_RELAYS_PATH,
    MAX_ROTATIONS_PER_WORKER, SPARE_RELAYS,
)

WORKER_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fase0_worker.py")
MERGE_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fase0_merge.py")


def s3_client():
    return boto3.client("s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY)


def s3_exists(client, key: str) -> bool:
    try:
        client.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except Exception:
        return False


# ─── Fuentes de roles ────────────────────────────────────────────────────────

def parse_roles_split(cod: int) -> set[tuple[str, str]]:
    """Parsea roles_split/{cod}.txt → set de (manzana, predio)."""
    path = os.path.join(ROLES_SPLIT_DIR, f"{cod}.txt")
    roles = set()
    if not os.path.exists(path):
        return roles
    with open(path) as f:
        for line in f:
            if len(line) < 67:
                continue
            m = line[57:62].strip()
            p = line[62:67].strip()
            if m and p:
                try:
                    roles.add((str(int(m)).zfill(5), str(int(p)).zfill(5)))
                except ValueError:
                    pass
    return roles


def parse_catastro(cod: int) -> set[tuple[str, str]]:
    """Parsea catastro CSV filtrando por comuna → set de (manzana, predio)."""
    roles = set()
    if not os.path.exists(CATASTRO_CSV):
        return roles
    # Read in chunks to handle large file
    for chunk in pd.read_csv(CATASTRO_CSV, dtype=str, chunksize=100_000,
                             usecols=["comuna", "manzana", "predio"]):
        mask = chunk["comuna"].astype(int) == cod
        for _, r in chunk[mask].iterrows():
            m = str(int(r["manzana"])).zfill(5)
            p = str(int(r["predio"])).zfill(5)
            roles.add((m, p))
    return roles


def build_union(cod: int) -> list[tuple[str, str]]:
    """UNION de roles_split + catastro, dedup, ordenado."""
    r1 = parse_roles_split(cod)
    r2 = parse_catastro(cod)
    union = sorted(r1 | r2)
    return union


# ─── Rotación de IP ──────────────────────────────────────────────────────────

_spare_idx = 0

def rotate_tunnel(tunnel_id: int) -> bool:
    """Rota un túnel a un relay Mullvad diferente."""
    global _spare_idx
    if not os.path.exists(MULLVAD_RELAYS_PATH):
        return False

    relay_name = SPARE_RELAYS[_spare_idx % len(SPARE_RELAYS)]
    _spare_idx += 1

    try:
        relays = json.load(open(MULLVAD_RELAYS_PATH))
        target = None
        for r in relays["wireguard"]["relays"]:
            if r["hostname"].startswith(relay_name):
                target = r
                break
        if not target:
            return False

        ns = f"vpn{tunnel_id}"
        wg = f"wg{tunnel_id}"
        new_ep = target["ipv4_addr_in"]
        new_pk = target["public_key"]

        # Remove old peer, add new
        old_pk = subprocess.run(
            f"ip netns exec {ns} wg show {wg} peers".split(),
            capture_output=True, text=True
        ).stdout.strip().split("\n")[0]

        subprocess.run(
            f"ip netns exec {ns} wg set {wg} peer {old_pk} remove".split(),
            capture_output=True
        )
        subprocess.run(
            f"ip netns exec {ns} wg set {wg} peer {new_pk} "
            f"endpoint {new_ep}:51820 allowed-ips 0.0.0.0/0 "
            f"persistent-keepalive 25".split(),
            capture_output=True
        )
        time.sleep(2)
        print(f"  [ROTATE] T{tunnel_id} → {relay_name}", flush=True)
        return True
    except Exception as e:
        print(f"  [ROTATE FAIL] T{tunnel_id}: {e}", flush=True)
        return False


# ─── WMS names ───────────────────────────────────────────────────────────────

_wms_names = None

def get_wms_name(cod: int) -> str:
    global _wms_names
    if _wms_names is None:
        from fase0_config import load_wms_names
        _wms_names = load_wms_names()
    return _wms_names.get(cod, "")


# ─── Procesar una comuna ─────────────────────────────────────────────────────

def process_comuna(cod: int, workdir: str, n_tunnels: int,
                   client, skip_s3_check: bool = False, download_only: bool = False):
    """Descarga todos los predios de una comuna."""
    s3_key = f"{S3_OUTPUT_PREFIX}/comuna={cod}.csv"
    if not skip_s3_check and s3_exists(client, s3_key):
        print(f"[SKIP] {cod} (ya en S3)", flush=True)
        return

    wdir = Path(workdir) / str(cod)
    data_dir = wdir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    wms_name = get_wms_name(cod)

    # ── 1. Build UNION ───────────────────────────────────────────────────────
    print(f"\n{'='*60}", flush=True)
    print(f"[{cod}] Construyendo UNION de roles...", flush=True)
    t0_comuna = time.time()

    roles = build_union(cod)
    if not roles:
        print(f"  Sin roles para comuna {cod}. Saltando.", flush=True)
        return
    print(f"  UNION: {len(roles):,} predios", flush=True)

    # ── 2. Build queue (skip already fetched) ────────────────────────────────
    existing = set(f.replace(".json", "") for f in os.listdir(data_dir)
                   if f.endswith(".json"))

    queue_path = str(wdir / "queue.txt")
    pending = 0
    with open(queue_path, "w") as f:
        for mz, pr in roles:
            if f"{mz}_{pr}" in existing:
                continue
            f.write(f"{mz}|{pr}\n")
            pending += 1

    total = len(roles)
    already = len(existing)
    print(f"  Queue: {pending:,} pendientes (ya: {already:,})", flush=True)

    if pending == 0:
        print(f"  Todos descargados. Merge directo.", flush=True)
    else:
        # ── 3. Launch workers ────────────────────────────────────────────────
        n_use = min(n_tunnels, pending)
        print(f"  Lanzando {n_use} workers...", flush=True)

        procs = []
        counter_files = []
        for i in range(n_use):
            counter = str(wdir / f"worker_{i}.count")
            with open(counter, "w") as f:
                f.write("0")
            counter_files.append(counter)

            cmd = [
                "ip", "netns", "exec", f"vpn{i}",
                VENV_PYTHON, "-u", WORKER_SCRIPT,
                "--tunnel", str(i),
                "--queue", queue_path,
                "--outdir", str(data_dir),
                "--comuna", str(cod),
                "--wms-name", wms_name or "",
                "--counter", counter,
            ]
            proc = subprocess.Popen(
                cmd,
                cwd=os.path.dirname(WORKER_SCRIPT),
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            procs.append(proc)

        # ── 4. Monitor with stall detection + IP rotation ────────────────────
        t0 = time.time()
        last_progress = {i: (0, time.time()) for i in range(n_use)}
        rotations = {i: 0 for i in range(n_use)}

        # Timeout: si >95% completado y quedan <10 workers por >120s, matarlos
        TAIL_TIMEOUT_S = 120
        tail_start = None

        while True:
            alive = sum(1 for p in procs if p.poll() is None)
            if alive == 0:
                break

            # Count actual files (no contention)
            done = sum(1 for _ in data_dir.iterdir() if _.suffix == ".json")
            elapsed = time.time() - t0
            rate = done / elapsed if elapsed > 0 else 0
            eta = (total - done) / rate / 60 if rate > 0 else 0

            print(
                f"\r  [{cod}] {done:,}/{total:,} ({done/total*100:.1f}%) "
                f"| {alive} workers | {rate:.1f}/s | ETA {eta:.1f}min",
                end="", flush=True
            )

            # Tail timeout: si >99% done y pocos workers, matar después de 120s
            if done / total > 0.99 and alive <= 10:
                if tail_start is None:
                    tail_start = time.time()
                elif time.time() - tail_start > TAIL_TIMEOUT_S:
                    print(f"\n  [TAIL TIMEOUT] Matando {alive} workers lentos "
                          f"({total - done} predios → retry)", flush=True)
                    for p in procs:
                        if p.poll() is None:
                            p.kill()
                    break
            else:
                tail_start = None

            # Stall detection per worker
            for i in range(n_use):
                if procs[i].poll() is not None:
                    continue
                try:
                    cur = int(open(counter_files[i]).read().strip() or "0")
                except (FileNotFoundError, ValueError):
                    cur = 0

                prev_count, prev_time = last_progress[i]
                if cur > prev_count:
                    last_progress[i] = (cur, time.time())
                elif time.time() - prev_time > STALL_THRESHOLD_S:
                    if rotations[i] < MAX_ROTATIONS_PER_WORKER:
                        print(f"\n  [STALL] T{i} (stuck at {cur})", flush=True)
                        rotate_tunnel(i)
                        rotations[i] += 1
                        last_progress[i] = (cur, time.time())

            time.sleep(5)

        print(flush=True)
        elapsed = time.time() - t0
        done = sum(1 for _ in data_dir.iterdir() if _.suffix == ".json")
        print(f"  Workers done: {done:,} en {elapsed/60:.1f}min", flush=True)

        # Print worker errors
        total_rotations = sum(rotations.values())
        if total_rotations > 0:
            print(f"  Rotaciones IP: {total_rotations}", flush=True)

        # ── 4b. Retry automático de errores ──────────────────────────────────
        # Identificar JSONs con _error=max_retries, borrarlos y re-encolar
        import json as _json
        failed_keys = []
        for f in data_dir.iterdir():
            if f.suffix != ".json":
                continue
            try:
                with open(f) as fh:
                    d = _json.load(fh)
                if d.get("_error") == "max_retries":
                    failed_keys.append(f.stem)
                    f.unlink()
            except Exception:
                pass

        if failed_keys:
            print(f"  [RETRY] {len(failed_keys):,} predios fallaron por timeout. Re-intentando...", flush=True)
            retry_queue = str(wdir / "queue_retry.txt")
            with open(retry_queue, "w") as f:
                for key in failed_keys:
                    mz, pr = key.split("_")
                    f.write(f"{mz}|{pr}\n")

            # Lanzar workers de retry (mismos túneles)
            retry_procs = []
            for i in range(n_use):
                cmd = [
                    "ip", "netns", "exec", f"vpn{i}",
                    VENV_PYTHON, "-u", WORKER_SCRIPT,
                    "--tunnel", str(i),
                    "--queue", retry_queue,
                    "--outdir", str(data_dir),
                    "--comuna", str(cod),
                    "--wms-name", wms_name or "",
                    "--counter", str(wdir / f"retry_{i}.count"),
                ]
                proc = subprocess.Popen(
                    cmd, cwd=os.path.dirname(WORKER_SCRIPT),
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                )
                retry_procs.append(proc)

            # Esperar retry
            for p in retry_procs:
                p.wait()

            # Contar recuperados
            recovered = sum(1 for k in failed_keys
                            if (data_dir / f"{k}.json").exists())
            still_failed = len(failed_keys) - recovered
            done = sum(1 for _ in data_dir.iterdir() if _.suffix == ".json")
            print(f"  [RETRY] Recuperados: {recovered:,}, aún fallidos: {still_failed:,}", flush=True)
            print(f"  Total JSONs: {done:,}", flush=True)

    # ── 5. Métricas Phase 0 ─────────────────────────────────────────────────
    print(f"  [PASO 5] Calculando métricas...", flush=True)
    import json as _json2
    done_final = sum(1 for _ in data_dir.iterdir() if _.suffix == ".json")
    n_errors = 0
    n_coords = 0
    n_pp = 0
    for f in data_dir.iterdir():
        if f.suffix != ".json":
            continue
        try:
            with open(f) as fh:
                d = _json2.load(fh)
            if d.get("_error"):
                n_errors += 1
            if d.get("lat") is not None and d.get("lat") != "None":
                n_coords += 1
            if d.get("predioPublicado_predio") is not None and d.get("predioPublicado_predio") != "None":
                n_pp += 1
        except Exception:
            pass

    metrics_f0 = {
        "total_predios": total,
        "descargados": done_final,
        "con_coords": n_coords,
        "sin_coords": done_final - n_coords,
        "con_predio_publicado": n_pp,
        "errores_red": n_errors,
        "workers": n_use if "n_use" in dir() else 0,
        "tiempo_s": round(time.time() - t0_comuna),
    }
    metrics_path = str(wdir / "metrics.json")
    with open(metrics_path, "w") as f:
        _json2.dump({"comuna": cod, "fase0": metrics_f0}, f, indent=2)
    print(f"  Métricas: {metrics_path}", flush=True)

    # ── 6. Merge JSONs → CSV ─────────────────────────────────────────────────
    print(f"  [PASO 6] Merge JSONs → CSV (catastro + herencia)...", flush=True)
    out_csv = str(wdir / f"comuna={cod}.csv")
    merge_cmd = (
        f"{VENV_PYTHON} -u {MERGE_SCRIPT} "
        f"--comuna {cod} --datadir {str(data_dir)} --output {out_csv}"
    )
    subprocess.run(merge_cmd, shell=True,
                   cwd=os.path.dirname(MERGE_SCRIPT))

    if not os.path.exists(out_csv):
        print(f"  ✗ CSV no generado para {cod}", flush=True)
        return

    if download_only:
        print(f"  [download-only] Descarga completada. Saliendo.", flush=True)
        return

    # ── 7. Descargar vectores urbanos + TIF ────────────────────────────────
    print(f"  [PASO 7] Descargando vectores urbanos + TIF...", flush=True)
    vec_key = f"2025ss_bcn/vectors/comuna={cod}.gpkg"
    vec_local = str(wdir / f"vectors_{cod}.gpkg")
    out_gpkg = str(wdir / f"comuna={cod}.parquet")

    try:
        if not os.path.exists(vec_local):
            client.download_file(S3_BUCKET, vec_key, vec_local)
    except Exception as e:
        print(f"  ⚠ Vectores urbanos no disponibles: {e}", flush=True)

    tif_local = str(wdir / f"tif_{cod}.tif")
    tif_key = f"2025ss_bcn/TIFs/comuna={cod}.tif"
    try:
        if not os.path.exists(tif_local):
            client.download_file(S3_BUCKET, tif_key, tif_local)
    except Exception:
        tif_local = ""

    # ── 8a. Vectorización: selective_tif (descarga tiles z19 + vectoriza por bloques) ─
    selective_tif = os.path.join(os.path.dirname(WORKER_SCRIPT), "fase0_selective_tif.py")
    tif_vectors = str(wdir / f"vectors_tif_{cod}.gpkg")
    if os.path.exists(selective_tif) and not os.path.exists(tif_vectors):
        print(f"  [PASO 8a] Descarga tiles z19 + vectorización por bloques...", flush=True)
        csv_path = str(wdir / f"comuna={cod}.csv")
        vec_cmd = [
            VENV_PYTHON, "-u", selective_tif,
            "--comuna", str(cod),
            "--wms-name", wms_name or "",
            "--csv", csv_path,
            "--outdir", str(wdir),
            "--tunnels", str(n_tunnels),
            "--output-vectors", tif_vectors,
        ]
        subprocess.run(vec_cmd, cwd=os.path.dirname(WORKER_SCRIPT))

    # ── 8a2. Merge: urbanos (S3) + agrícolas (TIF), dedup por overlap ─────
    # Urbanos tienen prioridad. Agrícolas del TIF solo se agregan si no
    # solapan >50% con un polígono urbano existente.
    import geopandas as _gpd
    from shapely.strtree import STRtree as _STRtree
    all_vectors_path = str(wdir / f"vectors_all_{cod}.gpkg")

    urban_vecs = None
    tif_vecs = None
    if os.path.exists(vec_local):
        urban_vecs = _gpd.read_file(vec_local, engine="pyogrio")
    if os.path.exists(tif_vectors):
        tif_vecs = _gpd.read_file(tif_vectors, engine="pyogrio")

    if urban_vecs is not None and tif_vecs is not None:
        # Normalize CRS
        target_crs = urban_vecs.crs
        if tif_vecs.crs and tif_vecs.crs != target_crs:
            tif_vecs = tif_vecs.to_crs(target_crs)

        # Filter TIF polygons that overlap >50% with urban
        urban_tree = _STRtree(urban_vecs.geometry.values)
        keep = []
        for idx, row in tif_vecs.iterrows():
            g = row.geometry
            dominated = False
            for ui in urban_tree.query(g):
                inter = urban_vecs.geometry.iloc[ui].intersection(g).area
                if inter / max(g.area, 1e-12) > 0.50:
                    dominated = True
                    break
            if not dominated:
                keep.append(idx)

        tif_new = tif_vecs.loc[keep]
        all_vecs = _gpd.pd.concat([urban_vecs, tif_new], ignore_index=True)
        all_vecs = _gpd.GeoDataFrame(all_vecs, geometry="geometry", crs=target_crs)
        all_vecs.to_file(all_vectors_path, engine="pyogrio")
        print(f"  [PASO 8a2] Merge: {len(urban_vecs)} urbanos + "
              f"{len(tif_new)} agrícolas (de {len(tif_vecs)} TIF, "
              f"{len(tif_vecs)-len(tif_new)} duplicados) = {len(all_vecs)}",
              flush=True)
        match_vectors = all_vectors_path
    elif urban_vecs is not None:
        match_vectors = vec_local
    elif tif_vecs is not None:
        match_vectors = tif_vectors
    else:
        match_vectors = ""

    # ── 8b. Match coords → polígonos combinados (PIP + nearest ≤10m) ──────
    if match_vectors and os.path.exists(match_vectors):
        print(f"  [PASO 8b] Match (PIP + nearest ≤10m) contra vectores combinados...", flush=True)
        match_script = os.path.join(os.path.dirname(WORKER_SCRIPT), "fase0_match.py")
        match_cmd = [
            VENV_PYTHON, "-u", match_script,
            "--comuna", str(cod),
            "--csv", out_csv,
            "--vectors", match_vectors,
            "--output", out_gpkg,
            "--metrics", metrics_path,
        ]
        subprocess.run(match_cmd, cwd=os.path.dirname(WORKER_SCRIPT))
    else:
        print(f"  ⚠ Sin vectores disponibles para match", flush=True)

    # ── 8c. Recovery (post-match) ────────────────────────────────────────────
    recovery_script = os.path.join(os.path.dirname(WORKER_SCRIPT), "fase0_recovery.py")
    if os.path.exists(out_gpkg) and os.path.exists(recovery_script):
        print(f"  [PASO 8c] Recovery (AH/CSA UTM + nearest unlimited + OCR + addr inherit)...", flush=True)
        recovery_cmd = [
            VENV_PYTHON, "-u", recovery_script,
            "--comuna", str(cod),
            "--gpkg", out_gpkg,
            "--vectors", match_vectors or vec_local,
            "--tif", tif_local,
            "--csv", out_csv,
            "--metrics", metrics_path,
        ]
        subprocess.run(recovery_cmd, cwd=os.path.dirname(WORKER_SCRIPT))

    # ── 9. Copiar TIF a F0/ ──────────────────────────────────────────────────
    print(f"  [PASO 9] Copiar TIF + subir a S3...", flush=True)
    tif_src_key = f"2025ss_bcn/TIFs/comuna={cod}.tif"
    tif_dst_key = f"{S3_OUTPUT_PREFIX}/comuna={cod}.tif"
    try:
        client.head_object(Bucket=S3_BUCKET, Key=tif_src_key)
        print(f"    Copiando TIF → F0/...", flush=True)
        client.copy_object(
            Bucket=S3_BUCKET,
            CopySource={"Bucket": S3_BUCKET, "Key": tif_src_key},
            Key=tif_dst_key,
        )
    except Exception:
        print(f"  ⚠ TIF no encontrado en {tif_src_key}", flush=True)

    # ── 9. Upload todo a S3 ──────────────────────────────────────────────────
    print(f"  Subiendo a S3 ({S3_OUTPUT_PREFIX})...", flush=True)

    # CSV (pipe-separated)
    csv_key = f"{S3_OUTPUT_PREFIX}/comuna={cod}.csv"
    csv_mb = os.path.getsize(out_csv) / 1024 / 1024
    print(f"    ↑ {csv_key} ({csv_mb:.0f} MB)", flush=True)
    client.upload_file(out_csv, S3_BUCKET, csv_key)

    # GeoParquet
    if os.path.exists(out_gpkg):
        parquet_key = f"{S3_OUTPUT_PREFIX}/comuna={cod}.parquet"
        parquet_mb = os.path.getsize(out_gpkg) / 1024 / 1024
        print(f"    ↑ {parquet_key} ({parquet_mb:.0f} MB)", flush=True)
        client.upload_file(out_gpkg, S3_BUCKET, parquet_key)

    # Metrics JSON
    if os.path.exists(metrics_path):
        metrics_key = f"{S3_OUTPUT_PREFIX}/comuna={cod}_metrics.json"
        print(f"    ↑ {metrics_key}", flush=True)
        client.upload_file(metrics_path, S3_BUCKET, metrics_key)

    # ── 10. Limpieza workdir ────────────────────────────────────────────────
    print(f"  [PASO 10] Limpieza workdir...", flush=True)
    import shutil
    try:
        pass  # shutil.rmtree(str(wdir))  # disabled for debug
        print(f"    Borrado {wdir}", flush=True)
    except Exception as e:
        print(f"    ⚠ Limpieza falló: {e}", flush=True)

    elapsed_total = time.time() - t0_comuna
    print(f"  ✓ {cod} completada en {elapsed_total/60:.1f}min", flush=True)


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--comuna", type=int, help="Procesar una sola comuna")
    group.add_argument("--all", action="store_true", help="Procesar todas (347)")
    group.add_argument("--queue", help="Archivo queue: cod|nombre por línea")
    parser.add_argument("--tunnels", type=int, default=NUM_TUNNELS)
    parser.add_argument("--workdir", default="/tmp/fase0_v2")
    parser.add_argument("--skip-s3-check", action="store_true")
    parser.add_argument("--download-only", action="store_true", help="Solo descargar JSONs + CSV (pasos 1-6), no vectorizar")
    args = parser.parse_args()

    # Ensure catastro CSV exists
    if not os.path.exists(CATASTRO_CSV):
        print(f"Descargando catastro CSV desde S3...", flush=True)
        client = s3_client()
        client.download_file(S3_BUCKET, CATASTRO_S3_KEY, CATASTRO_CSV)

    client = s3_client()

    if args.comuna:
        process_comuna(args.comuna, args.workdir, args.tunnels, client,
                       args.skip_s3_check, getattr(args, "download_only", False))
    elif args.queue:
        with open(args.queue) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                cod = int(line.split("|")[0])
                process_comuna(cod, args.workdir, args.tunnels, client,
                               args.skip_s3_check)
    elif args.all:
        # Get all comunas from roles_split
        comunas = set()
        for f in os.listdir(ROLES_SPLIT_DIR):
            if f.endswith(".txt"):
                try:
                    comunas.add(int(f.replace(".txt", "")))
                except ValueError:
                    pass
        # Also from catastro
        if os.path.exists(CATASTRO_CSV):
            cat_comunas = pd.read_csv(
                CATASTRO_CSV, dtype=str, usecols=["comuna"],
                nrows=0  # just get columns
            )
            for chunk in pd.read_csv(CATASTRO_CSV, dtype=str,
                                     usecols=["comuna"], chunksize=500_000):
                comunas.update(int(c) for c in chunk["comuna"].unique())

        comunas = sorted(comunas)
        print(f"Total comunas: {len(comunas)}", flush=True)
        for cod in comunas:
            process_comuna(cod, args.workdir, args.tunnels, client,
                           args.skip_s3_check)

    print(f"\n✓ Phase 0 v2 completada.", flush=True)


if __name__ == "__main__":
    main()
