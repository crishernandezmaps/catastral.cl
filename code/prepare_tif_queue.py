#!/usr/bin/env python3
"""
Pre-calcula la queue para descarga de TIFs.

Para cada comuna genera: cod, nombre_wms, bbox, tier, tiles_estimados.
- BCN bbox si < BCN_TILE_LIMIT tiles (comunas chicas/urbanas)
- DB bbox (catastro_actual lat/lon con filtro IQR) si BCN bbox es muy grande

Bbox de predios se obtiene de PostgreSQL en roles.tremen.tech (una sola query).

Output: TSV ordenado por tiles_total ascendente dentro de cada tier (A primero, B despues).

Uso:
    python3 prepare_tif_queue.py [--output /tmp/tif_queue.tsv]
"""

import json
import math
import os
import sys
import unicodedata
import argparse

TIER_FILE = os.environ.get(
    "TIER_FILE",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "TIER_TIF.txt"),
)
BCN_BBOX_FILE = os.environ.get(
    "BCN_BBOX_FILE",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "bcn_bbox.json"),
)
DB_URL = os.environ.get(
    "DB_URL",
    "postgresql://roles_reader:YOUR_DB_PASSWORD@YOUR_DB_IP:5435/roles",
)
ZOOM = 19
PAD_DB = 0.002    # ~200m padding para bbox desde DB lat/lon
BCN_TILE_LIMIT = 50000


def normalizar(texto):
    texto = texto.replace("\xd1", "N").replace("\xf1", "n")
    texto = texto.replace("Ñ", "N").replace("ñ", "n")
    nfkd = unicodedata.normalize("NFKD", texto)
    clean = "".join(c for c in nfkd if not unicodedata.combining(c)).upper()
    return clean.replace(" ", "_")


EXCEPCIONES_WMS = {
    "13101": "SANTIAGO_CENTRO",
}


def ll2t(lat, lon, z):
    lr = math.radians(lat)
    nn = 2 ** z
    return (
        int((lon + 180) / 360 * nn),
        int((1 - math.asinh(math.tan(lr)) / math.pi) / 2 * nn),
    )


def bbox_to_tiles(min_lon, min_lat, max_lon, max_lat):
    sx, sy = ll2t(max_lat, min_lon, ZOOM)
    mx, my = ll2t(min_lat, max_lon, ZOOM)
    if sx > mx:
        sx, mx = mx, sx
    if sy > my:
        sy, my = my, sy
    tx = mx - sx + 1
    ty = my - sy + 1
    return tx, ty


def load_db_bboxes():
    """Load IQR-filtered bboxes for all comunas from PostgreSQL."""
    import psycopg2
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    # Use percentiles for IQR filtering directly in SQL
    cur.execute("""
        WITH stats AS (
            SELECT comuna,
                   percentile_cont(0.25) WITHIN GROUP (ORDER BY lat) AS lat_q1,
                   percentile_cont(0.75) WITHIN GROUP (ORDER BY lat) AS lat_q3,
                   percentile_cont(0.25) WITHIN GROUP (ORDER BY lon) AS lon_q1,
                   percentile_cont(0.75) WITHIN GROUP (ORDER BY lon) AS lon_q3
            FROM catastro_actual
            WHERE lat IS NOT NULL AND lat != 0 AND lon != 0
            GROUP BY comuna
        )
        SELECT s.comuna,
               MIN(c.lat) FILTER (WHERE c.lat BETWEEN s.lat_q1 - 3*(s.lat_q3-s.lat_q1) AND s.lat_q3 + 3*(s.lat_q3-s.lat_q1)),
               MAX(c.lat) FILTER (WHERE c.lat BETWEEN s.lat_q1 - 3*(s.lat_q3-s.lat_q1) AND s.lat_q3 + 3*(s.lat_q3-s.lat_q1)),
               MIN(c.lon) FILTER (WHERE c.lon BETWEEN s.lon_q1 - 3*(s.lon_q3-s.lon_q1) AND s.lon_q3 + 3*(s.lon_q3-s.lon_q1)),
               MAX(c.lon) FILTER (WHERE c.lon BETWEEN s.lon_q1 - 3*(s.lon_q3-s.lon_q1) AND s.lon_q3 + 3*(s.lon_q3-s.lon_q1)),
               COUNT(*) FILTER (WHERE c.lat BETWEEN s.lat_q1 - 3*(s.lat_q3-s.lat_q1) AND s.lat_q3 + 3*(s.lat_q3-s.lat_q1))
        FROM catastro_actual c
        JOIN stats s ON c.comuna = s.comuna
        WHERE c.lat IS NOT NULL AND c.lat != 0 AND c.lon != 0
        GROUP BY s.comuna
        ORDER BY s.comuna
    """)
    result = {}
    for row in cur.fetchall():
        cod, lat_min, lat_max, lon_min, lon_max, cnt = row
        if lat_min is not None and lon_min is not None:
            result[str(cod)] = {
                "lat_min": float(lat_min),
                "lat_max": float(lat_max),
                "lon_min": float(lon_min),
                "lon_max": float(lon_max),
                "count": cnt,
            }
    conn.close()
    return result


def load_db_nombres():
    """Load comuna names from DB."""
    import psycopg2
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT comuna, comuna_nombre
        FROM catastro_actual
        WHERE comuna_nombre IS NOT NULL
        LIMIT 1000
    """)
    result = {}
    for cod, nombre in cur.fetchall():
        result[str(cod)] = nombre
    conn.close()
    return result


def calc_comuna(cod, tier, bcn_data, db_bboxes, db_nombres):
    """Hybrid: BCN bbox if < BCN_TILE_LIMIT, else DB bbox with IQR."""
    entry = bcn_data.get(cod)
    bcn_nombre = entry.get("nombre", "") if entry else ""
    bcn_bbox_str = entry.get("bbox", "") if entry else ""
    source = None

    # Try BCN bbox first
    if bcn_bbox_str:
        bcn_parts = [float(x) for x in bcn_bbox_str.split(",")]
        if len(bcn_parts) == 4:
            tx, ty = bbox_to_tiles(*bcn_parts)
            if tx * ty <= BCN_TILE_LIMIT:
                source = "BCN"
                min_lon, min_lat, max_lon, max_lat = bcn_parts
                nombre = bcn_nombre

    # Fallback to DB bbox
    if source is None:
        db_entry = db_bboxes.get(cod)
        if db_entry:
            min_lon = db_entry["lon_min"] - PAD_DB
            min_lat = db_entry["lat_min"] - PAD_DB
            max_lon = db_entry["lon_max"] + PAD_DB
            max_lat = db_entry["lat_max"] + PAD_DB
            nombre = db_nombres.get(cod, bcn_nombre)
            source = "DB"
        elif bcn_bbox_str:
            bcn_parts = [float(x) for x in bcn_bbox_str.split(",")]
            min_lon, min_lat, max_lon, max_lat = bcn_parts
            nombre = bcn_nombre
            source = "BCN*"
        else:
            return None

    if not nombre:
        nombre = bcn_nombre or db_nombres.get(cod, "")
    if not nombre:
        return None

    nombre_wms = EXCEPCIONES_WMS.get(cod, normalizar(nombre))
    tx, ty = bbox_to_tiles(min_lon, min_lat, max_lon, max_lat)

    return {
        "cod": cod,
        "nombre_wms": nombre_wms,
        "bbox": f"{min_lon:.6f},{min_lat:.6f},{max_lon:.6f},{max_lat:.6f}",
        "tier": tier,
        "tiles_x": tx,
        "tiles_y": ty,
        "tiles_total": tx * ty,
        "ram_gb": round(tx * 256 * ty * 256 * 4 / (1024 ** 3), 1),
        "source": source,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="/tmp/tif_queue.tsv")
    args = parser.parse_args()

    # Load BCN bboxes
    with open(BCN_BBOX_FILE) as f:
        bcn_data = json.load(f)
    print(f"BCN bbox: {len(bcn_data)} comunas cargadas")

    # Load DB bboxes (single query, IQR filtered)
    print("Cargando bboxes desde DB (IQR filtered)...")
    db_bboxes = load_db_bboxes()
    db_nombres = load_db_nombres()
    print(f"DB bbox: {len(db_bboxes)} comunas con coordenadas")

    # Read tier assignments
    order = []
    with open(TIER_FILE) as f:
        for line in f:
            parts = line.strip().split("\t")
            if parts[0] == "cod_comuna":
                continue
            order.append((parts[0], parts[1]))

    print(f"Procesando {len(order)} comunas...")
    results = []

    for i, (cod, tier) in enumerate(order):
        info = calc_comuna(cod, tier, bcn_data, db_bboxes, db_nombres)
        if info:
            results.append(info)
            flag = ""
            if info["tiles_total"] > 50000:
                flag = " ** LARGE **"
            print(
                f"  [{i+1}/{len(order)}] {cod} {info['nombre_wms']} ({info['source']}): "
                f"{info['tiles_x']}x{info['tiles_y']}={info['tiles_total']} tiles, "
                f"{info['ram_gb']} GB{flag}"
            )
        else:
            print(f"  [{i+1}/{len(order)}] {cod}: NO DATA")
        sys.stdout.flush()

    # Sort: Tier A ascending by tiles_total, then Tier B ascending by tiles_total
    tier_a = sorted([r for r in results if r["tier"] == "A"], key=lambda x: x["tiles_total"])
    tier_b = sorted([r for r in results if r["tier"] == "B"], key=lambda x: x["tiles_total"])
    results = tier_a + tier_b

    with open(args.output, "w") as f:
        f.write(
            "cod\tnombre_wms\tbbox\ttier\ttiles_x\ttiles_y\ttiles_total\tram_gb\n"
        )
        for r in results:
            f.write(
                f"{r['cod']}\t{r['nombre_wms']}\t{r['bbox']}\t{r['tier']}\t"
                f"{r['tiles_x']}\t{r['tiles_y']}\t{r['tiles_total']}\t{r['ram_gb']}\n"
            )

    large = [r for r in results if r["tiles_total"] > 50000]
    print(f"\nQueue: {args.output}")
    print(f"Total: {len(results)} comunas ({len(tier_a)} Tier A + {len(tier_b)} Tier B)")
    print(f"Large (>50K tiles): {len(large)}")
    for r in large:
        print(f"  {r['cod']} {r['nombre_wms']}: {r['tiles_total']} tiles, {r['ram_gb']} GB")


if __name__ == "__main__":
    main()
