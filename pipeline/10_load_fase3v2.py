#!/usr/bin/env python3
"""Load fase4v2 data into catastro_actual.

Phases:
  1. Create catastro_new staging table
  2. Load 343 CSVs from S3 (per-comuna streaming)
  3. Load geometries from GPKGs (per-comuna streaming)
  4. Atomic swap catastro_new -> catastro_actual
  5. Rebuild indexes

Usage:
  python 10_load_fase3v2.py                        # full run
  python 10_load_fase3v2.py --phase csv            # only CSVs
  python 10_load_fase3v2.py --phase gpkg           # only geometries
  python 10_load_fase3v2.py --phase swap           # swap + indexes
  python 10_load_fase3v2.py --comunas 13101,13102  # specific comunas
"""
import csv
import json
import logging
import os
import shutil
import sys
import time
from pathlib import Path

import boto3
import psycopg

from config import DB_DSN, S3_ENDPOINT, S3_REGION, S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET

LOG_FILE = Path("/tmp/fase4v2_load.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, mode="a"),
    ],
)
log = logging.getLogger("fase4v2")

STAGING_DIR = Path("/tmp/fase4v2_staging")
CATALOG_PATH = Path(__file__).parent.parent / "catalog_fase4v2.json"
TABLE = "catastro_new"
MIN_DISK_GB = 2

# ── Column mapping: CSV col -> (DB col, cast) ───────────────────────────
def _str(v): return v if v else None
def _int(v): return int(float(v)) if v else None
def _float(v): return float(v) if v else None
def _periodo(v): return v if v else "2025_2"

CSV_MAP = [
    ("periodo",                "periodo",               _periodo),
    ("comuna",                 "comuna",                _int),
    ("manzana",                "manzana",               _int),
    ("predio",                 "predio",                _int),
    ("txt_direccion",          "rc_direccion",          _str),
    ("txt_serie",              "rc_serie",              _str),
    ("txt_ind_aseo",           "rc_ind_aseo",           _str),
    ("txt_cuota_trimestral",   "rc_cuota_trimestral",   _int),
    ("txt_avaluo_total",       "rc_avaluo_total",       _int),
    ("txt_avaluo_exento",      "rc_avaluo_exento",      _int),
    ("txt_anio_term_exencion", "rc_anio_term_exencion", _int),
    ("txt_cod_ubicacion",      "rc_cod_ubicacion",      _str),
    ("txt_cod_destino",        "rc_cod_destino",        _str),
    ("dc_direccion",           "dc_direccion",          _str),
    ("dc_avaluo_fiscal",       "dc_avaluo_fiscal",      _int),
    ("dc_contribucion_semestral","dc_contribucion_semestral", _int),
    ("dc_cod_destino",         "dc_cod_destino",        _str),
    ("dc_avaluo_exento",       "dc_avaluo_exento",      _int),
    ("dc_sup_terreno",         "dc_sup_terreno",        _float),
    ("dc_cod_ubicacion",       "dc_cod_ubicacion",      _str),
    ("dc_bc1_comuna",          "dc_bc1_comuna",         _int),
    ("dc_bc1_manzana",         "dc_bc1_manzana",        _int),
    ("dc_bc1_predio",          "dc_bc1_predio",         _int),
    ("dc_bc2_comuna",          "dc_bc2_comuna",         _int),
    ("dc_bc2_manzana",         "dc_bc2_manzana",        _int),
    ("dc_bc2_predio",          "dc_bc2_predio",         _int),
    ("dc_padre_comuna",        "dc_padre_comuna",       _int),
    ("dc_padre_manzana",       "dc_padre_manzana",      _int),
    ("dc_padre_predio",        "dc_padre_predio",       _int),
    ("n_lineas_construccion",  "n_lineas_construccion", _int),
    ("sup_construida_total",   "sup_construida_total",  _float),
    ("anio_construccion_min",  "anio_construccion_min", _int),
    ("anio_construccion_max",  "anio_construccion_max", _int),
    ("materiales",             "materiales",            _str),
    ("calidades",              "calidades",             _str),
    ("pisos_max",              "pisos_max",             _int),
    ("serie",                  "serie",                 _str),
    ("lat",                    "lat",                   _float),
    ("lon",                    "lon",                   _float),
    ("valorComercial_clp_m2",  "valor_comercial_clp_m2",_float),
    ("ah",                     "ah",                    _str),
    ("ah_valorUnitario",       "ah_valor_unitario",     _float),
    ("destinoDescripcion",     "destino_descripcion",   _str),
    ("pol_area_m2",            "pol_area_m2",           _float),
    ("direccion_sii",          "direccion_sii",         _str),
    ("valorTotal",             "valor_total",           _int),
    ("valorAfecto",            "valor_afecto",          _int),
    ("valorExento",            "valor_exento",          _int),
    ("supTerreno",             "sup_terreno_api",       _float),
    ("supConsMt2",             "sup_cons_mt2",          _float),
]

DB_COLS = [m[1] for m in CSV_MAP] + ["anio", "semestre"]

CREATE_TABLE_SQL = f"""
DROP TABLE IF EXISTS {TABLE};
CREATE TABLE {TABLE} (
    id                        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    periodo                   TEXT NOT NULL,
    anio                      SMALLINT NOT NULL,
    semestre                  SMALLINT NOT NULL,
    comuna                    INTEGER NOT NULL,
    manzana                   INTEGER NOT NULL,
    predio                    INTEGER NOT NULL,
    rc_direccion              TEXT,
    rc_serie                  TEXT,
    rc_ind_aseo               TEXT,
    rc_cuota_trimestral       BIGINT,
    rc_avaluo_total           BIGINT,
    rc_avaluo_exento          BIGINT,
    rc_anio_term_exencion     SMALLINT,
    rc_cod_ubicacion          TEXT,
    rc_cod_destino            TEXT,
    dc_direccion              TEXT,
    dc_avaluo_fiscal          BIGINT,
    dc_contribucion_semestral BIGINT,
    dc_cod_destino            TEXT,
    dc_avaluo_exento          BIGINT,
    dc_sup_terreno            NUMERIC(16,2),
    dc_cod_ubicacion          TEXT,
    dc_bc1_comuna             INTEGER,
    dc_bc1_manzana            INTEGER,
    dc_bc1_predio             INTEGER,
    dc_bc2_comuna             INTEGER,
    dc_bc2_manzana            INTEGER,
    dc_bc2_predio             INTEGER,
    dc_padre_comuna           INTEGER,
    dc_padre_manzana          INTEGER,
    dc_padre_predio           INTEGER,
    n_lineas_construccion     SMALLINT,
    sup_construida_total      NUMERIC(16,2),
    anio_construccion_min     SMALLINT,
    anio_construccion_max     SMALLINT,
    materiales                TEXT,
    calidades                 TEXT,
    pisos_max                 SMALLINT,
    serie                     TEXT,
    lat                       DOUBLE PRECISION,
    lon                       DOUBLE PRECISION,
    geom                      geometry(Geometry, 4326),
    valor_comercial_clp_m2    NUMERIC(16,2),
    ah                        TEXT,
    ah_valor_unitario         NUMERIC(16,2),
    destino_descripcion       TEXT,
    pol_area_m2               NUMERIC(16,2),
    direccion_sii             TEXT,
    valor_total               BIGINT,
    valor_afecto              BIGINT,
    valor_exento              BIGINT,
    sup_terreno_api           NUMERIC(16,2),
    sup_cons_mt2              NUMERIC(16,2)
);
"""


def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        region_name=S3_REGION,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )


def load_catalog():
    with open(CATALOG_PATH) as f:
        return json.load(f)


def check_disk():
    usage = shutil.disk_usage("/tmp")
    free_gb = usage.free / (1024 ** 3)
    if free_gb < MIN_DISK_GB:
        raise RuntimeError(f"Only {free_gb:.1f} GB free on /tmp, need {MIN_DISK_GB} GB")


def parse_periodo(p):
    if not p or "_" not in str(p):
        return 2025, 2
    parts = str(p).split("_")
    try:
        return int(parts[0]), int(parts[1])
    except (ValueError, IndexError):
        return 2025, 2


def map_row(row):
    values = []
    for csv_col, db_col, cast_fn in CSV_MAP:
        raw = row.get(csv_col, "")
        if raw == "" or raw is None or str(raw).lower() == "nan":
            if db_col == "periodo":
                values.append("2025_2")
            else:
                values.append(None)
        else:
            try:
                values.append(cast_fn(raw))
            except (ValueError, TypeError):
                values.append(None)
    periodo_val = values[0] or "2025_2"
    anio, semestre = parse_periodo(periodo_val)
    values.extend([anio, semestre])
    return values


# ── Phase 1: Create table ────────────────────────────────────────────────

def phase_create_table():
    log.info(f"Phase 1: Creating {TABLE}")
    with psycopg.connect(DB_DSN) as conn:
        conn.execute(CREATE_TABLE_SQL)
        conn.commit()
    log.info(f"{TABLE} created")


# ── Phase 2: Load CSVs ──────────────────────────────────────────────────

def phase_load_csvs(catalog, comunas_filter=None):
    log.info("Phase 2: Loading CSVs from S3")
    s3 = get_s3()
    STAGING_DIR.mkdir(parents=True, exist_ok=True)

    comunas = catalog["comunas"]
    if comunas_filter:
        comunas = [c for c in comunas if c["codigo"] in comunas_filter]

    total_rows = 0
    t0 = time.time()
    cols_str = ", ".join(DB_COLS)

    with psycopg.connect(DB_DSN) as conn:
        for i, entry in enumerate(comunas):
            code = entry["codigo"]
            nombre = entry.get("nombre", code)
            csv_key = entry["archivos"]["csv"]["key"]

            # Skip if already loaded
            count = conn.execute(
                f"SELECT COUNT(*) FROM {TABLE} WHERE comuna = %s", [int(code)]
            ).fetchone()[0]
            if count > 0:
                log.info(f"[{i+1}/{len(comunas)}] SKIP {nombre} ({code}) — {count:,} rows")
                total_rows += count
                continue

            check_disk()
            local_csv = STAGING_DIR / f"comuna={code}.csv"
            try:
                log.info(f"[{i+1}/{len(comunas)}] Downloading {nombre} ({code})...")
                s3.download_file(S3_BUCKET, csv_key, str(local_csv))

                rows = 0
                with open(local_csv, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    with conn.cursor() as cur:
                        with cur.copy(f"COPY {TABLE} ({cols_str}) FROM STDIN") as copy:
                            for row in reader:
                                if not row.get("comuna") or not row.get("manzana") or not row.get("predio"):
                                    continue
                                copy.write_row(map_row(row))
                                rows += 1
                conn.commit()
                total_rows += rows
                pct = (i + 1) * 100 / len(comunas)
                elapsed = time.time() - t0
                rate = total_rows / elapsed if elapsed > 0 else 0
                log.info(f"  -> {rows:,} rows | Total: {total_rows:,} | {rate:,.0f} rows/s | {pct:.1f}%")

            except Exception as e:
                conn.rollback()
                log.error(f"  ERROR CSV {code}: {e}")
            finally:
                if local_csv.exists():
                    local_csv.unlink()

    elapsed = time.time() - t0
    log.info(f"Phase 2 complete: {total_rows:,} rows in {elapsed/60:.1f} min")


# ── Phase 3: Load geometries from GPKG ───────────────────────────────────

def phase_load_gpkg(catalog, comunas_filter=None):
    log.info("Phase 3: Loading geometries from GPKGs")

    try:
        import fiona
        from shapely.geometry import shape
    except ImportError:
        log.error("pip install fiona shapely")
        sys.exit(1)

    s3 = get_s3()
    STAGING_DIR.mkdir(parents=True, exist_ok=True)

    comunas = catalog["comunas"]
    if comunas_filter:
        comunas = [c for c in comunas if c["codigo"] in comunas_filter]

    total_geom = 0
    t0 = time.time()
    BATCH = 2000

    with psycopg.connect(DB_DSN) as conn:
        for i, entry in enumerate(comunas):
            code = entry["codigo"]
            nombre = entry.get("nombre", code)
            gpkg_key = entry["archivos"]["gpkg"]["key"]
            size_mb = entry["archivos"]["gpkg"]["tamano_mb"]

            # Skip if already loaded
            count = conn.execute(
                f"SELECT COUNT(*) FROM {TABLE} WHERE comuna = %s AND geom IS NOT NULL",
                [int(code)]
            ).fetchone()[0]
            if count > 0:
                log.info(f"[{i+1}/{len(comunas)}] SKIP {nombre} ({code}) — {count:,} geoms")
                total_geom += count
                continue

            check_disk()
            local_gpkg = STAGING_DIR / f"comuna={code}.gpkg"
            geoms = 0

            try:
                log.info(f"[{i+1}/{len(comunas)}] Downloading {nombre} ({code}, {size_mb:.0f} MB)...")
                s3.download_file(S3_BUCKET, gpkg_key, str(local_gpkg))

                batch = []
                with fiona.open(str(local_gpkg)) as src:
                    for feat in src:
                        geom = feat.get("geometry")
                        props = feat.get("properties", {})
                        if not geom:
                            continue
                        v = props.get("v", "")
                        if not v or "|" not in str(v):
                            continue
                        parts = str(v).split("|")
                        if len(parts) != 3:
                            continue
                        try:
                            c, m, p = int(parts[0]), int(parts[1]), int(parts[2])
                        except ValueError:
                            continue
                        try:
                            wkt = shape(geom).wkt
                        except Exception:
                            continue
                        batch.append((wkt, c, m, p))
                        if len(batch) >= BATCH:
                            _flush(conn, batch)
                            geoms += len(batch)
                            batch = []

                if batch:
                    _flush(conn, batch)
                    geoms += len(batch)

                conn.commit()
                total_geom += geoms
                pct = (i + 1) * 100 / len(comunas)
                log.info(f"  -> {geoms:,} geoms | Total: {total_geom:,} | {pct:.1f}%")

            except Exception as e:
                conn.rollback()
                log.error(f"  ERROR GPKG {code}: {e}")
            finally:
                if local_gpkg.exists():
                    local_gpkg.unlink()

    elapsed = time.time() - t0
    log.info(f"Phase 3 complete: {total_geom:,} geometries in {elapsed/60:.1f} min")


def _flush(conn, batch):
    with conn.cursor() as cur:
        cur.executemany(
            f"UPDATE {TABLE} SET geom = ST_GeomFromText(%s, 4326) WHERE comuna = %s AND manzana = %s AND predio = %s",
            batch,
        )


# ── Phase 4: Swap tables ────────────────────────────────────────────────

def phase_swap():
    log.info("Phase 4: Swapping tables")
    with psycopg.connect(DB_DSN) as conn:
        v2 = conn.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
        v2g = conn.execute(f"SELECT COUNT(*) FROM {TABLE} WHERE geom IS NOT NULL").fetchone()[0]
        old = conn.execute("SELECT COUNT(*) FROM catastro_actual").fetchone()[0]
        log.info(f"  {TABLE}: {v2:,} rows, {v2g:,} geoms")
        log.info(f"  catastro_actual: {old:,} rows")

        if v2 == 0:
            log.error(f"{TABLE} is empty, aborting!")
            return

        conn.execute("DROP TABLE IF EXISTS catastro_old")
        conn.execute("ALTER TABLE catastro_actual RENAME TO catastro_old")
        conn.execute(f"ALTER TABLE {TABLE} RENAME TO catastro_actual")
        conn.commit()
        log.info("  Swap done")

    phase_indexes()

    with psycopg.connect(DB_DSN) as conn:
        conn.execute("DROP TABLE IF EXISTS catastro_old")
        conn.commit()
    log.info("  Dropped catastro_old")


# ── Phase 5: Indexes ────────────────────────────────────────────────────

def phase_indexes():
    log.info("Phase 5: Building indexes")
    t0 = time.time()
    indexes = [
        "CREATE UNIQUE INDEX idx_actual_rol ON catastro_actual (comuna, manzana, predio)",
        "CREATE INDEX idx_actual_comuna ON catastro_actual (comuna)",
        "CREATE INDEX idx_actual_destino ON catastro_actual (dc_cod_destino)",
        "CREATE INDEX idx_actual_sup ON catastro_actual (dc_sup_terreno) WHERE dc_sup_terreno IS NOT NULL",
        "CREATE INDEX idx_actual_avaluo ON catastro_actual (rc_avaluo_total) WHERE rc_avaluo_total IS NOT NULL",
        "CREATE INDEX idx_actual_direccion ON catastro_actual USING gin (rc_direccion gin_trgm_ops)",
        "CREATE INDEX idx_actual_coords ON catastro_actual USING GIST (geography(ST_SetSRID(ST_MakePoint(lon, lat), 4326))) WHERE lat IS NOT NULL AND lon IS NOT NULL",
        "CREATE INDEX idx_catastro_actual_geom ON catastro_actual USING GIST (geom) WHERE geom IS NOT NULL",
    ]
    with psycopg.connect(DB_DSN, autocommit=True) as conn:
        for sql in indexes:
            name = sql.split("INDEX")[1].split("ON")[0].strip()
            log.info(f"  {name}...")
            conn.execute(f"DROP INDEX IF EXISTS {name}")
            conn.execute(sql)
        conn.execute("ANALYZE catastro_actual")
    log.info(f"Phase 5 complete in {(time.time()-t0)/60:.1f} min")


# ── Main ─────────────────────────────────────────────────────────────────

def main():
    args = sys.argv[1:]
    phase = None
    comunas_filter = None

    i = 0
    while i < len(args):
        if args[i] == "--phase" and i + 1 < len(args):
            phase = args[i + 1]; i += 2
        elif args[i] == "--comunas" and i + 1 < len(args):
            comunas_filter = set(args[i + 1].split(",")); i += 2
        else:
            i += 1

    catalog = load_catalog()
    log.info(f"Catalog: {catalog['total_comunas']} comunas (fase4v2)")
    t0 = time.time()

    if phase is None or phase == "create":
        phase_create_table()

    if phase is None or phase == "csv":
        if phase == "csv":
            try:
                with psycopg.connect(DB_DSN) as conn:
                    conn.execute(f"SELECT 1 FROM {TABLE} LIMIT 0")
            except Exception:
                phase_create_table()
        phase_load_csvs(catalog, comunas_filter)

    if phase is None or phase == "gpkg":
        # Create temp index for fast UPDATEs
        if phase == "gpkg" or phase is None:
            try:
                with psycopg.connect(DB_DSN, autocommit=True) as conn:
                    conn.execute(f"CREATE INDEX IF NOT EXISTS tmp_new_rol ON {TABLE} (comuna, manzana, predio)")
                log.info("  tmp index ready")
            except Exception:
                pass
        phase_load_gpkg(catalog, comunas_filter)

    if phase is None or phase == "swap":
        phase_swap()

    log.info(f"Done in {(time.time()-t0)/60:.1f} min")


if __name__ == "__main__":
    main()
