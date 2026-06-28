#!/usr/bin/env python3
"""
Prepara datos de Ñuñoa para visualización 3D.

Ejecutar en el VPS (root@46.62.214.65):
    cd /root/carto_predios/sii_vectorizer
    source ../venv/bin/activate
    python3 prepare_nunoa_data.py

O en cualquier máquina con acceso a S3 y geopandas instalado.

Output: nunoa_3d.geojson (~40-60 MB)
"""
import geopandas as gpd
import numpy as np
import json
import os
import sys
import tempfile
import time

# -- Config --
COMUNA_COD = "15105"
COMUNA_NOMBRE = "ÑUÑOA"
S3_BUCKET = "siipredios"
GPKG_KEY = f"2025ss_bcn/fase4v2/comuna={COMUNA_COD}.gpkg"
OUTPUT_FILE = "nunoa_3d.geojson"

# Simplificación: tolerancia en grados (~2m a latitud -33°)
SIMPLIFY_TOLERANCE = 0.00002
# Decimales de coordenadas (5 = ~1m precisión, suficiente para 3D)
COORD_PRECISION = 5
# Área mínima de polígono en m² (filtrar artefactos)
MIN_AREA_M2 = 5


def download_from_s3(key, local_path):
    """Descarga archivo de S3 Hetzner."""
    import boto3
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT", "https://nbg1.your-objectstorage.com"),
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )
    print(f"  Descargando s3://{S3_BUCKET}/{key} ...")
    s3.download_file(S3_BUCKET, key, local_path)
    size_mb = os.path.getsize(local_path) / 1024 / 1024
    print(f"  ✓ {size_mb:.0f} MB descargados")


def find_avaluo_column(gdf):
    """Busca la columna de avalúo fiscal en orden de preferencia."""
    candidates = ["valorTotal", "rc_avaluo_total", "dc_avaluo_fiscal", "avaluo_total"]
    for col in candidates:
        if col in gdf.columns:
            return col
    return None


def find_destino_column(gdf):
    """Busca la columna de destino predial."""
    candidates = ["destinoDescripcion", "dc_cod_destino", "destino"]
    for col in candidates:
        if col in gdf.columns:
            return col
    return None


def compute_height(avaluo_series):
    """
    Calcula alturas 3D a partir del avalúo fiscal usando escala logarítmica.

    Mapeo:
      100K CLP  →  5m
      1M   CLP  → 20m
      10M  CLP  → 60m
      50M  CLP  → 100m
      100M CLP  → 120m
      500M CLP  → 160m
      1B   CLP  → 180m
    """
    log_val = np.log10(avaluo_series.clip(lower=1))
    # Mapear rango log [5, 10] → alturas [5, 200]
    height = (log_val - 5) * 40
    return height.clip(lower=3, upper=300)


def round_geometry_coords(geom, precision):
    """Redondea coordenadas de una geometría para reducir tamaño."""
    import shapely
    return shapely.set_precision(geom, 10 ** (-precision))


def main():
    t0 = time.time()
    print(f"═══ Preparación datos 3D: {COMUNA_NOMBRE} ({COMUNA_COD}) ═══\n")

    # --- 1. Obtener GPKG ---
    gpkg_path = None
    local_gpkg = f"/tmp/comuna={COMUNA_COD}.gpkg"

    if os.path.exists(local_gpkg):
        print(f"[1/5] GPKG ya existe en {local_gpkg}")
        gpkg_path = local_gpkg
    else:
        print(f"[1/5] Descargando GPKG desde S3...")
        try:
            download_from_s3(GPKG_KEY, local_gpkg)
            gpkg_path = local_gpkg
        except Exception as e:
            print(f"  ✗ Error S3: {e}")
            print(f"  Buscando GPKG local...")
            # Buscar en rutas comunes del VPS
            for path in [
                f"/tmp/f4v2/comuna={COMUNA_COD}.gpkg",
                f"/tmp/fase4v2/comuna={COMUNA_COD}.gpkg",
                f"/root/carto_predios/output/comuna={COMUNA_COD}.gpkg",
            ]:
                if os.path.exists(path):
                    gpkg_path = path
                    print(f"  ✓ Encontrado: {path}")
                    break

    if not gpkg_path:
        print("✗ No se encontró el GPKG. Asegúrate de tener acceso a S3 o el archivo local.")
        sys.exit(1)

    # --- 2. Cargar y filtrar ---
    print(f"\n[2/5] Cargando GPKG...")
    gdf = gpd.read_file(gpkg_path)
    print(f"  {len(gdf)} filas cargadas, {len(gdf.columns)} columnas")

    # Solo filas con geometría
    gdf = gdf[gdf.geometry.notna()].copy()
    print(f"  {len(gdf)} filas con geometría")

    # Asegurar EPSG:4326
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        print(f"  Reproyectando de {gdf.crs} a EPSG:4326...")
        gdf = gdf.to_crs(epsg=4326)

    # Filtrar polígonos muy pequeños
    if "pol_area_m2" in gdf.columns:
        area = gdf["pol_area_m2"].astype(float, errors="ignore")
        mask = area >= MIN_AREA_M2
        n_removed = (~mask).sum()
        if n_removed > 0:
            gdf = gdf[mask].copy()
            print(f"  Filtrados {n_removed} polígonos < {MIN_AREA_M2} m²")

    # --- 3. Extraer columnas útiles ---
    print(f"\n[3/5] Extrayendo atributos...")

    avaluo_col = find_avaluo_column(gdf)
    destino_col = find_destino_column(gdf)

    result = gpd.GeoDataFrame(geometry=gdf.geometry)

    # Avalúo fiscal
    if avaluo_col:
        result["avaluo"] = (
            gdf[avaluo_col]
            .astype(str)
            .str.replace(",", ".", regex=False)
            .str.extract(r"([\d.]+)", expand=False)
            .astype(float, errors="ignore")
            .fillna(0)
            .astype(int)
        )
        print(f"  Avalúo: columna '{avaluo_col}'")
        print(f"    Min: ${result['avaluo'].min():,.0f} — Median: ${result['avaluo'].median():,.0f} — Max: ${result['avaluo'].max():,.0f}")
    else:
        result["avaluo"] = 0
        print("  ⚠ No se encontró columna de avalúo")

    # Altura 3D
    result["height"] = compute_height(result["avaluo"].astype(float)).round(1)

    # Destino
    if destino_col:
        result["destino"] = gdf[destino_col].fillna("").astype(str)
        # Abreviar destinos largos
        destino_map = {
            "HABITACIONAL": "H",
            "COMERCIO": "C",
            "INDUSTRIA": "I",
            "OFICINA": "O",
            "EDUCACION Y CULTURA": "E",
            "HOTEL, MOTEL": "HO",
            "ADMINISTRACION PUBLICA": "AP",
            "ESTACIONAMIENTO": "EST",
            "DEPORTE Y RECREACION": "D",
            "SALUD": "S",
            "CULTO": "CU",
            "BODEGA": "B",
            "AGRICOLA": "A",
            "MINERIA": "M",
            "BIEN COMUN": "BC",
            "SITIO ERIAZO": "SE",
        }
        for full, abbr in destino_map.items():
            result.loc[result["destino"].str.upper() == full, "destino"] = abbr
        print(f"  Destino: columna '{destino_col}'")
    else:
        result["destino"] = ""

    # Rol
    if "manzana" in gdf.columns and "predio" in gdf.columns:
        mz = gdf["manzana"].astype(str).str.strip().str.lstrip("0")
        pr = gdf["predio"].astype(str).str.strip().str.lstrip("0")
        result["rol"] = mz + "-" + pr
    elif "rol" in gdf.columns:
        result["rol"] = gdf["rol"].astype(str)
    else:
        result["rol"] = ""

    # Pisos (para tooltip)
    if "pisos_max" in gdf.columns:
        result["pisos"] = gdf["pisos_max"].fillna("").astype(str)

    print(f"  {len(result)} predios listos")

    # --- 4. Simplificar geometrías ---
    print(f"\n[4/5] Simplificando geometrías (tolerancia ~2m)...")
    result["geometry"] = result.geometry.simplify(SIMPLIFY_TOLERANCE, preserve_topology=True)

    # Redondear coordenadas
    result["geometry"] = result.geometry.apply(
        lambda g: round_geometry_coords(g, COORD_PRECISION)
    )

    # Remover geometrías vacías post-simplificación
    result = result[~result.geometry.is_empty].copy()
    print(f"  {len(result)} polígonos tras simplificación")

    # --- 5. Exportar GeoJSON ---
    print(f"\n[5/5] Exportando {OUTPUT_FILE}...")
    result.to_file(OUTPUT_FILE, driver="GeoJSON")

    size_mb = os.path.getsize(OUTPUT_FILE) / 1024 / 1024
    elapsed = time.time() - t0

    print(f"\n{'═' * 50}")
    print(f"  ✓ {OUTPUT_FILE}")
    print(f"  {len(result):,} polígonos")
    print(f"  {size_mb:.1f} MB")
    print(f"  {elapsed:.0f}s")
    print(f"{'═' * 50}")
    print(f"\nCopia el archivo a tu máquina local y ábrelo con tour_3d.html")


if __name__ == "__main__":
    main()
