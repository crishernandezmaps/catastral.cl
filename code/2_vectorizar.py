"""
Paso 2: Vectorizar GeoTIFF del SII en poligonos de predios.

Lee un GeoTIFF RGBA descargado del SII y extrae los poligonos de predios
usando gdal_polygonize sobre Band 1 (Red) directamente, igual que QGIS.

Logica:
  - gdal_polygonize.py -b 1 genera poligonos para cada valor unico de Red
  - Se filtran los poligonos con DN == 182 (interior de predios en el mapa SII)
  - Holes compactos (texto/numeros) se rellenan automaticamente
  - Holes alargados (pasajes internos) se conservan
  - Resultado: GeoJSON + GeoPackage con poligonos angulares

Uso:
    python 2_vectorizar.py --input output/15105_NUNOA_z19/15105_NUNOA.tif
    python 2_vectorizar.py --input mi_comuna.tif --min-area 80 --max-hole 120
"""

import subprocess
import tempfile
import shutil
import rasterio
from rasterio.windows import Window
from shapely.geometry import Polygon
import geopandas as gpd
import numpy as np
import os
import argparse
import time
import math

# Buscar gdal_polygonize.py: primero en PATH, luego en QGIS
GDAL_POLYGONIZE = shutil.which("gdal_polygonize.py") or \
    "/Applications/QGIS.app/Contents/MacOS/bin/gdal_polygonize.py"


# ---------------------------------------------------------------------------
# Filtrado de poligonos
# ---------------------------------------------------------------------------

def _filtrar_poligono(poly, min_area_m2, max_area_m2, max_hole_m2):
    """
    Filtra y limpia un poligono. Retorna (feature_dict, holes_filled, holes_kept)
    o (None, holes_filled, holes_kept) si no pasa los filtros.
    """
    if not poly.is_valid:
        poly = poly.buffer(0)
    if not poly.is_valid:
        return None, 0, 0

    hf = 0
    hk = 0

    if poly.interiors:
        kept_holes = []
        for ring in poly.interiors:
            hp = Polygon(ring)
            h_area = hp.area
            h_perim = hp.length
            compactness = (4 * math.pi * h_area / (h_perim ** 2)
                           if h_perim > 0 else 1.0)
            if h_area < max_hole_m2 and compactness > 0.25:
                hf += 1
            else:
                kept_holes.append(ring)
                hk += 1
        poly = Polygon(poly.exterior, kept_holes)

    area = poly.area
    if area < min_area_m2 or area > max_area_m2:
        return None, hf, hk

    return {"geometry": poly, "area_m2": round(area, 1)}, hf, hk


# ---------------------------------------------------------------------------
# Polygonize via gdal_polygonize.py (identico a QGIS)
# ---------------------------------------------------------------------------

def _polygonize_gdal(tif_path, interior_value):
    """
    Polygonize Band 1 usando gdal_polygonize.py (identico a QGIS):
      gdal_polygonize.py input.tif -b 1 -f GPKG output.gpkg OUTPUT DN

    Retorna lista de poligonos con DN == interior_value.
    """
    tmp_gpkg = tempfile.mktemp(suffix='.gpkg')
    try:
        cmd = [GDAL_POLYGONIZE, tif_path, '-b', '1', '-f', 'GPKG',
               tmp_gpkg, 'OUTPUT', 'DN']
        t1 = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"gdal_polygonize fallo: {result.stderr}")

        gdf = gpd.read_file(tmp_gpkg)
        elapsed = time.time() - t1
        print(f"  {len(gdf):,} poligonos totales en {elapsed:.1f}s")

        # Filtrar por DN == interior_value
        gdf_interior = gdf[gdf['DN'] == interior_value]
        print(f"  {len(gdf_interior):,} poligonos con DN=={interior_value}")

        polys = gdf_interior.geometry.tolist()
        del gdf, gdf_interior
        return polys
    finally:
        if os.path.exists(tmp_gpkg):
            os.remove(tmp_gpkg)


# ---------------------------------------------------------------------------
# Fix: blank blocks sin bordes en z19 (SII WMS rendering issue)
# ---------------------------------------------------------------------------

WMS_BASE_URL = "https://www4.sii.cl/mapasui/services/ui/wmsProxyService/call"


def _fix_blank_blocks(tif_path, big_polys, cod_comuna, nombre_wms,
                      interior_value=182, eacano='2025'):
    """
    Corrige bloques blank en el TIF usando bordes de WMS a baja resolucion.

    El SII no renderiza bordes de predios a z19 en ciertas zonas.
    A resoluciones mas bajas (2m, 1m, 0.5m/px) SI los renderiza.
    Usamos esos bordes como mascara sobre el TIF z19.
    """
    import requests
    from PIL import Image
    import io

    fixed_count = 0

    with rasterio.open(tif_path, 'r+') as dst:
        for res in [2.0, 1.0, 0.5]:
            for poly in big_polys:
                bounds = poly.bounds
                pad = 10
                bbox = (bounds[0] - pad, bounds[1] - pad,
                        bounds[2] + pad, bounds[3] + pad)
                width_m = bbox[2] - bbox[0]
                height_m = bbox[3] - bbox[1]

                req_w = max(256, int(width_m / res))
                req_h = max(256, int(height_m / res))

                params = {
                    'service': 'WMS', 'request': 'GetMap',
                    'layers': f'sii:BR_CART_{nombre_wms}_WMS',
                    'styles': 'PREDIOS_WMS_V0', 'format': 'image/png',
                    'transparent': 'true', 'version': '1.1.1',
                    'comuna': cod_comuna, 'eac': '0', 'eacano': eacano,
                    'height': str(req_h), 'width': str(req_w),
                    'srs': 'EPSG:3857',
                    'bbox': f'{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}'
                }

                try:
                    resp = requests.get(WMS_BASE_URL, params=params, timeout=60)
                    if resp.status_code != 200:
                                        continue
                    lowres = Image.open(io.BytesIO(resp.content)).convert('RGBA')
                except Exception:
                    continue
                arr = np.array(lowres)
                R_lr, A_lr = arr[:, :, 0], arr[:, :, 3]

                boundary = ((R_lr != interior_value) & (A_lr > 0)).astype(
                    np.uint8) * 255
                if (boundary > 0).sum() < 10:
                    continue

                # Mapear a coordenadas de pixel del TIF
                row_top, col_left = dst.index(bbox[0], bbox[3])
                row_bot, col_right = dst.index(bbox[2], bbox[1])
                if row_top > row_bot:
                    row_top, row_bot = row_bot, row_top
                if col_left > col_right:
                    col_left, col_right = col_right, col_left

                # Clamp to raster bounds
                row_top = max(0, row_top)
                col_left = max(0, col_left)
                row_bot = min(dst.height, row_bot)
                col_right = min(dst.width, col_right)

                z19_w = col_right - col_left
                z19_h = row_bot - row_top
                if z19_w <= 0 or z19_h <= 0:
                    continue

                win = Window(col_left, row_top, z19_w, z19_h)
                R_z19 = dst.read(1, window=win)

                boundary_z19 = np.array(
                    Image.fromarray(boundary).resize(
                        (R_z19.shape[1], R_z19.shape[0]), Image.NEAREST))

                R_z19[boundary_z19 > 0] = 0
                dst.write(R_z19, 1, window=win)
                fixed_count += 1

    return fixed_count


# ---------------------------------------------------------------------------
# Pipeline de vectorizacion
# ---------------------------------------------------------------------------

def vectorizar_predios(tif_path, output_dir=None, min_area_m2=1,
                       max_area_m2=50000, max_hole_m2=100,
                       interior_value=182, prefix=None,
                       cod_comuna=None, nombre_wms=None, eacano='2025'):
    """
    Vectoriza un GeoTIFF del SII en poligonos de predios.

    Args:
        tif_path: Ruta al GeoTIFF (RGBA, EPSG:3857)
        output_dir: Directorio de salida (default: junto al TIF)
        min_area_m2: Area minima de predio (m2)
        max_area_m2: Area maxima de predio (m2)
        max_hole_m2: Holes menores a esto se rellenan (texto/numeros)
        interior_value: Valor de Band 1 (Red) para interior de predios
        prefix: Prefijo para archivos de salida (ej: '13101_SANTIAGO_CENTRO')
                Si es None, se usa 'predios'.
    """
    print("=" * 70)
    print("VECTORIZACION DE PREDIOS SII")
    print("=" * 70)

    t0 = time.time()

    if output_dir is None:
        output_dir = os.path.dirname(tif_path)
    os.makedirs(output_dir, exist_ok=True)

    # --- 1. Info del raster ---
    print(f"\n[1/4] Cargando {tif_path}...")
    with rasterio.open(tif_path) as src:
        w, h = src.width, src.height
        pixel_size = abs(src.transform[0])
        crs = src.crs

    print(f"  Pixel: {pixel_size:.4f} m ({pixel_size**2:.4f} m2)")
    print(f"  CRS: {crs}")
    print(f"  Tamano: {w}x{h} px ({w*h/1e6:.0f}M px)")

    # --- 2. Polygonize Band 1 (gdal_polygonize.py, identico a QGIS) ---
    print(f"\n[2/4] Polygonize Band 1 (gdal_polygonize.py)...")
    raw_polys = _polygonize_gdal(tif_path, interior_value)

    # --- 3. Fix blank blocks (SII no renderiza bordes en z19 para ciertas zonas) ---
    # Se trabaja sobre una copia del TIF para preservar el original
    work_tif = tif_path  # por defecto, usar el original
    big_polys = [p for p in raw_polys if p.area > max_area_m2]
    if big_polys and cod_comuna and nombre_wms:
        print(f"\n[3/4] Corrigiendo {len(big_polys)} bloques blank "
              f"(WMS low-res)...")
        fixed_tif = tif_path.replace('.tif', '_fixed.tif')
        shutil.copy2(tif_path, fixed_tif)
        n_fixed = _fix_blank_blocks(fixed_tif, big_polys, cod_comuna,
                                    nombre_wms, interior_value, eacano)
        if n_fixed > 0:
            print(f"  {n_fixed} mascaras aplicadas, re-polygonizando...")
            raw_polys = _polygonize_gdal(fixed_tif, interior_value)
            work_tif = fixed_tif
        else:
            print(f"  Sin bordes adicionales encontrados")
            os.remove(fixed_tif)
    elif big_polys:
        print(f"\n[3/4] {len(big_polys)} bloques grandes detectados "
              f"(sin WMS params para corregir)")
    else:
        print(f"\n[3/4] Sin bloques blank")

    # --- 4. Filtrar y limpiar ---
    print(f"\n[4/4] Filtrando ({min_area_m2}-{max_area_m2} m2, holes < {max_hole_m2} m2)...")
    features = []
    holes_filled = 0
    holes_kept = 0

    for poly in raw_polys:
        feat, hf, hk = _filtrar_poligono(poly, min_area_m2, max_area_m2, max_hole_m2)
        holes_filled += hf
        holes_kept += hk
        if feat is not None:
            features.append(feat)

    del raw_polys

    print(f"  {len(features):,} predios validos")
    print(f"  Holes: {holes_filled:,} rellenados (texto), {holes_kept:,} conservados (pasajes)")

    # --- Estadisticas ---
    if features:
        areas = np.array([f["area_m2"] for f in features])
        print(f"\n  Distribucion de area:")
        bins = [(1,5), (5,20), (20,50), (50,100), (100,200), (200,300), (300,500),
                (500,1000), (1000,5000), (5000,50000)]
        for lo, hi in bins:
            c = np.sum((areas >= lo) & (areas < hi))
            if c > 0:
                label = f"{lo}-{hi}" if hi < 1000 else f"{lo/1000:.0f}K-{hi/1000:.0f}K"
                print(f"    {label:>10s} m2: {c:>5,}")

    # --- Exportar ---
    if not features:
        print(f"\n  Sin predios válidos en este cluster")
        return None

    print(f"\nExportando...")
    gdf = gpd.GeoDataFrame(features, crs=crs)

    base_name = prefix or "predios"

    # GeoJSON en EPSG:4326
    gdf_4326 = gdf.to_crs("EPSG:4326")
    geojson_path = os.path.join(output_dir, f"{base_name}.geojson")
    gdf_4326.to_file(geojson_path, driver="GeoJSON")
    size_mb = os.path.getsize(geojson_path) / (1024*1024)
    print(f"  GeoJSON: {geojson_path} ({size_mb:.1f} MB)")

    del gdf_4326  # liberar

    # GeoPackage en EPSG:3857
    gpkg_path = os.path.join(output_dir, f"{base_name}.gpkg")
    gdf.to_file(gpkg_path, driver="GPKG")
    size_mb = os.path.getsize(gpkg_path) / (1024*1024)
    print(f"  GeoPackage: {gpkg_path} ({size_mb:.1f} MB)")

    elapsed = time.time() - t0
    print(f"\n{'='*70}")
    print(f"RESULTADO: {len(features):,} predios en {elapsed:.1f}s")
    print(f"{'='*70}")

    return gdf


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Vectoriza GeoTIFF del SII en poligonos de predios"
    )
    parser.add_argument("--input", required=True,
                        help="Ruta al GeoTIFF")
    parser.add_argument("--output-dir", default=None,
                        help="Directorio de salida (default: junto al TIF)")
    parser.add_argument("--min-area", type=float, default=1,
                        help="Area minima predio m2 (default: 1)")
    parser.add_argument("--max-area", type=float, default=50000,
                        help="Area maxima predio m2 (default: 50000)")
    parser.add_argument("--max-hole", type=float, default=100,
                        help="Holes < esto se rellenan (default: 100 m2)")
    parser.add_argument("--interior-value", type=int, default=182,
                        help="Valor Red de interior de predios (default: 182)")
    parser.add_argument("--prefix", default=None,
                        help="Prefijo para archivos (ej: 15105_NUNOA). "
                             "Default: nombre del TIF sin extension")

    args = parser.parse_args()

    prefix = args.prefix
    if prefix is None:
        prefix = os.path.splitext(os.path.basename(args.input))[0]

    vectorizar_predios(
        tif_path=args.input,
        output_dir=args.output_dir,
        min_area_m2=args.min_area,
        max_area_m2=args.max_area,
        max_hole_m2=args.max_hole,
        interior_value=args.interior_value,
        prefix=prefix,
    )
