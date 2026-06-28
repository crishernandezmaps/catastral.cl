#!/usr/bin/env python3
"""Exporta Ñuñoa como GeoJSON liviano para viz 3D."""
import geopandas as gpd
import numpy as np
import time, os

t0 = time.time()
print("Cargando GPKG...")
gdf = gpd.read_file("/tmp/nunoa_f4v2.gpkg")
print(f"  {len(gdf)} filas, {len(gdf.columns)} cols, {time.time()-t0:.0f}s")

# Solo filas con geometria
gdf = gdf[gdf.geometry.notna()].copy()
print(f"  {len(gdf)} con geometria")

# Asegurar EPSG:4326
if gdf.crs and gdf.crs.to_epsg() != 4326:
    print(f"  Reproyectando de {gdf.crs} a EPSG:4326...")
    gdf = gdf.to_crs(epsg=4326)

# Buscar columna de avaluo
avaluo_col = None
for col in ["valorTotal", "rc_avaluo_total", "dc_avaluo_fiscal"]:
    if col in gdf.columns:
        avaluo_col = col
        break
print(f"  Avaluo: {avaluo_col}")

# Extraer columnas
result = gpd.GeoDataFrame(geometry=gdf.geometry)
result["avaluo"] = (
    gdf[avaluo_col].astype(str)
    .str.replace(",", ".", regex=False)
    .str.extract(r"([\d.]+)", expand=False)
    .astype(float, errors="ignore")
    .fillna(0).astype(int)
)

# Altura log
log_val = np.log10(result["avaluo"].clip(lower=1).astype(float))
result["height"] = ((log_val - 5) * 40).clip(3, 300).round(1)

# Destino
for col in ["destinoDescripcion", "dc_cod_destino"]:
    if col in gdf.columns:
        result["destino"] = gdf[col].fillna("").astype(str)
        break

# Rol
if "manzana" in gdf.columns and "predio" in gdf.columns:
    mz = gdf["manzana"].astype(str).str.strip().str.lstrip("0")
    pr = gdf["predio"].astype(str).str.strip().str.lstrip("0")
    result["rol"] = mz + "-" + pr

# Pisos
if "pisos_max" in gdf.columns:
    result["pisos"] = gdf["pisos_max"].fillna("").astype(str)

# Simplificar geometrias (~2m)
print("Simplificando geometrias...")
result["geometry"] = result.geometry.simplify(0.00002, preserve_topology=True)
result = result[~result.geometry.is_empty].copy()
print(f"  {len(result)} poligonos finales")

# Stats
av = result["avaluo"]
print(f"  Avaluo min={av.min()} med={av.median()} max={av.max()}")

# Exportar
out = "/tmp/nunoa_3d.geojson"
print(f"Exportando a {out}...")
result.to_file(out, driver="GeoJSON")
size = os.path.getsize(out) / 1024 / 1024
print(f"OK: {len(result)} predios, {size:.1f} MB, {time.time()-t0:.0f}s total")
