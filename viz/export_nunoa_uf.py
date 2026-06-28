#!/usr/bin/env python3
"""Exporta Ñuñoa con precio suelo UF/m2 para viz 3D."""
import geopandas as gpd
import pandas as pd
import numpy as np
import time, os

t0 = time.time()
print("Cargando GPKG...")
gdf = gpd.read_file("/tmp/nunoa_f4v2.gpkg")
print(f"  {len(gdf)} filas, {time.time()-t0:.0f}s")

gdf = gdf[gdf.geometry.notna()].copy()
if gdf.crs and gdf.crs.to_epsg() != 4326:
    gdf = gdf.to_crs(epsg=4326)

# Parsear UF/m2: "7,54 UF" -> 7.54
raw = gdf["obs_valor_comercial_m2_suelo"].astype(str)
uf_vals = raw.str.replace(" UF", "", regex=False).str.replace(",", ".", regex=False)
uf_vals = pd.to_numeric(uf_vals, errors="coerce")
# Filtrar basura (codigos comuna, etc)
uf_vals[uf_vals >= 200] = np.nan

result = gpd.GeoDataFrame(geometry=gdf.geometry)
result["uf_m2"] = uf_vals.round(2).fillna(0)

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

# Simplificar
print("Simplificando geometrias...")
result["geometry"] = result.geometry.simplify(0.00002, preserve_topology=True)
result = result[~result.geometry.is_empty].copy()

# Stats
uf = result["uf_m2"]
con_dato = (uf > 0).sum()
print(f"  {len(result)} poligonos, {con_dato} con UF/m2")
print(f"  Min={uf[uf>0].min():.1f}  Med={uf[uf>0].median():.1f}  Max={uf.max():.1f}")

out = "/tmp/nunoa_3d.geojson"
print(f"Exportando a {out}...")
result.to_file(out, driver="GeoJSON")
size = os.path.getsize(out) / 1024 / 1024
print(f"OK: {size:.1f} MB, {time.time()-t0:.0f}s")
