#!/usr/bin/env python3
"""Exporta Ñuñoa con AH como variable categórica para viz 3D."""
import geopandas as gpd
import pandas as pd
import numpy as np
import time, os, json, hashlib, colorsys

t0 = time.time()
print("Cargando GPKG...")
gdf = gpd.read_file("/tmp/nunoa_f4v2.gpkg")
print(f"  {len(gdf)} filas, {time.time()-t0:.0f}s")

gdf = gdf[gdf.geometry.notna()].copy()
if gdf.crs and gdf.crs.to_epsg() != 4326:
    gdf = gdf.to_crs(epsg=4326)

# AH
ah = gdf["ah"].fillna("").astype(str)
ah[ah == "nan"] = ""

# Generar color único por AH usando hue spacing
ah_unicos = sorted(ah[ah != ""].unique())
n = len(ah_unicos)
print(f"  {n} AH unicas")

# Generar paleta con hues bien distribuidos y saturación/luminosidad variada
ah_colors = {}
for i, a in enumerate(ah_unicos):
    hue = (i * 137.508) % 360  # golden angle para máxima separación
    sat = 0.65 + (i % 3) * 0.12
    lit = 0.50 + (i % 4) * 0.08
    r, g, b = colorsys.hls_to_rgb(hue / 360, lit, sat)
    ah_colors[a] = "#{:02x}{:02x}{:02x}".format(int(r*255), int(g*255), int(b*255))

# Asignar índice numérico a cada AH (para height expression)
ah_index = {a: i for i, a in enumerate(ah_unicos)}

result = gpd.GeoDataFrame(geometry=gdf.geometry)
result["ah"] = ah
result["ah_idx"] = ah.map(ah_index).fillna(-1).astype(int)

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

# Simplificar
print("Simplificando geometrias...")
result["geometry"] = result.geometry.simplify(0.00002, preserve_topology=True)
result = result[~result.geometry.is_empty].copy()

con_ah = (result["ah"] != "").sum()
print(f"  {len(result)} poligonos, {con_ah} con AH")

out = "/tmp/nunoa_3d.geojson"
print(f"Exportando a {out}...")
result.to_file(out, driver="GeoJSON")
size = os.path.getsize(out) / 1024 / 1024
print(f"OK: {size:.1f} MB, {time.time()-t0:.0f}s")

# Guardar paleta como JSON para el HTML
palette_out = "/tmp/nunoa_ah_palette.json"
with open(palette_out, "w") as f:
    json.dump(ah_colors, f)
print(f"Paleta: {palette_out} ({len(ah_colors)} colores)")

# Mostrar paleta
for a in ah_unicos[:10]:
    cnt = (result["ah"] == a).sum()
    print(f"  {a}: {ah_colors[a]} ({cnt} predios)")
print(f"  ... y {len(ah_unicos)-10} mas")
