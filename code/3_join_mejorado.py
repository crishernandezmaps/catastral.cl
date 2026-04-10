#!/usr/bin/env python3
"""
3_join_mejorado.py — Join mejorado: asigna un polígono a cada predio.

Diferencia clave vs Fase 3 original:
  - F3 original: asigna UN rol a cada polígono (dirección polígono→predio)
  - F3 mejorado: asigna UN polígono a cada predio (dirección predio→polígono)
  Esto resuelve multi-unidad naturalmente: N predios → mismo polígono.

Input:
  - F0 CSV: todos los predios con rol, lat, lon, datos tabulares
  - F2 GPKG: todos los polígonos vectorizados (solo geometry + area_m2)
  - (opcional) --nombre: nombre WMS para getFeatureInfo de manzanas sin coords

Output:
  - CSV consolidado: todas las columnas F0 + pol_area_m2
  - GPKG: predios con geometría + polígonos huérfanos (sin dato tabular)

Métodos de asignación (en orden):
  1. Point-in-polygon: coordenada del predio cae dentro de un polígono
  2. Nearest 10m: polígono más cercano dentro de 10m
  3. Herencia coordenada: predios que comparten lat/lon heredan polígono
  4. Manzana neighbor: predios sin match heredan del vecino numérico
  5. getFeatureInfo (SII): para manzanas sin NINGÚN anchor, consultar SII
  6. Nearest sin restricción: último recurso, polígono más cercano en la comuna

Después del join, se agregan polígonos huérfanos (sin predio) al output.

Uso:
    python3 3_join_mejorado.py \
        --csv fase0/comuna=16162.csv \
        --gpkg vectors/comuna=16162.gpkg \
        --output /tmp/test_join/16162 \
        --cod 16162 \
        --nombre P_AGUIRRE_CERDA
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import time

import geopandas as gpd
import numpy as np
import pandas as pd
from pyproj import Transformer
from shapely.geometry import Point
from shapely.ops import transform as shp_transform

# ---------------------------------------------------------------------------
BASEDIR = os.path.dirname(os.path.abspath(__file__))
VENV_PYTHON = "/root/carto_predios/venv/bin/python3"
FEATUREINFO_SCRIPT = os.path.join(BASEDIR, "featureinfo_worker.py")
NUM_FI_TUNNELS = 30
WMS_LAYER_TPL = "sii:BR_CART_{nombre_wms}_WMS"

T_4326_TO_3857 = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
T_3857_TO_4326 = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)


def load_predios(csv_path):
    """Carga F0 CSV, convierte coords a numérico."""
    df = pd.read_csv(csv_path, engine="python", dtype=str, on_bad_lines="skip",
                      encoding_errors="replace")
    df["_lat"] = pd.to_numeric(df.get("lat"), errors="coerce")
    df["_lon"] = pd.to_numeric(df.get("lon"), errors="coerce")
    valid = (df["_lat"] >= -62) & (df["_lat"] <= -15) & \
            (df["_lon"] >= -80) & (df["_lon"] <= -64)
    df.loc[~valid, ["_lat", "_lon"]] = np.nan
    return df


MAX_HOLE_FILL_M2 = 200  # hoyos < 200 m² se rellenan siempre (números del WMS)


def _fill_small_holes(geom):
    """Rellena hoyos pequeños (números de predio renderizados por el WMS)."""
    if geom is None or geom.is_empty or geom.geom_type != "Polygon":
        return geom
    if not geom.interiors:
        return geom
    from shapely.geometry import Polygon as SPoly
    kept = [ring for ring in geom.interiors
            if SPoly(ring).area >= MAX_HOLE_FILL_M2]
    if len(kept) == len(list(geom.interiors)):
        return geom  # nada que rellenar
    return SPoly(geom.exterior, kept)


def load_polygons(gpkg_path):
    """Carga F2 GPKG, rellena hoyos de texto WMS, asegura EPSG:3857."""
    gdf = gpd.read_file(gpkg_path)
    if gdf.crs and gdf.crs.to_epsg() != 3857:
        gdf = gdf.to_crs("EPSG:3857")
    # Rellenar hoyos pequeños (números de predio del WMS)
    n_before = sum(len(list(g.interiors)) for g in gdf.geometry if g and g.geom_type == "Polygon" and g.interiors)
    gdf["geometry"] = gdf["geometry"].apply(_fill_small_holes)
    n_after = sum(len(list(g.interiors)) for g in gdf.geometry if g and g.geom_type == "Polygon" and g.interiors)
    if n_before != n_after:
        print(f"  Hoyos rellenados: {n_before - n_after} (números WMS <{MAX_HOLE_FILL_M2}m²)")
    # Recalcular área post-fill
    gdf["area_m2"] = gdf.geometry.area
    gdf = gdf.reset_index(drop=True)
    gdf["_poly_idx"] = gdf.index
    return gdf


# ---------------------------------------------------------------------------
# Método 5: getFeatureInfo para manzanas sin anchor
# ---------------------------------------------------------------------------
def run_featureinfo_for_polygons(gdf_poly, unmatched_poly_idxs, nombre_wms,
                                  workdir):
    """
    Para polígonos sin predio asignado, consulta getFeatureInfo por centroide.
    Retorna dict {poly_idx: rol} para los que el SII responde.
    """
    if not nombre_wms or not unmatched_poly_idxs:
        return {}

    queue_path = os.path.join(workdir, "fi_queue.txt")
    outdir = os.path.join(workdir, "fi_results")
    os.makedirs(outdir, exist_ok=True)

    # Escribir centroides de polígonos sin match como queue
    with open(queue_path, "w") as f:
        for pi in unmatched_poly_idxs:
            geom = gdf_poly.iloc[pi].geometry
            centroid = geom.centroid
            # Convertir centroide a 4326
            lon, lat = T_3857_TO_4326.transform(centroid.x, centroid.y)
            f.write(f"{pi},{lat},{lon}\n")

    layer = WMS_LAYER_TPL.format(nombre_wms=nombre_wms)

    # Lanzar workers
    procs = []
    for i in range(NUM_FI_TUNNELS):
        cmd = ["ip", "netns", "exec", f"vpn{i}",
               VENV_PYTHON, FEATUREINFO_SCRIPT,
               "--tunnel", str(i), "--layer", layer,
               "--queue", queue_path, "--outdir", outdir]
        procs.append(subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE))

    for p in procs:
        p.communicate()

    # Leer resultados
    results = {}
    for fname in os.listdir(outdir):
        if not fname.endswith(".json"):
            continue
        try:
            with open(os.path.join(outdir, fname)) as f:
                data = json.load(f)
            rol = (data.get("rol") or "").strip()
            # poly_idx está en el nombre del archivo: p_{idx}.json
            pi = int(fname.replace("p_", "").replace(".json", ""))
            if rol:
                results[pi] = rol
        except Exception:
            pass

    # Cleanup
    shutil.rmtree(outdir, ignore_errors=True)
    if os.path.exists(queue_path):
        os.remove(queue_path)

    return results


# ---------------------------------------------------------------------------
# Join principal (vectorizado — gpd.sjoin + sjoin_nearest)
# ---------------------------------------------------------------------------
def join_predios_to_polygons(df, gdf_poly, nombre_wms=None, workdir="/tmp"):
    t0 = time.time()
    n = len(df)

    df["_poly_idx"] = np.nan
    df["_match_method"] = None
    df["_orig_idx"] = df.index  # preservar índice original

    # --- Construir GeoDataFrame de puntos en EPSG:3857 ---
    has_coords = df["_lat"].notna() & df["_lon"].notna()
    gdf_pts = None
    if has_coords.any():
        xs, ys = T_4326_TO_3857.transform(
            df.loc[has_coords, "_lon"].values,
            df.loc[has_coords, "_lat"].values)
        gdf_pts = gpd.GeoDataFrame(
            {"_orig_idx": df.loc[has_coords].index},
            geometry=gpd.points_from_xy(xs, ys),
            crs="EPSG:3857")

    # =================================================================
    # Método 1: Point-in-polygon (vectorizado con sjoin)
    # =================================================================
    n1 = 0
    if gdf_pts is not None and not gdf_pts.empty:
        joined = gpd.sjoin(gdf_pts, gdf_poly[["geometry", "_poly_idx"]],
                           how="inner", predicate="within")
        # Desduplicar: un punto puede caer en overlap de polígonos → tomar primero
        joined = joined.drop_duplicates(subset=["_orig_idx"], keep="first")
        for _, row in joined.iterrows():
            oi = int(row["_orig_idx"])
            df.at[oi, "_poly_idx"] = row["_poly_idx"]
            df.at[oi, "_match_method"] = "1_contains"
        n1 = len(joined)

    t1 = time.time()
    print(f"  [1] Point-in-polygon: {n1} ({t1-t0:.1f}s)")

    # =================================================================
    # Método 2: Nearest 10m (vectorizado con sjoin_nearest)
    # =================================================================
    NEAREST_MAX = 10
    n2 = 0
    unmatched_mask = has_coords & df["_poly_idx"].isna()
    if unmatched_mask.any() and gdf_pts is not None:
        pts_unmatched = gdf_pts[gdf_pts["_orig_idx"].isin(
            df[unmatched_mask].index)].copy()
        if not pts_unmatched.empty:
            nearest = gpd.sjoin_nearest(
                pts_unmatched, gdf_poly[["geometry", "_poly_idx"]],
                how="inner", max_distance=NEAREST_MAX, distance_col="_dist")
            nearest = nearest.drop_duplicates(subset=["_orig_idx"], keep="first")
            for _, row in nearest.iterrows():
                oi = int(row["_orig_idx"])
                df.at[oi, "_poly_idx"] = row["_poly_idx"]
                df.at[oi, "_match_method"] = f"2_nearest_{row['_dist']:.1f}m"
            n2 = len(nearest)

    t2 = time.time()
    print(f"  [2] Nearest 10m: {n2} ({t2-t1:.1f}s)")

    # =================================================================
    # Método 3: Herencia coordenada (vectorizado con merge)
    # =================================================================
    n3 = 0
    df["_coord_key"] = None
    mask_lat = df["_lat"].notna()
    if mask_lat.any():
        df.loc[mask_lat, "_coord_key"] = (
            df.loc[mask_lat, "_lat"].round(6).astype(str) + "," +
            df.loc[mask_lat, "_lon"].round(6).astype(str))

        matched_coords = df[df["_poly_idx"].notna() & df["_coord_key"].notna()]
        coord_map = matched_coords.drop_duplicates(
            subset=["_coord_key"])[["_coord_key", "_poly_idx"]]
        coord_map = coord_map.rename(columns={"_poly_idx": "_inherited_pi"})

        unmatched_with_key = df["_poly_idx"].isna() & df["_coord_key"].notna()
        if unmatched_with_key.any():
            merged = df.loc[unmatched_with_key, ["_coord_key"]].merge(
                coord_map, on="_coord_key", how="left")
            inherited = merged["_inherited_pi"].notna()
            idxs = df[unmatched_with_key].index[inherited.values]
            df.loc[idxs, "_poly_idx"] = merged.loc[inherited.values, "_inherited_pi"].values
            df.loc[idxs, "_match_method"] = "3_coord_inherit"
            n3 = len(idxs)

    df = df.drop(columns=["_coord_key"], errors="ignore")
    t3 = time.time()
    print(f"  [3] Herencia coord: {n3} ({t3-t2:.1f}s)")

    # =================================================================
    # Método 4: Manzana neighbor (vectorizado con merge_asof)
    # =================================================================
    MAX_DIFF = 20
    n4 = 0
    mz_lookup = {}  # kept for method 6 fallback
    if "manzana" in df.columns and "predio" in df.columns:
        df["_predio_n"] = pd.to_numeric(df["predio"], errors="coerce")

        # Build lookup from matched
        matched_mz = df[df["_poly_idx"].notna() & df["_predio_n"].notna()].copy()
        for mz, grp in matched_mz.groupby("manzana"):
            mz_lookup[mz] = list(zip(
                grp["_predio_n"].astype(int).tolist(),
                grp["_poly_idx"].astype(int).tolist()))

        unmatched_mz = df[df["_poly_idx"].isna() & df["_predio_n"].notna()].copy()
        for idx in unmatched_mz.index:
            mz = df.at[idx, "manzana"]
            pn = df.at[idx, "_predio_n"]
            if not mz or pd.isna(pn):
                continue
            pn = int(pn)
            neighbors = mz_lookup.get(mz, [])
            if not neighbors:
                continue
            best_pn, best_pi = min(neighbors, key=lambda x: abs(x[0] - pn))
            diff = abs(best_pn - pn)
            if diff <= MAX_DIFF:
                df.at[idx, "_poly_idx"] = best_pi
                df.at[idx, "_match_method"] = f"4_manzana_d{diff}"
                n4 += 1
        df = df.drop(columns=["_predio_n"], errors="ignore")

    t4 = time.time()
    print(f"  [4] Manzana neighbor (d<={MAX_DIFF}): {n4} ({t4-t3:.1f}s)")

    # =================================================================
    # Método 5: getFeatureInfo para manzanas sin anchor
    # =================================================================
    n5 = 0
    still_unmatched = df[df["_poly_idx"].isna()]
    if len(still_unmatched) > 0 and nombre_wms:
        mz_matched = set(df[df["_poly_idx"].notna()]["manzana"].dropna().unique())
        mz_unmatched = set(still_unmatched["manzana"].dropna().unique())
        orphan_manzanas = mz_unmatched - mz_matched

        if orphan_manzanas:
            print(f"  [5] {len(orphan_manzanas)} manzanas sin anchor → getFeatureInfo...")
            used = set(df["_poly_idx"].dropna().astype(int).unique())
            unused_idxs = [i for i in range(len(gdf_poly)) if i not in used]

            fi_results = run_featureinfo_for_polygons(
                gdf_poly, unused_idxs, nombre_wms, workdir)

            if fi_results:
                fi_mz_lookup = {}
                for pi, rol in fi_results.items():
                    parts = rol.split("-")
                    if len(parts) == 2:
                        mz_padded = parts[0]
                        pn = int(parts[1])
                        fi_mz_lookup.setdefault(mz_padded, []).append((pn, pi))

                df["_predio_n"] = pd.to_numeric(df.get("predio"), errors="coerce")
                for idx in df[df["_poly_idx"].isna()].index:
                    mz = df.at[idx, "manzana"]
                    pn = df.at[idx, "_predio_n"]
                    if not mz or pd.isna(pn):
                        continue
                    neighbors = fi_mz_lookup.get(mz, [])
                    if not neighbors:
                        continue
                    pn = int(pn)
                    best_pn, best_pi = min(neighbors,
                                           key=lambda x: abs(x[0] - pn))
                    df.at[idx, "_poly_idx"] = best_pi
                    df.at[idx, "_match_method"] = f"5_fi_manzana_d{abs(best_pn - pn)}"
                    n5 += 1
                df = df.drop(columns=["_predio_n"], errors="ignore")
                print(f"       FI: {len(fi_results)} roles → {n5} predios")
    t5 = time.time()
    if nombre_wms:
        print(f"  [5] getFeatureInfo: {n5} ({t5-t4:.1f}s)")

    # =================================================================
    # Método 6: Fallback (nearest sin límite + manzana any)
    # =================================================================
    n6 = 0
    still_unmatched = df[df["_poly_idx"].isna()]

    # 6a: nearest sin límite para los que tienen coords
    has_x = "_x3857" in df.columns
    if has_x and not still_unmatched.empty:
        with_coords = still_unmatched.index[
            df.loc[still_unmatched.index].apply(
                lambda r: pd.notna(r.get("_x3857")), axis=1) if has_x
            else pd.Series(False, index=still_unmatched.index)]
        # Build points for remaining
        for idx in with_coords:
            if pd.isna(df.at[idx, "_x3857"]):
                continue
            pt_coords = gpd.GeoDataFrame(
                {"_orig_idx": [idx]},
                geometry=[Point(df.at[idx, "_x3857"], df.at[idx, "_y3857"])],
                crs="EPSG:3857")
            near = gpd.sjoin_nearest(pt_coords, gdf_poly[["geometry", "_poly_idx"]],
                                     how="inner", max_distance=500)
            if not near.empty:
                df.at[idx, "_poly_idx"] = near.iloc[0]["_poly_idx"]
                df.at[idx, "_match_method"] = "6_fallback_nearest"
                n6 += 1

    # 6b: manzana any para sin coords
    for idx in df[df["_poly_idx"].isna()].index:
        if "manzana" in df.columns:
            mz = df.at[idx, "manzana"]
            if mz and mz in mz_lookup:
                _, best_pi = mz_lookup[mz][0]
                df.at[idx, "_poly_idx"] = best_pi
                df.at[idx, "_match_method"] = "6_manzana_any"
                n6 += 1

    t6 = time.time()
    print(f"  [6] Fallback: {n6} ({t6-t5:.1f}s)")

    # =================================================================
    # Asignar geometría (vectorizado)
    # =================================================================
    total_matched = df["_poly_idx"].notna().sum()
    print(f"\n  TOTAL: {total_matched}/{n} ({total_matched/n*100:.1f}%)")
    still = n - total_matched
    if still > 0:
        print(f"  Sin match: {still}")

    # Vectorized geometry assignment via index lookup
    poly_area = gdf_poly["area_m2"].values
    df["pol_area_m2"] = df["_poly_idx"].apply(
        lambda pi: poly_area[int(pi)] if pd.notna(pi) else None)

    # Build geometry column: map poly_idx → geometry, then convert CRS
    pi_series = df["_poly_idx"]
    geom_list = [gdf_poly.iloc[int(pi)].geometry if pd.notna(pi) else None
                 for pi in pi_series]
    df["geometry"] = geom_list

    # Convert matched geometries from 3857 → 4326 in bulk
    has_geom = df["geometry"].notna()
    if has_geom.any():
        gdf_matched = gpd.GeoDataFrame(
            {"_i": df[has_geom].index},
            geometry=df.loc[has_geom, "geometry"].tolist(),
            crs="EPSG:3857").to_crs("EPSG:4326")
        df.loc[has_geom, "geometry"] = gdf_matched["geometry"].values

    df = df.drop(columns=["_orig_idx"], errors="ignore")
    return df


# ---------------------------------------------------------------------------
# Agregar polígonos huérfanos (sin predio) al output — vectorizado
# ---------------------------------------------------------------------------
def append_orphan_polygons(df, gdf_poly):
    """Agrega polígonos sin predio como filas extra con geometría pero sin datos."""
    used = set(df["_poly_idx"].dropna().astype(int).unique())
    orphan_mask = ~gdf_poly["_poly_idx"].isin(used)

    if not orphan_mask.any():
        return df, 0

    orphans = gdf_poly[orphan_mask][["geometry", "area_m2", "_poly_idx"]].copy()
    orphans = orphans.rename(columns={"area_m2": "pol_area_m2"})
    orphans["_match_method"] = "orphan_polygon"

    # Convert to 4326 in bulk
    orphans_gdf = gpd.GeoDataFrame(orphans, geometry="geometry",
                                    crs="EPSG:3857").to_crs("EPSG:4326")
    # Convert df to GeoDataFrame with same CRS before concat
    df_gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
    df_combined = pd.concat([df_gdf, orphans_gdf], ignore_index=True)
    return df_combined, len(orphans_gdf)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--csv", required=True, help="F0 CSV path")
    parser.add_argument("--gpkg", required=True, help="F2 GPKG path")
    parser.add_argument("--output", required=True, help="Output directory")
    parser.add_argument("--cod", required=True, help="Código comuna")
    parser.add_argument("--nombre", default=None,
                        help="Nombre WMS (para getFeatureInfo de manzanas huérfanas)")
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)

    print(f"=== Join mejorado — comuna {args.cod} ===")
    print()

    print("Cargando F0 CSV...")
    df = load_predios(args.csv)
    print(f"  {len(df)} predios, {df['_lat'].notna().sum()} con coords")

    print("Cargando F2 GPKG...")
    gdf_poly = load_polygons(args.gpkg)
    print(f"  {len(gdf_poly)} polígonos")
    print()

    # Join
    print("Ejecutando join...")
    df = join_predios_to_polygons(df, gdf_poly, args.nombre, args.output)
    print()

    # Agregar polígonos huérfanos
    df, n_orphans = append_orphan_polygons(df, gdf_poly)
    print(f"Polígonos huérfanos agregados: {n_orphans}")
    print()

    # Distribución por método
    print("Distribución:")
    methods = df["_match_method"].value_counts()
    for m, c in methods.head(15).items():
        print(f"  {m}: {c}")
    remainder = len(methods) - 15
    if remainder > 0:
        print(f"  ... y {remainder} métodos más")
    sin = df["_match_method"].isna().sum()
    if sin > 0:
        print(f"  sin match: {sin}")
    print()

    # Stats
    predios = df[df["_match_method"] != "orphan_polygon"]
    if "ubicacion" in predios.columns:
        urb = predios[predios["ubicacion"].str.contains("URBANA", na=False)]
        urb_con = urb["pol_area_m2"].notna().sum()
        print(f"URBANA: {urb_con}/{len(urb)} ({urb_con/len(urb)*100:.1f}%)")

    total_con = predios["pol_area_m2"].notna().sum()
    print(f"Total: {total_con}/{len(predios)} ({total_con/len(predios)*100:.1f}%)")
    print(f"Filas output: {len(df)} ({len(predios)} predios + {n_orphans} polígonos huérfanos)")
    print()

    # Guardar
    out_csv = os.path.join(args.output, f"comuna={args.cod}.csv")
    out_gpkg = os.path.join(args.output, f"comuna={args.cod}.gpkg")

    drop_cols = ["geometry", "_lat", "_lon", "_x3857", "_y3857",
                 "_poly_idx", "_match_method"]
    df.drop(
        columns=[c for c in drop_cols if c in df.columns], errors="ignore"
    ).to_csv(out_csv, index=False)
    print(f"CSV: {out_csv}")

    gdf_out = gpd.GeoDataFrame(
        df.drop(columns=["_lat", "_lon", "_x3857", "_y3857"], errors="ignore"),
        geometry="geometry", crs="EPSG:4326")
    gdf_out.to_file(out_gpkg, driver="GPKG", layer="predios")
    print(f"GPKG: {out_gpkg}")


if __name__ == "__main__":
    main()
