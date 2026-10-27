"""Paquete de entrega 2026S2 de una o más comunas (corre en la .33). GeoParquet con metadato `geo` (EPSG:4326, WKB)
+ CSV (separador |, con lat/lon, sin geometría) + diccionario, en un ZIP subido a s3://siipredios/licencias/entregas/<ID>/
y presignado por 7 días. Uso: python3 arma_entrega.py <ID> <nombre_zip> <cod>:<Nombre> [<cod>:<Nombre> ...]"""
import json, os, subprocess, sys, zipfile
import duckdb, pyarrow.parquet as pq
U = '/opt/pse3/data/predios_2026s2/chile_predios_2026s2_completa.parquet'
ID, ZIPN, COMUNAS = sys.argv[1], sys.argv[2], [a.split(':', 1) for a in sys.argv[3:]]
TMP = f'/root/tmp_entrega_{ID}'; os.makedirs(TMP, exist_ok=True)
con = duckdb.connect(); con.sql('load spatial')
archivos = []
for cod, nom in COMUNAS:
    base = f"{nom.replace(' ', '_')}_{cod}_2026S2"
    t = con.sql(f"select * exclude (geometry), geometry from '{U}' where comuna={int(cod)}").arrow()
    t = t.read_all() if hasattr(t, "read_all") else t
    geo = {"version": "1.0.0", "primary_column": "geometry",
           "columns": {"geometry": {"encoding": "WKB", "geometry_types": [], "crs": "EPSG:4326"}}}
    t = t.replace_schema_metadata({**(t.schema.metadata or {}), b'geo': json.dumps(geo).encode()})
    pq.write_table(t, f'{TMP}/{base}.parquet', compression='zstd')
    con.sql(f"copy (select * exclude (geometry) from '{U}' where comuna={int(cod)}) to '{TMP}/{base}.csv' (header, delimiter '|')")
    n = con.sql(f"select count(*), count(geometry), count(*) filter (where ubicacion='URBANA') from '{U}' where comuna={int(cod)}").fetchone()
    print(cod, nom, 'filas', n[0], 'con_pol', n[1], 'urbanas', n[2], flush=True)
    archivos += [f'{base}.parquet', f'{base}.csv']
archivos.append('Diccionario_de_variables_Catastral_2026S2.pdf')
with zipfile.ZipFile(f'{TMP}/{ZIPN}', 'w', zipfile.ZIP_DEFLATED) as z:
    for a in archivos: z.write(f'{TMP}/{a}', a)
key = f's3://siipredios/licencias/entregas/{ID}/{ZIPN}'
subprocess.run(['aws', '--profile', '18o', 's3', 'cp', f'{TMP}/{ZIPN}', key, '--content-type', 'application/zip', '--quiet'], check=True)
url = subprocess.run(['aws', '--profile', '18o', 's3', 'presign', key, '--expires-in', '604800'], capture_output=True, text=True, check=True).stdout.strip()
open(f'{TMP}/link.txt', 'w').write(url)
print('zip MB', round(os.path.getsize(f'{TMP}/{ZIPN}') / 1e6, 1), key)
