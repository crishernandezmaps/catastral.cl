"""Paquetes de la tienda en versión 2026S2 (corre en la .33). Una pasada sobre chile_predios_2026s2_completa.parquet:
por cada código SII del catálogo de la tienda escribe comuna=<cod>.parquet (zstd, metadato geo EPSG:4326) y
comuna=<cod>.csv (separador |, sin geometría), los sube a s3://siipredios/licencias/base_2026s2/<Nombre>_<cod>/ y arma
comunas_2026s2.json con el mismo formato que comunas_z16.json. No toca licencias/base_z16 (2026S1).
Uso: python3 genera_base_2026s2.py comunas_z16.json [--subir]"""
import json, os, subprocess, sys
import duckdb, pyarrow.parquet as pq
U = '/opt/pse3/data/predios_2026s2/chile_predios_2026s2_completa.parquet'
OUT = '/root/base_2026s2'; PREF = 's3://siipredios/licencias/base_2026s2'
GEO = json.dumps({"version": "1.0.0", "primary_column": "geometry",
                  "columns": {"geometry": {"encoding": "WKB", "geometry_types": [], "crs": "EPSG:4326"}}}).encode()
cat = json.load(open(sys.argv[1]))
con = duckdb.connect(); con.sql("set threads=8")
os.makedirs(OUT, exist_ok=True)
print('particionando…', flush=True)
con.sql(f"copy (select * from '{U}') to '{OUT}/_part_pq' (format parquet, partition_by (comuna), overwrite_or_ignore, compression zstd)")
con.sql(f"copy (select * exclude (geometry) from '{U}') to '{OUT}/_part_csv' (format csv, delimiter '|', header, partition_by (comuna), overwrite_or_ignore)")
nuevo, faltan = [], []
cuentas = dict(con.sql(f"select comuna::varchar, count(*) from '{U}' group by 1").fetchall())
for c in cat:
    cod = c['id']; carpeta = f"{c['nombre'].replace(' ', '_')}_{cod}"
    src_pq = f"{OUT}/_part_pq/comuna={cod}"; src_csv = f"{OUT}/_part_csv/comuna={cod}"
    if not os.path.isdir(src_pq):
        faltan.append(cod); continue
    os.makedirs(f"{OUT}/{carpeta}", exist_ok=True)
    t = pq.read_table(src_pq)                     # la partición no trae la columna comuna: se repone
    if 'comuna' not in t.schema.names:
        import pyarrow as pa
        t = t.append_column('comuna', pa.array([int(cod)] * t.num_rows, pa.int32()))
        orden = ['comuna'] + [n for n in t.schema.names if n != 'comuna']; t = t.select(orden)
    t = t.replace_schema_metadata({**(t.schema.metadata or {}), b'geo': GEO})
    pq.write_table(t, f"{OUT}/{carpeta}/comuna={cod}.parquet", compression='zstd')
    # CSV: la partición tampoco trae comuna; se antepone
    with open(f"{OUT}/{carpeta}/comuna={cod}.csv", 'w') as fo:
        for f in sorted(os.listdir(src_csv)):
            with open(f"{src_csv}/{f}") as fi:
                h = fi.readline()
                if fo.tell() == 0: fo.write('comuna|' + h)
                for linea in fi: fo.write(f"{cod}|{linea}")
    nuevo.append({**c, 'parquet': f"{PREF}/{carpeta}/comuna={cod}.parquet", 'csv': f"{PREF}/{carpeta}/comuna={cod}.csv",
                  'total_predios': cuentas.get(cod, 0), 'periodo': '2026S2'})
json.dump(nuevo, open(f"{OUT}/comunas_2026s2.json", 'w'), ensure_ascii=False, indent=1)
print('comunas', len(nuevo), 'faltan', faltan, flush=True)
if '--subir' in sys.argv:
    subprocess.run(['aws', '--profile', '18o', 's3', 'sync', OUT, PREF, '--exclude', '_part_*', '--exclude', 'comunas_2026s2.json', '--only-show-errors'], check=True)
    print('subido', flush=True)
