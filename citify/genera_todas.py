"""Genera en la .33 los GeoJSON de todas las comunas de manifiesto.json (de menor a mayor), salvo las ya subidas.
Uso: nohup python3 genera_todas.py > genera.log 2>&1 &"""
import json, os, subprocess
AQUI = os.path.dirname(os.path.abspath(__file__)); OUT = '/root/tmp_citify'
os.makedirs(OUT, exist_ok=True)
YA = {'Panquehue', 'San Joaquín'}
for it in json.load(open(os.path.join(AQUI, 'manifiesto.json'))):
    f = os.path.join(OUT, it['archivo'])
    if it['comuna'] in YA or os.path.exists(f + '.ok'):
        continue
    r = subprocess.run(['python3', os.path.join(AQUI, 'geojson_comuna.py'), ','.join(map(str, it['cods'])), f, it['comuna']], capture_output=True, text=True)
    if r.returncode == 0:
        open(f + '.ok', 'w').write(r.stdout)
    print(it['comuna'], r.returncode, r.stdout.strip(), r.stderr.strip()[-300:], flush=True)
