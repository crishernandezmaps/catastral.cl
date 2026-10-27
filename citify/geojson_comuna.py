"""GeoJSON de una comuna desde la base 2026S2 completa (todas las columnas, nulos omitidos, coordenadas a 6 decimales).
Uso (en la .33): python3 geojson_comuna.py <cod_SII[,cod2,...]> <salida.geojson> <Nombre comuna>"""
import duckdb, json, sys, math, datetime, decimal, os
cod, out = ','.join(str(int(c)) for c in sys.argv[1].split(',')), sys.argv[2]  # varios códigos SII = una comuna Citify (Santiago)
NOMBRE = sys.argv[3]  # nombre de la comuna tal como lo muestra Citify: su visor exige 'comuna' como texto
TOL = 0.0
U = '/opt/pse3/data/predios_2026s2/chile_predios_2026s2_completa.parquet'
con = duckdb.connect(); con.sql("load spatial")
FUERA = {'geometry', 'bbox_min_lon', 'bbox_max_lon', 'bbox_min_lat', 'bbox_max_lat', 'id_prev', 'tabla_origen'}
# LIVIANO=1: para comunas que superan el tope de texto de Chrome (~536 MB; Citify da «Error al procesar el archivo»).
# Quita columnas que repiten otra (difieren en <0,01 % de las filas) y deja coordenadas a 5 decimales (~1 m).
LIVIANO = os.environ.get('LIVIANO') == '1'
if LIVIANO:
    FUERA |= {'sup_terreno', 'dc_sup_terreno', 'valor_total', 'dc_avaluo_fiscal', 'valor_afecto', 'valor_exento',
              'dc_avaluo_exento', 'direccion_sii', 'dc_direccion', 'destino_desc', 'dc_contribucion_semestral'}
# MINIMO=1: solo los atributos que muestra el tooltip de Citify + llaves (para comunas que ni en LIVIANO bajan de ~500 MB)
MINIMO = os.environ.get('MINIMO') == '1'
SOLO = {'rol', 'direccion', 'destino_descripcion', 'm2_terreno', 'sup_construida_total', 'pisos_max', 'anio_construccion_max',
        'materiales', 'avaluo_total', 'contrib_sem', 'val_com_uf', 'cod_comuna', 'manzana', 'predio', 'poligono_id', 'lat', 'lon'}
DEC = 5 if (LIVIANO or MINIMO) else 6
# TRAMOS=1: agrega `tramo_avaluo` (texto ordenable) para pintar por avalúo fiscal en Citify («Por valores de atributo»)
TRAMOS = os.environ.get('TRAMOS') == '1'
CORTES = [(10e6, '1 · Menos de $10 millones'), (25e6, '2 · $10 a $25 millones'), (40e6, '3 · $25 a $40 millones'),
          (60e6, '4 · $40 a $60 millones'), (100e6, '5 · $60 a $100 millones'), (300e6, '6 · $100 a $300 millones')]
def tramo(v):
    if not v: return 'Sin avalúo informado'
    for tope, etq in CORTES:
        if v < tope: return etq
    return '7 · Más de $300 millones'
COORD = {'lat', 'lon'}
cols = [c[0] for c in con.sql(f"describe select * from '{U}'").fetchall() if c[0] not in FUERA]
if MINIMO: cols = [c for c in cols if c in SOLO]
# Citify arma la lista de atributos del tooltip mirando el primer predio de cada archivo: va primero el más completo.
TOOLTIP = ['rol', 'direccion', 'destino_descripcion', 'm2_terreno', 'sup_construida_total', 'pisos_max', 'anio_construccion_max', 'materiales', 'avaluo_total', 'contrib_sem', 'val_com_uf']
ORDEN = ' + '.join(f'("{c}" is not null)::int' for c in TOOLTIP)
rel = con.sql(f"select {', '.join(chr(34)+c+chr(34) for c in cols)}, ST_AsGeoJSON(ST_GeomFromWKB(geometry)) gj from '{U}' where comuna in ({cod}) and geometry is not null order by {ORDEN} desc")
def limpia(v, k=''):
    if isinstance(v, float):
        return None if math.isnan(v) else round(v, DEC if k in COORD else 2)
    if isinstance(v, (datetime.date, datetime.datetime)): return v.isoformat()
    if isinstance(v, decimal.Decimal): return float(v)
    if isinstance(v, bytes): return None
    return v
def r6(c):
    return [r6(x) for x in c] if isinstance(c[0], list) else [round(c[0], DEC), round(c[1], DEC)]
n = 0
with open(out, 'w') as f:
    f.write('{"type":"FeatureCollection","features":[')
    for row in rel.fetchall():
        props = {k: limpia(v, k) for k, v in zip(cols, row[:-1]) if v is not None and limpia(v, k) is not None}
        props['comuna'] = NOMBRE
        if TRAMOS: props['tramo_avaluo'] = tramo(props.get('avaluo_total'))  # para pintar por avalúo fiscal (demo)
        if MINIMO: props['version_datos'] = 'Liviana: solo los atributos visibles'  # leyenda en el tooltip de Citify
        g = json.loads(row[-1]); g['coordinates'] = r6(g['coordinates'])
        f.write((',' if n else '') + json.dumps({'type': 'Feature', 'properties': props, 'geometry': g}, ensure_ascii=False, separators=(',', ':')))
        n += 1
    f.write(']}')
tot = con.sql(f"select count(*), count(geometry) from '{U}' where comuna in ({cod})").fetchone()
print('features', n, 'filas_comuna', tot[0], 'con_geom', tot[1])
