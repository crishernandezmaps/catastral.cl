"""Lista de precios de Catastral.cl: venta de comunas sueltas, precio por tamaño y descuentos por cantidad.
Fuente única: https://catastral.cl/app/api/ventas/precios (lo mismo que cobra el checkout /comprar).
Nombres y regiones: catastralV2/backend/data/comunas_v2.json (códigos SII). Uso: python3 arma_pdf.py"""
import json, os, urllib.request
from datetime import date
from weasyprint import HTML

AQUI = os.path.dirname(os.path.abspath(__file__))
BASE = '/Users/newmarkchile/Documents/TREMEN/5_PROJECTS/CATASTRAL'
P = json.load(urllib.request.urlopen(urllib.request.Request('https://catastral.cl/app/api/ventas/precios', headers={'User-Agent': 'catastral.cl-entregas/1.0'}), timeout=30))
json.dump(P, open(os.path.join(AQUI, 'precios_fuente.json'), 'w'), ensure_ascii=False, indent=1)
CAT = json.load(open(f'{BASE}/catastralV2/backend/data/comunas_v2.json'))
LOGO = open(f'{BASE}/catastralV2/frontend/public/logo-catastral.svg').read().replace('stroke="#0f172a"', 'stroke="#ffffff"')

UF, IVA, TOPE = P['uf_dia'], 1.19, P['tope']
TIER = {t['tier']: t for t in P['precios_tamano']}
DESC = P['descuentos']
HOY = date.today()
REGIONES = ['Arica y Parinacota', 'Tarapacá', 'Antofagasta', 'Atacama', 'Coquimbo', 'Valparaíso', 'Metropolitana',
            "O'Higgins", 'Maule', 'Ñuble', 'Biobío', 'Araucanía', 'Los Ríos', 'Los Lagos', 'Aysén', 'Magallanes']


def uf(x):  return f"{x:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.').rstrip('0').rstrip(',')
def clp(x): return '$' + f"{round(x):,}".replace(',', '.')
def pct(n):
    for d in DESC:
        if d['desde'] <= n <= d['hasta']: return d['pct']
    return 0


def ejemplo(etq, mezcla):
    n = sum(mezcla.values()); bruto = sum(TIER[t]['precio_uf'] * k for t, k in mezcla.items())
    neto = bruto * (1 - pct(n) / 100)
    return (f"<tr><td>{etq}</td><td class='num'>{n}</td><td class='num'>{uf(bruto)} UF</td><td class='num'>{pct(n)} %</td>"
            f"<td class='num'><b>{uf(neto)} UF</b></td><td class='num'>{clp(neto * UF * IVA)}</td></tr>")


filas_tam = ''.join(f"<tr><td><span class='tier t{t}'>{TIER[t]['nombre'].capitalize()}</span></td><td>{TIER[t]['umbral'].replace('mas', 'más')}</td>"
                    f"<td class='num'>{sum(1 for c in CAT if P['tiers'][c['id']] == t)}</td>"
                    f"<td class='num'><b>{uf(TIER[t]['precio_uf'])} UF</b></td><td class='num'>{clp(TIER[t]['precio_uf'] * UF * IVA)}</td></tr>" for t in (1, 2, 3))
filas_desc = "<tr><td>1 a 2</td><td class='num'>—</td></tr>" + ''.join(
    f"<tr><td>{d['desde']} a {d['hasta']}</td><td class='num'><b>{d['pct']} %</b></td></tr>" for d in DESC)
ejemplos = ''.join([
    ejemplo('1 comuna grande', {3: 1}),
    ejemplo('3 comunas chicas', {1: 3}),
    ejemplo('2 grandes y 3 medianas', {3: 2, 2: 3}),
    ejemplo('10 comunas grandes', {3: 10}),
    ejemplo('33 comunas: 10 grandes, 13 medianas y 10 chicas', {3: 10, 2: 13, 1: 10}),
    ejemplo(f'{TOPE} comunas grandes (tope por compra)', {3: TOPE}),
])

anexo = ''
for r in REGIONES:
    cs = sorted((c for c in CAT if c['region'] == r), key=lambda c: c['nombre'])
    filas = ''.join(f"<tr><td>{c['nombre']}</td><td class='c'>{c['id']}</td><td><span class='tier t{P['tiers'][c['id']]}'>{TIER[P['tiers'][c['id']]]['nombre']}</span></td>"
                    f"<td class='num'>{uf(TIER[P['tiers'][c['id']]]['precio_uf'])} UF</td>"
                    f"<td class='num'>{clp(TIER[P['tiers'][c['id']]]['precio_uf'] * UF * IVA)}</td></tr>" for c in cs)
    anexo += f"<div class='reg'><h3>{r} <span>{len(cs)} comunas</span></h3><table><thead><tr><th>Comuna</th><th>Cód. SII</th><th>Tamaño</th><th class='num'>Neto</th><th class='num'>Con IVA</th></tr></thead><tbody>{filas}</tbody></table></div>"

fecha = f"{HOY.day} de {['enero','febrero','marzo','abril','mayo','junio','julio','agosto','septiembre','octubre','noviembre','diciembre'][HOY.month-1]} de {HOY.year}"
html = f"""<!DOCTYPE html><html lang="es"><head><meta charset="utf-8"><title>Lista de precios — Catastral.cl</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:opsz,wght@9..40,400;9..40,500;9..40,600;9..40,700&family=DM+Serif+Display&display=swap" rel="stylesheet">
<style>
:root {{ --ink:#000; --muted:#555; --line:#e0e0e0; --bg2:#f4f4f4; --lime:#bafb00; --dark:#0f172a; }}
@page {{ size: A4; margin: 14mm 14mm 16mm;
  @bottom-left {{ content: "Catastral.cl · Lista de precios · {fecha}"; font: 7.5pt 'DM Sans'; color: #999; }}
  @bottom-right {{ content: counter(page) " / " counter(pages); font: 7.5pt 'DM Sans'; color: #999; }} }}
@page :first {{ margin-top: 0; margin-left: 0; margin-right: 0; }}
* {{ box-sizing: border-box; }}
body {{ font-family: 'DM Sans', sans-serif; color: var(--ink); font-size: 9.4pt; line-height: 1.48; margin: 0; }}
.top {{ background: var(--dark); color: #fff; padding: 9mm 14mm 7mm; }}
.band {{ display: flex; justify-content: space-between; align-items: flex-start; }}
.brand {{ display: flex; align-items: center; gap: 3mm; }} .brand svg {{ width: 11mm; height: 11mm; }}
.brand span {{ font-weight: 600; font-size: 13pt; letter-spacing: .04em; }} .brand b {{ color: var(--lime); font-weight: 600; }}
.meta {{ text-align: right; font-size: 8.3pt; color: #bbb; line-height: 1.6; }} .meta strong {{ color: #fff; }}
.kicker {{ display: inline-block; background: var(--lime); color: #000; font-size: 7.3pt; font-weight: 700; letter-spacing: .08em;
  text-transform: uppercase; padding: 2px 9px; border-radius: 99px; margin: 5mm 0 2.5mm; }}
h1 {{ font-family: 'DM Serif Display', serif; font-weight: 400; font-size: 22pt; line-height: 1.1; margin: 0; }} h1 em {{ font-style: normal; color: var(--lime); }}
.top p {{ color: #c9c9c9; margin: 3mm 0 0; max-width: 165mm; font-size: 9.6pt; }}
.wrap {{ padding: 5mm 14mm 0; }}
h2 {{ font-family: 'DM Serif Display', serif; font-weight: 400; font-size: 14pt; margin: 5mm 0 2mm; }}
h2.pb {{ page-break-before: always; margin-top: 0; }}
p {{ margin: 0 0 2.2mm; }} .nota {{ color: var(--muted); font-size: 8.3pt; }}
table {{ width: 100%; border-collapse: collapse; font-size: 8.5pt; margin: 0 0 2mm; }}
thead th {{ text-align: left; font-weight: 600; font-size: 7.2pt; text-transform: uppercase; letter-spacing: .06em; color: var(--muted);
  padding: 0 5px 4px; border-bottom: 1.5px solid #000; }}
tbody td {{ padding: 3px 5px; border-bottom: 1px solid var(--line); vertical-align: top; }} tr {{ break-inside: avoid; }}
.num {{ text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }} .c {{ color: var(--muted); font-variant-numeric: tabular-nums; }}
.tier {{ display: inline-block; font-size: 7.2pt; padding: 0 7px; border-radius: 99px; border: 1px solid var(--line); text-transform: capitalize; }}
.tier.t2 {{ background: var(--bg2); }} .tier.t3 {{ background: var(--lime); border-color: var(--lime); }}
.dos {{ display: flex; gap: 8mm; }} .dos > div {{ flex: 1; }}
.caja {{ background: var(--bg2); border-radius: 6px; padding: 3.5mm 4.5mm; margin: 3mm 0; }}
.caja b.l {{ display: inline-block; background: var(--lime); padding: 0 5px; }}
ul {{ margin: 0 0 2mm; padding-left: 4.5mm; }} li {{ margin-bottom: 1mm; }}
.anexo {{ columns: 2; column-gap: 7mm; }} .anexo table {{ font-size: 7.4pt; }} .anexo tbody td {{ padding: 1.6px 3px; }}
.reg {{ break-inside: auto; }} .reg h3 {{ font-size: 9pt; margin: 2.5mm 0 1mm; break-after: avoid; }} .reg h3 span {{ color: var(--muted); font-weight: 400; font-size: 7.5pt; }}
</style></head><body>
<div class="top"><div class="band"><div class="brand">{LOGO}<span>CATASTRAL<b>.CL</b></span></div>
<div class="meta">Lista de precios<br><strong>{fecha}</strong><br>Un proyecto de Tremen</div></div>
<span class="kicker">Venta de datos por comuna · Catastro 2026S2</span>
<h1>Precio por comuna, <em>según su tamaño</em></h1>
<p>Cada comuna se vende con su división predial completa: cada rol del SII con el polígono de su lote, urbano y rural, y su ficha
(destino, superficies, construcciones y avalúo), en la versión más reciente del catastro: <b>2026S2</b> (segundo semestre de 2026). Se puede comprar una sola comuna o varias; a partir de tres se aplica un descuento por cantidad.</p></div>

<div class="wrap">
<h2>Precio por tamaño de comuna</h2>
<table><thead><tr><th>Tamaño</th><th>Criterio</th><th class="num">Comunas</th><th class="num">Precio neto</th><th class="num">Con IVA (ref.)</th></tr></thead><tbody>{filas_tam}</tbody></table>
<p class="nota">Precios en UF + IVA. El valor en pesos es referencial, con la UF del día ({clp(UF)}) e IVA del 19 %; se cobra con la UF del día de pago.
El tamaño de cada comuna está en el anexo.</p>

<div class="dos"><div>
<h2>Descuento por cantidad</h2>
<table><thead><tr><th>Comunas en la compra</th><th class="num">Descuento</th></tr></thead><tbody>{filas_desc}</tbody></table>
<p class="nota">El descuento se aplica sobre el <b>total</b> de la compra (la suma de los precios por tamaño), no comuna a comuna. Tope: {TOPE} comunas por compra.</p>
</div><div>
<h2>Qué incluye</h2>
<ul><li>División predial completa de cada comuna, versión <b>2026S2</b>: un polígono por rol, con su ficha SII.</li>
<li>Formatos GeoParquet y CSV, con diccionario de variables.</li>
<li>Entrega el mismo día, por enlace de descarga.</li>
<li>Licencia de uso para la organización compradora.</li>
<li>Compra en línea en <b>catastral.cl/comprar</b>, con pago por Flow o transferencia.</li></ul>
</div></div>

<h2>Ejemplos de cálculo</h2>
<table><thead><tr><th>Compra</th><th class="num">Comunas</th><th class="num">Suma</th><th class="num">Desc.</th><th class="num">Total neto</th><th class="num">Con IVA (ref.)</th></tr></thead><tbody>{ejemplos}</tbody></table>

<div class="caja"><b class="l">Más de {TOPE} comunas o todo Chile</b> &nbsp;Suscripción nacional: acceso a todas las comunas del país en nuestra plataforma,
con todas sus capas, descarga de comunas, actualización semestral y soporte, por <b>20 UF + IVA al mes</b>, con un plazo mínimo de 3 meses.
Para cotizaciones especiales, escríbanos a <b>cris@catastral.cl</b> · +56 9 3178 3512.</div>

<h2>Anexo: tamaño y precio de cada comuna</h2>
<p class="nota">Precio de una comuna comprada sola. En compras de 3 o más comunas se aplica la escala de descuento sobre el total.</p>
<div class="anexo">{anexo}</div>
</div></body></html>"""
open(os.path.join(AQUI, 'lista_precios.html'), 'w').write(html)
HTML(string=html, base_url=AQUI).write_pdf(os.path.join(AQUI, 'lista_precios_catastral.pdf'))
print('ok', UF, len(CAT))
