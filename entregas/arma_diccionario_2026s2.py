"""Diccionario de variables de la compra por comuna, versión 2026S2 (130 columnas de chile_predios_2026s2_completa).
Contenido: ../ags_valparaiso-vina_2026-10-15/dic.html (genérico). Uso: python3 arma_diccionario.py"""
import os
from weasyprint import HTML
AQUI = os.path.dirname(os.path.abspath(__file__)); BASE = '/Users/newmarkchile/Documents/TREMEN/5_PROJECTS/CATASTRAL'
LOGO = open(f'{BASE}/catastralV2/frontend/public/logo-catastral.svg').read().replace('stroke="#0f172a"', 'stroke="#ffffff"')
DIC = open(os.path.join(AQUI, '..', 'ags_valparaiso-vina_2026-10-15', 'dic.html')).read().replace('`', '')
html = f"""<!DOCTYPE html><html lang="es"><head><meta charset="utf-8"><title>Diccionario de variables — Catastral.cl 2026S2</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:opsz,wght@9..40,400;9..40,600;9..40,700&family=DM+Serif+Display&display=swap" rel="stylesheet">
<style>
@page {{ size: A4; margin: 14mm 14mm 16mm;
  @bottom-left {{ content: "Catastral.cl · Diccionario de variables · Catastro 2026S2"; font: 7.5pt 'DM Sans'; color: #999; }}
  @bottom-right {{ content: counter(page) " / " counter(pages); font: 7.5pt 'DM Sans'; color: #999; }} }}
@page :first {{ margin-top: 0; margin-left: 0; margin-right: 0; }}
body {{ font-family: 'DM Sans', sans-serif; font-size: 9pt; line-height: 1.45; margin: 0; color: #000; }}
.top {{ background: #0f172a; color: #fff; padding: 9mm 14mm 7mm; }}
.brand {{ display: flex; align-items: center; gap: 3mm; }} .brand svg {{ width: 11mm; height: 11mm; }}
.brand span {{ font-weight: 600; font-size: 13pt; letter-spacing: .04em; }} .brand b {{ color: #bafb00; font-weight: 600; }}
.kicker {{ display: inline-block; background: #bafb00; color: #000; font-size: 7.3pt; font-weight: 700; letter-spacing: .08em;
  text-transform: uppercase; padding: 2px 9px; border-radius: 99px; margin: 5mm 0 2.5mm; }}
h1 {{ font-family: 'DM Serif Display', serif; font-weight: 400; font-size: 21pt; margin: 0; }} h1 em {{ font-style: normal; color: #bafb00; }}
.top p {{ color: #c9c9c9; margin: 3mm 0 0; font-size: 9.4pt; }}
.wrap {{ padding: 4mm 14mm 0; }}
h3 {{ font-size: 10pt; font-weight: 700; margin: 4.5mm 0 1.2mm; break-after: avoid; }}
table {{ width: 100%; border-collapse: collapse; font-size: 8.3pt; }}
thead th {{ text-align: left; font-weight: 600; font-size: 7.2pt; text-transform: uppercase; letter-spacing: .06em; color: #555;
  padding: 0 5px 4px; border-bottom: 1.5px solid #000; }}
tbody td {{ padding: 3px 5px; border-bottom: 1px solid #e0e0e0; vertical-align: top; }} tr {{ break-inside: avoid; }}
td:first-child {{ width: 42%; }} code {{ font-size: 7.6pt; background: #f4f4f4; padding: 0 3px; border-radius: 3px; }}
</style></head><body>
<div class="top"><div class="brand">{LOGO}<span>CATASTRAL<b>.CL</b></span></div>
<span class="kicker">Diccionario de variables · Catastro 2026S2</span>
<h1>Qué trae cada <em>comuna</em></h1>
<p>Variables de la división predial que se entrega por comuna (GeoParquet y CSV): cada rol del SII con el polígono de su lote y su ficha,
segundo semestre de 2026.</p></div>
<div class="wrap">{DIC}</div></body></html>"""
HTML(string=html, base_url=AQUI).write_pdf(os.path.join(AQUI, 'diccionario_variables_catastral_2026S2.pdf'))
print('ok')
