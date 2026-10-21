"""Sube a Citify desde la .33 las comunas que faltan, usando el Chrome de la VPS (CDP 127.0.0.1:9222, sesión iniciada
por Cris vía noVNC) y el mismo cargador del Mac (_cargador.js). Los archivos se sirven desde /root/tmp_citify por
127.0.0.1:8765. Registro en subidas.txt (mismo formato que en el Mac). Uso: python3 conductor_vps.py [MB_max]"""
import json, os, sys, time
from playwright.sync_api import sync_playwright
AQUI = os.path.dirname(os.path.abspath(__file__)); DATOS = '/root/tmp_citify'
MB = float(sys.argv[1]) if len(sys.argv) > 1 else 800
# Comunas a saltar (coma): p. ej. las que superan el tope de ~536 MB de texto de Chrome y requieren versión aligerada
SALTAR = set(filter(None, os.environ.get('SALTAR', '').split(',')))
MAN = json.load(open(os.path.join(AQUI, 'manifiesto.json')))
REG = os.path.join(AQUI, 'subidas.txt')
PORT = {'region': 'Valparaíso', 'comuna': 'Panquehue', 'archivo': 'Portador_Panquehue.geojson'}
CARGADOR = open(os.path.join(AQUI, '_cargador.js')).read()
def log(*a): print(time.strftime('%H:%M:%S'), *a, flush=True)
def ya(): return {x for x in open(REG).read().split('\n') if x}
def tandas():
    hechas = ya(); t, mb = [], 0
    for it in MAN:
        if it['comuna'] in hechas or it['comuna'] in SALTAR: continue
        s = os.path.getsize(os.path.join(DATOS, it['archivo'])) / 1e6
        if t and mb + s > MB:
            yield t; t, mb = [], 0
        t.append(it); mb += s
    if t: yield t
ABRE = """async()=>{const s=ms=>window.__wsleep(ms);
 // tras guardar, el diálogo anterior tarda en cerrarse: esperar a que desaparezca antes de abrir uno nuevo
 for(let w=0;w<15&&document.querySelector('[role=dialog]');w++) await s(1000);
 if(!document.querySelector('[role=dialog]')){const ic=document.querySelector('tr svg[data-testid=EditIcon]');
  const k=Object.keys(ic).find(k=>k.startsWith('__reactProps'));ic[k].onClick({stopPropagation(){},preventDefault(){},currentTarget:ic,target:ic});}
 for(let w=0;w<60&&!document.querySelector('[role=dialog]');w++) await s(1000); await s(3000); return !!document.querySelector('[role=dialog]')}"""
class SinSesion(Exception): pass
def prepara(page, recargar=False):
    # Recarga completa solo para liberar memoria; la navegación va por el router de Next (no por URL).
    if recargar: page.reload(); time.sleep(8)
    if '/landing' in page.url or 'auth0' in page.url: raise SinSesion(page.url)
    page.evaluate("()=>window.next.router.push('/plataforma/capas')")
    page.wait_for_selector('tr svg[data-testid=EditIcon]', timeout=120000)
    page.evaluate(CARGADOR); time.sleep(2)
with sync_playwright() as p:
    br = p.chromium.connect_over_cdp('http://127.0.0.1:9222')
    page = next(pg for c in br.contexts for pg in c.pages if 'citify' in pg.url)
    page.set_default_timeout(0)
    prepara(page, recargar=True); n = 0  # arranque limpio: descarta diálogos que hayan quedado abiertos
    try:
     for t in tandas():
         lista = [{k: it[k] for k in ('region', 'comuna', 'archivo')} for it in t] + [PORT]
         for intento in range(3):
             if (n and n % 3 == 0 and intento == 0) or intento: prepara(page, recargar=True)  # libera memoria
             if not page.evaluate(ABRE): log('no abre el editor'); continue
             page.evaluate('l=>window.__subir(l)', lista)
             e = page.evaluate('()=>window.__est')
             log('tanda', [x['comuna'] for x in t], e['fase'], f"{len(e['hechos'])}/{len(lista)}", f"{e.get('seg')}s", e.get('err') or '', e.get('atr'))
             if e['fase'] == 'listo' and len(e['hechos']) == len(lista):
                 with open(REG, 'a') as f:
                     for it in t: f.write(it['comuna'] + '\n')
                 n += 1; break
         else:
             log('FALLA tras 3 intentos; me detengo'); break
    except SinSesion as e:
        log('SESIÓN PERDIDA en Citify (', e, ') — hay que volver a iniciar sesión en noVNC')
    log('FIN', len(ya()), 'de', len(MAN))
