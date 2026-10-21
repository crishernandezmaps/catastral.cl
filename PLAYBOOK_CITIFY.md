# Playbook — subir la capa predial a Citify

_Proceso oficial desde el 2026-10-20. Scripts en [`citify/`](citify/)._

Citify (app.citify.cl) es **revendedor**: publica nuestra capa predial SII en su GeoMarket.
La carga se hace por su interfaz web: editor de la capa, un archivo GeoJSON por comuna. Citify no
tiene API de carga. **Todo corre en la VPS `46.224.221.33`**; el Mac solo sirve para iniciar sesión
una vez por pantalla remota. Subir desde el navegador del Mac obliga a bajar 28 GB y volver a
subirlos; el 2026-10-20 eso se cayó a 200 KB/s.

## Estado de la capa

| | |
|---|---|
| Capa | «Predios SII 2026S2 · Catastral» (id **1111**), categoría «Catastro predial», grupo «Catastral.cl», nivel Comunal |
| En GeoMarket | Catastral → Catastro predial → Catastral.cl → casilla + elegir comuna (la lista se filtra por la región elegida arriba) |
| Pintado | «Por valores de atributo» = `destino_descripcion` |
| Tooltip | 12 atributos (ver abajo) + «Versión de datos», que solo aparece en las 6 comunas livianas; el archivo trae las 123 columnas, salvo en esas 6 |
| Habilitación a usuarios | Switches en el detalle de la capa. **No se tocan sin OK de Cris** (es publicar a terceros) |

## Arquitectura

```
Mac ──ssh -L 6080──► .33: noVNC :6080 ─► x11vnc :5900 ─► Xvfb :99 ─► google-chrome (perfil /root/citify_vps/citify-chrome)
                                                                         ▲ CDP 127.0.0.1:9222
                .33: conductor_vps.py (Playwright) ──────────────────────┘
                     └─ inyecta _cargador.js; la página lee los GeoJSON de 127.0.0.1:8765 (serve_cors_vps.py → /root/tmp_citify)
```

Todo escucha **solo en 127.0.0.1** de la .33. La pantalla remota se ve únicamente por el túnel SSH.

## Paso a paso

1. **Generar los GeoJSON** (en la .33, desde la base 2026S2 completa):
   `python3 genera_todas.py` → `/root/tmp_citify/<Comuna>_<cod>_2026S2.geojson` (~28 GB las 345 comunas, ~40 min).
   Una sola: `python3 geojson_comuna.py <cod_SII[,cod2]> <salida> "<Nombre como en Citify>"`.
2. **Levantar el navegador**: `setsid nohup /root/citify_vps/levanta.sh </dev/null &` (Xvfb, Chrome, VNC y noVNC).
   Servidor de archivos: `setsid nohup python3 serve_cors_vps.py /root/tmp_citify </dev/null &`.
3. **Túnel desde el Mac**: `ssh -N -L 6080:127.0.0.1:6080 root@46.224.221.33` y abrir
   `http://localhost:6080/vnc.html` → Conectar → **Cris inicia sesión en Citify** (Claude no ingresa contraseñas).
4. **Cerrar Citify en cualquier otro navegador.** Citify mantiene **una sola sesión activa**: si otro
   navegador la usa, la de la .33 se cae y redirige a `/api/auth/logout`.
5. **Lanzar el conductor**: `cd /root/citify_vps && setsid nohup python3 conductor_vps.py 800 > conductor.log 2>&1 </dev/null &`
   y seguir con `tail -f conductor.log`. Registra lo subido en `subidas.txt` y retoma desde ahí.
6. **Verificar** en el mapa una comuna chica (tooltip de 12 atributos) y cuadrar el inventario
   (editor → «Volver a regiones» → contador «n/N archivos» por región).

## Reglas que costaron caro

| Regla | Por qué |
|---|---|
| **Código SII ≠ código INE** (San Joaquín = 16163, no 13129) | El cruce se hace por código SII; el catálogo está en `comunas_v2.json` |
| **`comuna` debe ser texto** con el nombre tal como lo muestra Citify | Con el código numérico, el visor se cae (`properties.comuna.toLowerCase is not a function`) y no se ve nada |
| Varias comunas SII → una de Citify | Santiago = 13101+13135+13134; San Javier → «San Javier de Loncomilla»; Puerto Natales → «Natales»; La Calera → «Calera»; Lo Miranda → «Doñihue»; Trehuaco → «Treguaco»; Paihuano → «Paiguano»; Puerto Williams → «Cabo de Hornos». Antártica queda vacía |
| **Tooltip: solo 12 atributos** (pedido del dueño de Citify) | Rol, Dirección, Comuna, Destino, Superficie terreno (m²), Superficie construida (m²), Pisos, Año de construcción, Materialidad, Avalúo fiscal, Contribución semestral, **Valor comercial estimado (UF/m²)**. `val_com_uf` es **por m²**, no el valor del predio |
| Citify **rehace la lista de atributos con cada carga**, mirando el primer predio de cada archivo | Por eso: (a) `geojson_comuna.py` ordena primero el predio más completo; (b) cada tanda lleva el **portador** `Portador_Panquehue.geojson`, que repone los 12; (c) `_cargador.js` deja los 12 antes de cada «Guardar» |
| «Guardar» se pulsa dos veces | El primer clic muestra el mapeo de atributos |
| El editor tarda en abrir: 1 a 3 min | Al abrirlo, Citify baja y descifra uno de los archivos ya subidos |
| Chrome frena los `setTimeout` de páginas no visibles | Las esperas del cargador van por un Web Worker (`__wsleep`) |
| Nunca dos ciclos en la misma página | Se pelean el editor; `__corriendo` lo impide |
| **Tope de ~536 MB por archivo** (largo máximo de un texto en Chrome) | Sobre ese tamaño, Citify responde «Error al procesar el archivo». Esas comunas van en **versión liviana**, con `MINIMO=1 python3 geojson_comuna.py …`: solo los 12 atributos del tooltip más las llaves (rol, manzana, predio, poligono_id, lat/lon) y la leyenda `version_datos` = «Liviana: solo los atributos visibles», que el tooltip muestra como «Versión de datos». Desde el 2026-10-20 van así Antofagasta, Providencia, Viña del Mar, Ñuñoa, Las Condes y Santiago. Quitar solo las columnas duplicadas (`LIVIANO=1`) no alcanza: Las Condes baja de 1.067 a 930 MB |
| La memoria se acumula | El conductor recarga la página cada 3 tandas. Tandas de ≤800 MB en la .33 (≤500 MB si es desde un Mac) |

## Archivos

| Archivo | Qué hace |
|---|---|
| `citify/geojson_comuna.py` | GeoJSON de una comuna desde `chile_predios_2026s2_completa.parquet` (123 columnas, nulos omitidos, predio más completo primero) |
| `citify/genera_todas.py` | Genera todas las comunas de `manifiesto.json`, de menor a mayor |
| `citify/manifiesto.json`, `citify/citify_comunas.txt` | Cruce código SII → (región, comuna) de Citify; catálogo de Citify por región |
| `citify/_cargador.js` | Lo que corre dentro de la página: pone archivos en el editor, deja los 12 atributos y guarda (`__subir`, `__atributos`, `__reconcilia`) |
| `citify/conductor_vps.py` | Maneja el Chrome de la .33 por CDP: tandas, reintentos, recargas, registro |
| `citify/serve_cors_vps.py`, `citify/levanta.sh` | Servidor local de archivos y arranque del navegador remoto |

## Al terminar

Apagar el navegador remoto con `/root/citify_vps/apaga.sh` (detiene Chrome, Xvfb, VNC, noVNC, el servidor
de archivos y el conductor; deja todo instalado). El perfil con la sesión de Citify queda en
`/root/citify_vps/citify-chrome` y se reutiliza en la próxima carga con `levanta.sh`; si la sesión expiró,
Cris vuelve a iniciarla por noVNC. Los GeoJSON (`/root/tmp_citify`, ~28 GB) se borran al terminar: se
regeneran en ~40 min.

_2026-10-21: 345/345 cargadas; navegador apagado con el perfil conservado; `/root/tmp_citify` y `s3://siipredios/tmp_citify/` borrados._
