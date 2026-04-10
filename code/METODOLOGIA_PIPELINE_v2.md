# Metodología: Pipeline de Cartografía Predial SII v2

## Objetivo

Construir un dataset geoespacial completo de los ~9.5 millones de predios de Chile, combinando datos tabulares del SII (avalúos, destinos, superficies, coordenadas) con polígonos vectoriales extraídos del WMS cartográfico del SII. El resultado es un registro por predio con toda su información tabular y su geometría predial, más polígonos sin dato tabular para preservar cobertura espacial completa.

---

## Infraestructura

### VPS
- **Servidor**: Hetzner Cloud, 8 vCPU, 128GB RAM, 150GB SSD
- **IP**: `root@YOUR_VPS_IP`
- **OS**: Debian 12
- **Python**: `/root/carto_predios/venv/bin/python3` (3.12)
- **GDAL**: `gdal_polygonize.py`, `gdal_translate`, `gdalbuildvrt` (sistema)
- **Código**: `/root/carto_predios/sii_vectorizer/`

### Túneles VPN (Mullvad WireGuard)
El SII bloquea IPs de datacenter y aplica rate limit de 1 req/s por IP. Para paralelizar, usamos 30 túneles Mullvad WireGuard en network namespaces Linux.

**Configuración:**
```
/etc/wireguard/vpn0.conf ... vpn29.conf    # 30 configs WireGuard
```

Cada túnel corre en su propio network namespace:
```bash
ip netns add vpn0
ip link add wg-vpn0 type wireguard
ip link set wg-vpn0 netns vpn0
ip netns exec vpn0 wg setconf wg-vpn0 /etc/wireguard/vpn0.conf
ip netns exec vpn0 ip link set wg-vpn0 up
ip netns exec vpn0 ip addr add 10.66.0.X/32 dev wg-vpn0
ip netns exec vpn0 ip route add default dev wg-vpn0
```

Para ejecutar un proceso por un túnel específico:
```bash
ip netns exec vpn5 python3 mi_script.py  # usa la IP de vpn5
```

**Rotación automática de IP:**
Cuando una IP es bloqueada (429/timeout por 60s), el túnel se rota a un relay Mullvad diferente:
```bash
# En featureinfo_worker.py::rotate_tunnel()
# Lee /tmp/mullvad_relays.json → elige relay fresco → actualiza peer WireGuard
ip netns exec vpnN wg set wg-vpnN peer NEW_PK endpoint NEW_IP:51820
```

**Work stealing:**
Cuando un worker termina su porción de trabajo, roba trabajo del worker más atrasado:
```bash
# En batch_tif_30ns.sh: worker que termina → escanea chunks restantes
# → roba del chunk con más trabajo pendiente (mín 5 items)
```

### Almacenamiento S3
- **Endpoint**: `https://nbg1.your-objectstorage.com` (Hetzner Object Storage)
- **Bucket**: `siipredios`
- **Credenciales**: `AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY`

---

## Fase 0: Extracción de datos tabulares

### Objetivo
Descargar todos los datos tabulares de los ~9.5M predios de Chile desde la API interna del SII.

### Script principal
```
/root/carto_predios/sii_vectorizer/0_get_sii.py    (44 KB)
```

### Fuentes de datos
1. **API SII** (`www4.sii.cl/mapasui/services/data/mapasFacadeService/getPredioNacional`):
   - Avalúo fiscal (total, afecto, exento)
   - Destino (habitacional, comercio, oficina, etc.)
   - Superficie terreno y construida (m²)
   - Dirección, manzana, predio
   - Áreas homogéneas
   - Período de avalúo

2. **API BCN** (coordenadas):
   - Latitud/longitud de cada predio
   - Más precisa que coordenadas SII para ~80% de predios

3. **Observatorio de Mercado de Suelo Urbano 2025** (capas adicionales):
   - Valor comercial m² de suelo
   - Transacciones por destino
   - Áreas homogéneas RAV no agrícola

### Ejecución
```bash
# En VPS, tmux 'fase0':
cd /root/carto_predios/sii_vectorizer
tmux new -s fase0
python3 0_get_sii.py --all
```

El script itera las 347 comunas, descargando todos los predios de cada una. Usa 30 túneles Mullvad en paralelo (1 req/s por IP = 30 req/s efectivos).

### Rate limiting y paralelismo
- 1 request/segundo por IP (máximo seguro sin bloqueo)
- 30 IPs simultáneas = 30 req/s
- Si una IP recibe HTTP 429 → se rota automáticamente
- ~9.5M predios ÷ 30 rps = ~3.7 días teóricos, ~15 días reales (reintentos, rotaciones)

### Output
```
s3://siipredios/2025ss_bcn/sii_data/comuna={cod}.csv
```
- 347 archivos CSV, uno por comuna
- ~90+ columnas por predio
- Columnas clave: `rol`, `lat`, `lon`, `comuna`, `manzana`, `predio`, `ubicacion`, `valorTotal`, `supTerreno`, `supConsMt2`, `direccion_sii`, `obs_valor_comercial_m2_suelo`

### Lectura correcta del CSV
```python
# IMPORTANTE: engine='python' para manejar comas decimales chilenas
df = pd.read_csv(path, engine='python', dtype=str, on_bad_lines='skip')
# obs_valor_comercial_m2_suelo usa coma decimal dentro de comillas: "5,72 UF"
```

---

## Fase 1: Descarga cartográfica WMS

### Objetivo
Descargar las imágenes del mapa catastral del SII para cada comuna a máxima resolución (zoom 19, ~0.3m/pixel) y ensamblar en un GeoTIFF único.

### Scripts
```
/root/carto_predios/sii_vectorizer/batch_tif_30ns.sh       (22 KB) — orquestador
/root/carto_predios/sii_vectorizer/download_chunk.py        (9.4 KB) — worker descarga
/root/carto_predios/sii_vectorizer/prepare_tif_queue.py     (8.8 KB) — genera queue
/root/carto_predios/sii_vectorizer/comunas.py               (7.1 KB) — lista de comunas + normalización nombres WMS
```

### Fuente
- **WMS SII** (`www4.sii.cl/mapasui/services/ui/wmsProxyService/call`)
- Layer: `sii:BR_CART_{NOMBRE_WMS}_WMS` (ej: `sii:BR_CART_P_AGUIRRE_CERDA_WMS`)
- Formato: PNG 256×256 px por tile, zoom 19
- 25 comunas requieren corrección de nombre WMS (abreviaciones SII): ver `comunas.py::EXCEPCIONES_WMS`

### Proceso paso a paso

**1. Preparar queue:**
```bash
python3 prepare_tif_queue.py
# Genera: /tmp/tif_queue.txt
# Formato: {cod}|{nombre_wms}|{bbox}
# Ejemplo: 16162|P_AGUIRRE_CERDA|-70.72,-33.52,-70.64,-33.47
```

**2. Lanzar batch:**
```bash
tmux new -s fase1
bash batch_tif_30ns.sh /tmp/tif_queue.txt
```

**3. Por cada comuna, el batch:**
- Verifica si ya existe en S3 → skip
- Probe WMS: prueba 1 tile para validar nombre de layer
- Calcula tiles z19 desde bbox → distribuye en 30 chunks
- Lanza 30 workers en paralelo:
  ```bash
  ip netns exec vpn{i} python3 download_chunk.py \
      --comuna {cod} --nombre {nombre} \
      --chunk {i} --total-chunks 30 \
      --out-dir /tmp/tiles/{cod}
  ```
- Monitor cada 15s: si un worker no avanza en 60s → IP quemada → rota túnel
- Work stealing: worker que termina roba chunk del más atrasado
- Ensambla GeoTIFF (función `assemble_geotiff()`):
  ```python
  # Crea GeoTIFF RGBA, tiled=True, DEFLATE, EPSG:3857
  rasterio.open(out, 'w', driver='GTiff',
      height=th, width=tw, count=4, dtype=np.uint8,
      crs=CRS.from_epsg(3857), transform=transform,
      compress='deflate', tiled=True, blockxsize=256, blockysize=256)
  # Escribe cada tile en su posición exacta usando Window
  ```
- Sube a S3, limpia tiles locales

### Output
```
s3://siipredios/2025ss_bcn/TIFs/comuna={cod}.tif
```
- GeoTIFF RGBA, EPSG:3857, tiled+DEFLATE
- Tamaño: 10-500 MB por comuna
- 347 archivos

---

## Fase 2: Vectorización

### Objetivo
Extraer polígonos prediales del GeoTIFF rasterizado → geometrías vectoriales limpias.

### Scripts
```
/root/carto_predios/sii_vectorizer/2_vectorizar.py          (14 KB) — vectorización
/root/carto_predios/sii_vectorizer/batch_vectorize.sh        (5.3 KB) — batch para 347 comunas
```

### Proceso paso a paso

**1. Lanzar batch:**
```bash
tmux new -s fase2
bash batch_vectorize.sh
```

**2. Por cada comuna:**
```bash
python3 2_vectorizar.py \
    --input /tmp/TIFs/comuna={cod}.tif \
    --cod {cod} --nombre {nombre}
```

**3. Pipeline interno de `2_vectorizar.py`:**

a. **Polygonize** (`gdal_polygonize.py -b 1`):
   - Extrae polígonos por valor DN en banda roja
   - Resultado: miles de polígonos con atributo `DN`

b. **Filtrar por DN**:
   - Retener solo DN ∈ [160, 200] (interior predial rojo-marrón del WMS)
   - `interior_value=182` es el valor central

c. **Fix blank blocks**:
   - Para zonas grandes (>50,000 m²) donde el WMS no renderizó bordes internos
   - Re-descarga esas zonas a zoom mayor → aplica máscara al TIF → re-polygonize

d. **Filtrar y limpiar geometrías** (`_filtrar_poligono()`):
   - Área: [1, 50000] m² (elimina artefactos y ruido)
   - Relleno de hoyos compactos: hoyos <100 m² con compactness >0.25

e. **Export**:
   - GeoPackage en EPSG:3857 (archivo principal)
   - GeoJSON en EPSG:4326 (visualización)

### Output
```
s3://siipredios/2025ss_bcn/vectors/comuna={cod}.gpkg     — polígonos EPSG:3857
s3://siipredios/2025ss_bcn/vectors/comuna={cod}.geojson   — polígonos EPSG:4326
```
- Columnas: `geometry`, `area_m2`
- Cada polígono = un lote predial (puede contener 1 o más predios en multi-unidad)

---

## Fase 3: Join consolidado (v2)

### Objetivo
Asignar un polígono a cada predio tabular. Preservar polígonos sin match como huérfanos. Producir dataset final.

### Diferencia clave vs pipeline v1
El pipeline v1 (Fases 3-8 originales) asignaba UN rol a cada polígono (dirección polígono→predio). Un edificio con 50 departamentos asignaba solo 1 predio, los otros 49 requerían 5 fases adicionales de recuperación.

El pipeline v2 **invierte la dirección**: asigna UN polígono a cada predio (dirección predio→polígono). Múltiples predios apuntan naturalmente al mismo polígono. Un solo script reemplaza las Fases 3, 4, 5, 6 y 8 del pipeline v1.

### Script
```
/root/carto_predios/sii_vectorizer/3_join_mejorado.py    (22 KB)
```

### Input
```
s3://siipredios/2025ss_bcn/sii_data/comuna={cod}.csv     — F0 CSV (datos tabulares)
s3://siipredios/2025ss_bcn/vectors/comuna={cod}.gpkg      — F2 GPKG (polígonos vectorizados)
```

### Ejecución por comuna
```bash
python3 3_join_mejorado.py \
    --csv /tmp/f0/comuna={cod}.csv \
    --gpkg /tmp/f2/comuna={cod}.gpkg \
    --output /tmp/f3_output \
    --cod {cod} \
    --nombre {NOMBRE_WMS}   # opcional, activa getFeatureInfo para manzanas huérfanas
```

### Proceso paso a paso

#### Paso 0: Carga y limpieza de polígonos
- Cargar F2 GPKG → asegurar EPSG:3857
- **Relleno de hoyos <50 m²**: el WMS renderiza números de predio (32, 5, 13...) dentro de cada polígono. Estos píxeles tienen DN diferente → la vectorización crea hoyos con forma de número. Se rellenan todos los hoyos <50 m² sin importar compactness. Hoyos ≥50 m² se preservan (patios reales).
- Recalcular `area_m2` post-relleno

**Impacto del relleno**: +2,506 predios pasan de método 2 (nearest) a método 1 (contains) porque el punto lat/lon ya no cae en un hoyo de texto.

#### Paso 1: Carga de predios
- Cargar F0 CSV con `engine='python'`, `dtype=str`
- Convertir `lat`/`lon` a numérico
- Filtrar coordenadas fuera de Chile: lat ∈ [-62°, -15°], lon ∈ [-80°, -64°]
- Proyectar a EPSG:3857

#### Paso 2: Método 1 — Point-in-polygon (vectorizado)
```python
joined = gpd.sjoin(gdf_pts, gdf_poly, how="inner", predicate="within")
```
- Usa spatial index C/Cython (órdenes de magnitud más rápido que loop Python)
- Captura **~93-98%** de predios con coordenadas
- ~1 segundo para 25,000 predios

#### Paso 3: Método 2 — Nearest 10m (vectorizado)
```python
nearest = gpd.sjoin_nearest(pts_unmatched, gdf_poly, max_distance=10, distance_col="_dist")
```
- Para predios cuyo punto no cae dentro de ningún polígono
- Drift típico de coordenadas SII: ~0.2m mediana, hasta ~3m extremo
- 10m cubre 99.9% de los casos de drift
- Captura **~1-5%** adicional

#### Paso 4: Método 3 — Herencia por coordenada compartida
- Predios en edificios/condominios comparten exacta lat/lon
- Si predio A (ya matcheado) tiene misma coordenada que predio B (sin match) → B hereda el polígono de A
- Vectorizado con merge sobre `_coord_key`
- Captura **~0-1%** adicional

#### Paso 5: Método 4 — Manzana neighbor
- El rol chileno `MMMM-PPPP` codifica posición física:
  - `MMMM` = manzana (bloque catastral)
  - `PPPP` = número predial, asignado secuencialmente en orden físico
- Si predio `0471-0033` no tiene match, hereda del vecino numérico más cercano en la misma manzana (ej: `0471-0032`)
- Rango: diff ≤ 20 predios
- **No requiere coordenada** — usa solo estructura catastral del SII
- Captura **~1-4%** adicional

#### Paso 6: Método 5 — getFeatureInfo (SII, opcional)
- Solo para manzanas donde **CERO** predios tienen coordenada (sin ningún anchor espacial)
- Consulta la API getFeatureInfo del SII por el centroide de cada polígono sin asignar
- 30 workers paralelos via túneles Mullvad (`featureinfo_worker.py`)
- Cola compartida con `flock` (work stealing atómico)
- Workers terminan solos cuando la cola se vacía (sin timeout artificial)
- El SII retorna el `rol` del predio en esa posición → mapea polígono a manzana
- Captura **~0.1-0.5%** adicional

**Script worker:**
```
/root/carto_predios/sii_vectorizer/featureinfo_worker.py    (8.5 KB)
```
```bash
ip netns exec vpn{i} python3 featureinfo_worker.py \
    --tunnel {i} --layer sii:BR_CART_{NOMBRE}_WMS \
    --queue /tmp/fi_queue.txt --outdir /tmp/fi_results/
```

#### Paso 7: Método 6 — Fallback (último recurso)
- Con coordenada: polígono más cercano sin límite de distancia (hasta 500m)
- Sin coordenada: cualquier polígono de la misma manzana
- Baja precisión pero asigna geometría

#### Paso 8: Polígonos huérfanos
- Polígonos de F2 que no fueron asignados a ningún predio
- Se agregan como filas extra con geometría y `pol_area_m2` pero sin datos tabulares
- Preserva cobertura espacial completa
- Marcados con `_match_method = "orphan_polygon"`

#### Paso 9: Conversión CRS y export
- Todas las geometrías se convierten de EPSG:3857 a EPSG:4326 (vectorizado con `to_crs()`)
- Export CSV (sin geometría) + GPKG (con geometría)

### Output
```
s3://siipredios/2025ss_bcn/fase3v2/comuna={cod}.csv      — datos tabulares + pol_area_m2
s3://siipredios/2025ss_bcn/fase3v2/comuna={cod}.gpkg      — datos + geometría EPSG:4326
```

Columnas de cada fila-predio:
- Todas las columnas del F0 CSV (~90+)
- `pol_area_m2`: área del polígono asignado (m²)
- `_poly_idx`: índice del polígono en F2 (para debug)
- `_match_method`: método que logró el match (para auditoría)

Filas adicionales (polígonos huérfanos):
- `geometry`: polígono EPSG:4326
- `pol_area_m2`: área
- `_match_method`: "orphan_polygon"
- Todas las demás columnas: null

### Ejecución batch para 347 comunas
```bash
# En VPS:
tmux new -s fase3
python3 batch_join_v2.py   # por implementar — descarga F0+F2 de S3, corre 3_join_mejorado.py, sube a S3
```

### Rendimiento

**Pedro Aguirre Cerda (27,184 predios, 24,777 polígonos):**

| Método | Predios | % acum. | Tiempo |
|--------|---------|---------|--------|
| 1. Point-in-polygon | 25,406 | 93.5% | 1.0s |
| 2. Nearest 10m | 323 | 94.7% | 0.0s |
| 3. Herencia coord | 0 | 94.7% | 0.1s |
| 4. Manzana neighbor | 1,047 | 98.5% | 0.5s |
| 5. getFeatureInfo | 2 | 98.5% | 125s |
| 6. Fallback | 139 | 99.0% | 0.0s |
| **Total matched** | **26,917** | **99.0%** | — |
| Sin match | 267 | — | — |
| Polígonos huérfanos | 2,950 | — | — |
| **Filas output** | **30,134** | — | **~2 min** |

Sin `--nombre` (sin FI): ~7 segundos por comuna. Para 347 comunas: ~40 minutos.

### Protecciones
- **Coordenadas basura**: el SII tiene predios con `lat=-90.0` (polo sur). Se filtran a bounds Chile.
- **Hoyos de texto WMS**: se rellenan hoyos <50 m² al cargar polígonos.
- **Multi-unidad**: el join invertido (predio→polígono) lo maneja naturalmente.

---

## Fase 4: Enriquecimiento con catastro semestral

### Objetivo
Enriquecer el dataset de Fase 3 con las columnas de edificación/construcción del CSV semestral del SII (`catastro_2025_2.csv`), y agregar predios que existen en el catastro pero no estaban en Fase 3.

### Script
```
/root/carto_predios/sii_vectorizer/4_enrich_catastro.py    (8 KB)
/root/carto_predios/sii_vectorizer/5_generate_catalog.py   (5 KB) — genera JSON catálogo
```

### Input
```
s3://siipredios/2025ss_bcn/fase3v2/comuna={cod}.gpkg           — F3v2 (predios + geometría)
s3://siipredios/catastro_historico/output/catastro_2025_2.csv  — catastro semestral (1.6 GB, 9.4M filas, 39 cols)
```

### Fuente: catastro semestral SII
El archivo `catastro_2025_2.csv` proviene del TXT de ancho fijo `BRTMPNACROL_NAC_2025_2` del SII, procesado y disponible en catastral.cl como "Descarga Masiva". Contiene 39 columnas por predio:

- Identificación: `periodo`, `anio`, `semestre`, `comuna`, `manzana`, `predio`
- Rol de contribuciones: `rc_direccion`, `rc_serie`, `rc_ind_aseo`, `rc_cuota_trimestral`, `rc_avaluo_total`, `rc_avaluo_exento`, `rc_anio_term_exencion`, `rc_cod_ubicacion`, `rc_cod_destino`
- Datos catastro: `dc_direccion`, `dc_avaluo_fiscal`, `dc_contribucion_semestral`, `dc_cod_destino`, `dc_avaluo_exento`, `dc_sup_terreno`, `dc_cod_ubicacion`
- Bien común / padre: `dc_bc1_comuna`, `dc_bc1_manzana`, `dc_bc1_predio`, `dc_bc2_*`, `dc_padre_*`
- Construcción: `n_lineas_construccion`, `sup_construida_total`, `anio_construccion_min`, `anio_construccion_max`, `materiales`, `calidades`, `pisos_max`, `serie`

### Columnas nuevas agregadas a F3v2
Las siguientes 24 columnas del catastro no existían en F3v2 y se agregan via join:

```
dc_contribucion_semestral, dc_cod_destino, dc_avaluo_fiscal, dc_avaluo_exento,
dc_sup_terreno, dc_cod_ubicacion, dc_direccion,
dc_bc1_comuna, dc_bc1_manzana, dc_bc1_predio,
dc_bc2_comuna, dc_bc2_manzana, dc_bc2_predio,
dc_padre_comuna, dc_padre_manzana, dc_padre_predio,
n_lineas_construccion, sup_construida_total,
anio_construccion_min, anio_construccion_max,
materiales, calidades, pisos_max, serie
```

### Join key
`comuna + manzana + predio`, normalizado a entero para resolver diferencias de formato:
- F3v2 usa zero-padded: `manzana='00397'`, `predio='00013'`, `comuna='10102'`
- Catastro usa enteros: `manzana=397`, `predio=13`, `comuna=10102`

### Proceso paso a paso

**1. Carga catastro** (una vez):
- Descarga `catastro_2025_2.csv` de S3 (1.6 GB)
- Carga en pandas con `dtype=str` (~42s, ~11 GB RAM)
- Indexa por comuna normalizada

**2. Por cada comuna:**

a. **Descarga F3v2 GPKG** de S3

b. **LEFT JOIN F3v2 ← catastro** por manzana+predio:
   - Predios con match: se agregan las 24 columnas nuevas
   - Polígonos huérfanos (sin manzana/predio): columnas nuevas quedan null
   - Se preservan TODAS las geometrías

c. **Predios catastro-only**:
   - Predios en catastro que no existen en F3v2
   - Se agregan como filas nuevas sin geometría
   - Marcados con `_match_method = "catastro_only"`
   - Tienen datos tabulares del catastro (muchos con lat/lon del F0)

d. **Export**:
   - **GPKG**: solo filas con geometría (polígonos). Nunca se pierde un polígono.
   - **CSV**: todas las filas (predios con polígono + catastro-only + huérfanos). Toda la info.
   - **CSV raw**: slice crudo del catastro semestral para esa comuna.

### Output
```
s3://siipredios/2025ss_bcn/fase4v2/comuna={cod}.gpkg      — polígonos enriquecidos, EPSG:4326
s3://siipredios/2025ss_bcn/fase4v2/comuna={cod}.csv       — todo: ~112 columnas, sin geometría
s3://siipredios/2025ss_bcn/fase4v2/comuna={cod}_raw.csv   — catastro crudo, 39 columnas
s3://siipredios/2025ss_bcn/fase4v2/catalog.json           — catálogo JSON con metadatos
```

### Ejecución
```bash
# En VPS:
tmux new -s fase4
python3 4_enrich_catastro.py                    # todas las comunas (~50 min)
python3 4_enrich_catastro.py --cod 16162        # una sola comuna
python3 4_enrich_catastro.py --skip-existing    # skip ya procesadas

# Generar catálogo JSON
python3 5_generate_catalog.py                   # ~10 min
```

### Resultados globales (343 comunas)

| Categoría | Cantidad | % |
|-----------|----------|---|
| Total filas CSV | 11,306,273 | 100% |
| Total polígonos (GPKG) | 9,128,582 | 80.7% |
| Con datos + polígono | 7,292,537 | 64.5% |
| Solo polígono (huérfanos) | 1,836,045 | 16.2% |
| Catastro-only (sin polígono) | 2,177,687 | 19.3% |
| Con lat/lon | 7,083,741 | 62.6% |

### Ejemplo: Pedro Aguirre Cerda (16162)

| Categoría | Cantidad |
|-----------|----------|
| Polígono + datos | 26,915 (90.1%) |
| Solo polígono | 2,952 (9.9%) |
| Catastro-only | 2,211 |
| **Total CSV** | **32,078** |

### Catálogo JSON
`catalog.json` contiene un registro por comuna con:
- `codigo`, `nombre`
- `predios.total_csv`, `predios.total_poligonos`, `predios.con_datos_y_poligono`, `predios.solo_poligono`, `predios.catastro_only`, `predios.con_latlon`
- `archivos.gpkg.key`, `archivos.gpkg.tamano_mb`
- `archivos.csv.key`, `archivos.csv.tamano_mb`
- `archivos.csv_raw.key`, `archivos.csv_raw.tamano_mb`

---

## Mapa de datos en S3

```
s3://siipredios/
├── 2025ss_bcn/
│   ├── sii_data/                          # Fase 0: datos tabulares
│   │   └── comuna={cod}.csv               # 347 archivos, ~90 columnas cada uno
│   ├── TIFs/                              # Fase 1: GeoTIFFs
│   │   └── comuna={cod}.tif               # 347 archivos, RGBA EPSG:3857
│   ├── vectors/                           # Fase 2: polígonos vectorizados
│   │   ├── comuna={cod}.gpkg              # EPSG:3857
│   │   └── comuna={cod}.geojson           # EPSG:4326
│   ├── fase3v2/                           # Fase 3: join consolidado
│   │   ├── comuna={cod}.csv               # datos + pol_area_m2
│   │   └── comuna={cod}.gpkg              # datos + geometría EPSG:4326
│   ├── fase4v2/                           # Fase 4: enriquecido con catastro ★ FINAL
│   │   ├── comuna={cod}.gpkg              # polígonos enriquecidos EPSG:4326
│   │   ├── comuna={cod}.csv               # todo: ~112 cols, sin geometría
│   │   ├── comuna={cod}_raw.csv           # catastro crudo, 39 cols
│   │   └── catalog.json                   # catálogo JSON con metadatos
│   ├── fase4/                             # Pipeline v1 (legacy)
│   ├── fase5/                             # Pipeline v1 (legacy)
│   ├── fase6/                             # Pipeline v1 (legacy)
│   └── fase8/                             # Pipeline v1 (legacy)
└── catastro_historico/
    └── output/
        └── catastro_2025_2.csv            # TXT semestral SII procesado (1.6 GB)
```

---

## Ejecución completa desde cero

```bash
# Conectar a VPS
ssh root@YOUR_VPS_IP
cd /root/carto_predios/sii_vectorizer

# Fase 0: extracción datos (~15 días)
tmux new -s fase0
python3 0_get_sii.py --all

# Fase 1: descarga WMS (~10 días)
tmux new -s fase1
python3 prepare_tif_queue.py
bash batch_tif_30ns.sh /tmp/tif_queue.txt

# Fase 2: vectorización (~2 días)
tmux new -s fase2
bash batch_vectorize.sh

# Fase 3: join consolidado (~40 min sin FI, ~3 horas con FI)
tmux new -s fase3
python3 batch_join_v2.py

# Fase 4: enriquecimiento con catastro semestral (~50 min)
tmux new -s fase4
python3 4_enrich_catastro.py

# Generar catálogo JSON (~10 min)
python3 5_generate_catalog.py
```

---

## Limitaciones conocidas

1. **~1% de predios sin geometría**: predios en manzanas donde ninguno tiene coordenada y el getFeatureInfo no responde. Típicamente predios fiscales, terrenos eriazos, o registros incompletos.

2. **Polígonos multi-unidad**: un polígono de edificio se asigna a todos sus departamentos. La geometría es idéntica para todos — el WMS del SII no renderiza subdivisión horizontal.

3. **Drift de coordenadas SII**: ~0.2m mediana. El método 2 (nearest 10m) compensa. Algunos predios tienen `lat=-90.0` — se filtran.

4. **Cobertura rural**: comunas rurales/patagónicas ~70-90% porque el WMS no renderiza predios agrícolas extensos y muchos carecen de coordenadas.

5. **Nombres WMS no estándar**: 25 comunas tienen nombres abreviados o diferentes en el WMS (ej: "San Pedro de Atacama" → "SAN_PEDRO_ATACAMA"). Lista en `comunas.py::EXCEPCIONES_WMS`.

---

*Pipeline diseñado y ejecutado marzo-abril 2026. Datos reproducibles con los scripts indicados.*
