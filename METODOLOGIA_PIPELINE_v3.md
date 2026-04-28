# Metodologia: Pipeline de Cartografia Predial SII v3

## Objetivo

Construir un dataset geoespacial completo de los ~9.5 millones de predios de Chile, combinando datos tabulares del SII (avaluos, destinos, superficies, coordenadas) con poligonos vectoriales extraidos del WMS cartografico del SII. El resultado es un GeoParquet por comuna con toda la informacion tabular y geometria predial.

---

## Cambios respecto a v2

El pipeline v2 operaba en 5 fases separadas: descarga tabular → descarga tiles WMS → ensamblaje GeoTIFF → GDAL polygonize → join espacial. Cada fase era un script independiente con su propio batch.

El pipeline v3 integra todo en un **orquestador unico** (`fase0_orchestrator.py`) que ejecuta los 10 pasos secuencialmente por comuna. Los cambios principales:

| Aspecto | v2 | v3 |
|---------|----|----|
| Tuneles VPN | 30 | 70 |
| Descarga WMS | Tiles individuales 256px | Supercells 1024px (4x4 tiles) |
| Imagen intermedia | GeoTIFF completo por comuna | Ningun archivo intermedio |
| Vectorizacion | GDAL polygonize | Python (rasterio + scipy + shapely) |
| Bloques | Sin overlap → grid lines | 16384px con 1024px overlap |
| Match | 6 metodos + getFeatureInfo | 4 metodos + OCR recovery |
| Output | GPKG + CSV + CSV raw | GeoParquet + CSV |
| Batch | Secuencial por fase | 2 etapas: descarga secuencial + procesamiento paralelo |

---

## Infraestructura

### VPS
- **Servidor**: Hetzner Cloud, 8 vCPU, 128GB RAM, 150GB SSD
- **OS**: Debian 12
- **Python**: 3.12 con GeoPandas, rasterio, shapely, scipy, Pillow, boto3

### Tuneles VPN (Mullvad WireGuard)
70 tuneles Mullvad WireGuard en network namespaces Linux (`vpn0` a `vpn69`). Cada tunel tiene su propia IP publica.

```bash
ip netns exec vpn5 python3 mi_script.py  # ejecuta con IP de vpn5
```

**Rotacion automatica de IP:** cuando un worker no progresa en 60s, se rota el relay Mullvad del tunel afectado.

**Work stealing con flock:** cola compartida en archivo de texto. Todos los workers compiten por items usando `fcntl.flock()` (bloqueo atomico). Cuando un worker termina su batch, toma mas items de la cola. Esto elimina el problema de tail-end slowness.

### Almacenamiento S3
- **Endpoint**: Hetzner Object Storage (S3-compatible)
- **Bucket**: `siipredios`

---

## El orquestador: fase0_orchestrator.py

Ejecuta 10 pasos secuenciales por comuna:

### Paso 1-2: Descarga de datos tabulares

Descarga todos los predios de la comuna desde la API interna del SII (`getPredioNacional`).

- **Fuente de roles**: union de `roles_split/` (roles conocidos) + catastro semestral (roles adicionales)
- **Workers**: 70 tuneles en paralelo, cada uno en su network namespace
- **Cola**: archivo compartido con flock, cada worker toma batch de 20 items
- **Stall detection**: si un worker no progresa en 60s → rota IP del tunel
- **Session renewal**: tras 20 nulls consecutivos → nueva sesion HTTP
- **Output**: JSONs individuales por predio en `/tmp/fase0_v2/{cod}/data/`

### Paso 3-4: Merge y normalizacion

`fase0_merge.py` consolida los JSONs individuales en un CSV unico:
- Parsea cada JSON con las APIs `datosAh`, `datosCsa`, `predioPublicado`
- Normaliza columnas (lat/lon, avaluos, superficies, destinos)
- Maneja predios agricolas (UTM → WGS84) y predios publicados (rol_base 9xxx)

`fase0_normalize.py` aplica normalizaciones adicionales y limpieza de datos.

**Output**: `/tmp/fase0_v2/{cod}/comuna={cod}.csv`

### Paso 5-7: Descarga de supercells WMS

`fase0_selective_tif.py` descarga imagenes del mapa catastral:

**Supercells**: en vez de tiles individuales de 256px, se piden imagenes de 1024x1024 px (4x4 tiles z19) al WMS del SII. A esta resolucion el SII renderiza poligonos completos con bordes nitidos.

**Calculo del area de descarga**:
1. Obtiene el poligono BCN (shapefile) de la comuna
2. Aplica buffer de 1.5 km
3. Calcula las supercells z19 que intersectan el poligono buffereado
4. Genera cola de trabajo (`sc_queue.txt`)

**Descarga paralela**: `sc_worker.py` corre en cada tunel VPN. Misma logica de flock queue + work stealing. Batch size de 5 items para evitar tail-end slowness.

**Retries**: 3 rondas de reintento para supercells que fallaron (respuestas XML error del WMS).

### Paso 8: Vectorizacion por bloques

Agrupa las supercells descargadas en bloques de 16x16 supercells (16384x16384 px, ~5x5 km):

```
BLOCK_SIZE = 16384 px (16 supercells x 1024 px)
OVERLAP    = 1024 px  (1 supercell de overlap entre bloques adyacentes)
STEP       = 15360 px (15 supercells de avance)
```

Por cada bloque:
1. **Ensambla** las supercells en un array numpy RGBA
2. **Vectoriza urbano**: filtra DN ∈ [160, 200] (interior predial rojo-marron), polygonize con rasterio.features.shapes, filtro de area [1, 50000] m2, relleno de hoyos <50 m2
3. **Vectoriza agricola**: filtra alpha=179 (relleno verde agricola), dilata bordes 3px para separar predios, filtro area >5000 m2, relleno hoyos <500 m2
4. **Combina** poligonos urbanos + agricolas

**Merge entre bloques**: `unary_union` de todos los poligonos seguido de relleno de hoyos para eliminar las costuras. El overlap de 1024px garantiza que los poligonos en los bordes se fusionen correctamente sin grid lines.

### Paso 9: Match espacial

`fase0_match.py` asigna un poligono a cada predio:

1. **Point-in-polygon** (vectorizado con `sjoin`): predio cuya coordenada cae dentro de un poligono. Captura ~93-98%.
2. **Nearest 10m** (`sjoin_nearest`): predio cuyo punto esta a <10m de un poligono. Compensa drift de coordenadas SII (~0.2m mediana). Captura ~1-5%.
3. **Herencia por coordenada**: predios en edificios comparten lat/lon. Si A tiene match y B tiene misma coordenada → B hereda poligono de A.
4. **Manzana neighbor**: el rol `MMMM-PPPP` codifica posicion fisica. Predio sin match hereda del vecino numerico mas cercano en la misma manzana (diff ≤ 20).

**OCR recovery** (`fase0_recovery.py`): para predios que aun no tienen match, lee los numeros de predio renderizados dentro de cada poligono WMS usando OCR (Tesseract). Empareja el numero OCR con el predio tabular de la misma manzana.

**Poligonos huerfanos**: poligonos vectorizados que no matchean con ningun predio se incluyen con `_match_method = "unmatched_polygon"`. Nunca se descartan — preservan cobertura espacial completa.

**Herencia de direcciones**: predios que comparten poligono heredan la direccion del predio que la tiene, mejorando cobertura de datos.

### Paso 10: Upload a S3

Sube CSV + GeoParquet a S3 y limpia archivos temporales.

---

## Batch: 2 etapas

El procesamiento masivo se divide en dos etapas para optimizar el uso de recursos:

### Etapa 1: Descarga secuencial (`batch_etapa1.py`)

Ejecuta la descarga de datos tabulares y supercells WMS **secuencialmente** por comuna. Usa los 70 tuneles VPN para cada comuna.

```bash
python3 -u batch_etapa1.py 2>&1 | tee /tmp/batch_etapa1.log
```

### Etapa 2: Procesamiento paralelo (`batch_etapa2.py`)

Ejecuta vectorizacion + match + upload en **paralelo** (3 workers con `ProcessPoolExecutor`). No usa tuneles — es CPU-only.

```bash
python3 -u batch_etapa2.py --workers 3 2>&1 | tee /tmp/batch_etapa2.log
```

Las dos etapas corren en paralelo: E1 descarga secuencialmente mientras E2 procesa las comunas ya descargadas. E2 se relanza periodicamente para capturar comunas nuevas.

---

## Post-procesamiento: optimize_parquet.py

Simplifica geometrias para reducir tamano de archivo:

- **Predios matched**: `simplify(0.00001, preserve_topology=True)` — ~1 metro de tolerancia
- **Poligonos huerfanos**: `simplify(0.00005, preserve_topology=False)` — ~5 metros
- **Fix invalid**: `make_valid()` para geometrias invalidas post-simplificacion

Reduce vertices en ~99.7% (de 12.5M a ~40K en comuna tipica). Archivos pasan de ~3 GB a ~10 MB.

---

## Output final

```
s3://siipredios/sii_extractor/{NOMBRE}_{COD}/
├── comuna={cod}.parquet    # GeoParquet: datos + geometria EPSG:4326
└── comuna={cod}.csv        # CSV: datos tabulares (~90 columnas)
```

Cada fila del GeoParquet es un predio con:
- Todas las columnas tabulares del API SII
- `geometry`: poligono predial EPSG:4326
- `_match_method`: metodo que logro el match
- `_poly_idx`: indice del poligono vectorizado

Filas con `_match_method = "unmatched_polygon"` son poligonos sin dato tabular.

---

## Limitaciones conocidas

1. **~1% de predios sin geometria**: predios en manzanas sin coordenadas donde OCR no recupera match.

2. **Poligonos multi-unidad**: un poligono de edificio se asigna a todos sus departamentos. La geometria es identica — el WMS del SII no renderiza subdivision horizontal.

3. **Comunas con timeout en OCR**: comunas rurales grandes (>50K supercells) pueden exceder el timeout de 2 horas en la fase OCR. Se procesan manualmente sin timeout.

4. **Trehuaco (8108)**: comuna creada en 2020, no existe en el shapefile BCN. Requiere poligono actualizado.

---

*Pipeline v3 disenado y ejecutado marzo-abril 2026. Datos reproducibles con los scripts en `code_v3/`.*
