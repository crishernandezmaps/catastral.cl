# Catastral.cl -- Plataforma de Distribucion de Datos Catastrales

## Contexto

Este documento describe la plataforma web **Catastral.cl**, resultado aplicado del pipeline de inteligencia de datos espaciales desarrollado en la tesis *"Liberacion de Datos Catastrales Publicos en Chile"* (Universidad de Chile, Magister en Ingenieria de Negocios, 2026). La plataforma materializa el objetivo final del trabajo: hacer accesible, consultable y descargable el dataset predial nacional construido por el pipeline `sii_vectorizer`.

El pipeline transforma tres fuentes publicas del SII (archivo TXT semestral, API `getPredioNacional` y servicio WMS) en un dataset georreferenciado de 9.55 millones de predios con geometria vectorial, cubriendo las 347 comunas de Chile. La plataforma Catastral.cl es la interfaz que pone este dataset a disposicion de cualquier actor -- brokers inmobiliarios, investigadores, municipios, periodistas y ciudadanos -- eliminando las barreras tecnicas que la tesis identifica como el problema central.

---

## 1. Arquitectura general

```
VPS 46.224.221.33 (extractor)         VPS 46.62.214.65 (plataforma)
+-----------------------+             +----------------------------------+
|  sii_vectorizer        |             |  Nginx (SSL via Cloudflare)       |
|  (Python 3.11)         |--- S3 ---->|  FastAPI (uvicorn, 1 worker)      |
|  30 tuneles WireGuard  |             |  PostgreSQL 16 + PostGIS (Docker) |
|  GDAL 3.8 / GeoPandas  |             |  React SPA (Vite build)           |
+-----------------------+             +----------------------------------+
```

La arquitectura separa dos dominios:

- **Extraccion y procesamiento** (VPS extractor): ejecuta el pipeline de 8 fases documentado en el Capitulo 3 de la tesis. Produce archivos CSV, GeoJSON y GeoPackage por comuna, almacenados en Hetzner S3 (bucket `siipredios`).
- **Distribucion y comercializacion** (VPS plataforma): sirve la API REST, la base de datos con series historicas, y el frontend web. Consume los archivos generados por el pipeline.

### Stack tecnologico

| Componente | Tecnologia |
|-----------|-----------|
| Backend | FastAPI / Python 3.12 / psycopg3 |
| Frontend | React 19 / Vite 7 / Leaflet / Recharts |
| Base de datos | PostgreSQL 16 + PostGIS 3.5 (Docker) |
| Servidor web | Nginx con Cloudflare Origin Certificate (Full Strict SSL) |
| Modelo de acceso | LinkedIn share anonimo (acceso gratuito via difusion social) |
| Email | Resend API (OTP passwordless, solo admin) |
| Geocoding | HERE API (con rate limiting) |
| Storage | Hetzner Object Storage S3-compatible |
| Dominio | catastral.cl (NIC Chile) |
| DNS/CDN | Cloudflare (proxied) |

---

## 2. Base de datos

La base de datos PostgreSQL contiene tanto los datos catastrales del pipeline como las tablas de comercio de la plataforma.

### Tablas catastrales

| Tabla | Filas | Descripcion |
|-------|-------|-------------|
| `catastro_actual` | 9,407,339 | Periodo vigente (2025-S2), 40 columnas incluyendo `geom` geometry(Geometry, 4326). Corresponde al producto de la Fase 0 del pipeline cargado en PostgreSQL |
| `catastro_historico` | 136,630,730 | 16 semestres (2018-S1 a 2025-S2), 13 columnas. Permite analisis de evolucion de avaluos |
| `comunas_lookup` | 347 | Nombre y region por codigo SII |

Los datos de `catastro_actual` provienen del parseo del archivo `BRTMPNACROL` (Barrera 1 de la tesis) y de la extraccion masiva via API `getPredioNacional` (Barrera 2), enriquecidos con coordenadas, superficie de terreno, superficie construida, valor comercial de suelo por Area Homogenea, codigo de destino predial, y **geometria poligonal vectorizada** (5,671,116 polígonos, 60.3% de cobertura).

### Indices

- `idx_actual_rol` -- UNIQUE (comuna, manzana, predio)
- `idx_actual_direccion` -- GIN trigram para busqueda fuzzy de direcciones
- `idx_actual_coords` -- GIST espacial (PostGIS) para busqueda por coordenadas lat/lon
- `idx_catastro_actual_geom` -- GIST espacial sobre columna `geom` (poligonos)
- `idx_actual_comuna`, `idx_actual_destino`, `idx_actual_avaluo`, `idx_actual_sup`

### Cobertura de geometrias poligonales

Las geometrias de los predios (poligonos vectorizados desde el WMS del SII) fueron cargadas desde los archivos GeoJSON de Fase 6 del pipeline `sii_vectorizer`:

| Segmento | Total | Con geom | Cobertura |
|----------|-------|----------|-----------|
| **Nacional** | 9,407,339 | 5,671,116 | **60.3%** |
| Urbano (U) | 7,285,199 | 4,821,993 | 66.2% |
| Rural (R) | 2,018,772 | 765,565 | 37.9% |
| Habitacional (H) | 5,987,385 | 3,874,400 | 64.7% |
| Agricola (A) | 998,053 | 269,727 | 27.0% |

El 40% sin geometria corresponde principalmente a predios rurales/agricolas sin levantamiento cartografico y unidades dentro de edificios que comparten el poligono del lote padre. Esta cobertura refleja el maximo disponible en los archivos fuente del SII, no una limitacion del pipeline.

### Tablas de acceso y auditoria

| Tabla | Descripcion |
|-------|-------------|
| `users` | Usuarios admin (solo para el owner, OTP passwordless) |
| `share_tokens` | Tokens anonimos generados al compartir en LinkedIn (token UUID, post_url, linkedin_username, downloads_count) |
| `purchases` | Legacy: compras y grants admin (compatibilidad hacia atras) |
| `otp_codes` | Codigos OTP para login admin (TTL 10 min) |
| `domain_grants` | Legacy: whitelist de dominios email (compatibilidad hacia atras) |

---

## 3. Pipeline ETL (carga a PostgreSQL)

El directorio `pipeline/` contiene 8 scripts que descargan los CSVs generados por `sii_vectorizer` desde S3 y los cargan en PostgreSQL:

| Script | Funcion |
|--------|---------|
| `01_download_csvs.py` | Descarga CSVs desde S3 a `/tmp/roles_pipeline` |
| `02_load_latest.py` | Carga periodo actual en `catastro_actual` (9.4M filas) |
| `03_load_historical.py` | Carga 16 periodos historicos en `catastro_historico` (136M filas) |
| `04_build_indexes.py` | Crea indices trigram, espaciales y btree |
| `05_run_all.py` | Orquestador (ejecuta 01-04 en secuencia) |
| `06_load_coordinates.py` | Agrega lat/lon desde CSVs enriquecidos |
| `07_fix_shifted_coords.py` | Corrige coordenadas invertidas en comunas afectadas |
| `08_fix_s3_csvs.py` | Corrige datos desplazados directamente en S3 |
| `09_load_geometries.py` | **Carga poligonos vectorizados desde GeoJSON de S3 a columna `geom`** (streaming con ijson, maneja archivos hasta 11 GB) |

Este pipeline se ejecuta semestralmente, en sincronizacion con la publicacion del nuevo archivo `BRTMPNACROL` por parte del SII.

---

## 4. API REST (Backend)

El backend expone 40+ endpoints organizados en 9 routers, todos bajo el prefijo `/api`.

### 4.1 Endpoints publicos de consulta catastral

Estos endpoints implementan la funcionalidad de exploracion gratuita del dataset. Corresponden al acceso libre que la tesis propone como mecanismo de democratizacion.

**Busqueda de predios:**
- `GET /api/predios?direccion=&comuna=&page=` -- Busqueda por direccion, rol o filtros avanzados
- `GET /api/predios/autocomplete?q=` -- Autocompletado de direcciones (typeahead)
- `GET /api/predios/nearby?lat=&lon=&radius=` -- Busqueda espacial por coordenadas (PostGIS)
- `GET /api/predios/nearby/markers` -- Markers livianos para mapa

**Poligonos vectoriales (nuevos):**
- `GET /api/geojson/predio/:c/:m/:p` -- Retorna el poligono de un predio como FeatureCollection (~1-2 KB)
- `GET /api/geojson/nearby?lat=&lon=&radius=&limit=` -- Hasta 200 poligonos cercanos con color por destino. Usa bounding box GIST filter para performance

**Detalle de predio:**
- `GET /api/predios/:c/:m/:p` -- Detalle completo (39+ columnas: avaluo, destino, superficie, coordenadas, materiales, calidades)
- `GET /api/predios/:c/:m/:p/evolucion` -- Serie historica de 16 semestres (2018-2025)
- `GET /api/predios/:c/:m/:p/edificio` -- Contexto de edificio (multi-unidad)
- `GET /api/predios/:c/:m/:p/edificio3d` -- Datos para visualizacion 3D isometrica

**Catalogos:**
- `GET /api/comunas` -- 347 comunas con nombre y region
- `GET /api/destinos` -- Codigos de destino predial (H, C, I, O, E, etc.)

**Estadisticas:**
- `GET /api/estadisticas/resumen` -- Estadisticas nacionales (cache 1h)
- `GET /api/estadisticas/comunas` -- Estadisticas por comuna (cache 1h)

**Descargas historicas gratuitas:**
- `GET /api/descargas` -- Lista de 16 CSVs historicos disponibles
- `GET /api/descargas/:id/url` -- URL prefirmada S3 (15 min, rate limited)

**Geocoding:**
- `GET /api/geocode?q=` -- Forward geocoding (HERE API proxy)
- `GET /api/revgeocode?lat=&lon=` -- Reverse geocoding

### 4.2 Autenticacion

Sistema passwordless basado en OTP por email, **utilizado unicamente para acceso admin**:

- `POST /api/auth/request-code` -- Envia codigo OTP de 6 digitos por email (Resend)
- `POST /api/auth/verify-code` -- Verifica OTP, emite JWT en cookie httpOnly (24h)
- `POST /api/auth/logout` -- Limpia cookie de sesion

Rate limits: 5 OTP por 15 min por IP, 10 verificaciones por 15 min por IP.

El boton de login no se muestra en la navegacion publica. El admin accede directamente via URL `/admin`, que dispara el modal de login automaticamente.

### 4.3 LinkedIn Share (modelo de acceso)

Sistema anonimo de desbloqueo basado en el "pago con compartir en LinkedIn":

- `POST /api/share/confirm` -- Publico. Valida URL de post de LinkedIn, extrae username (regex sobre `linkedin.com/posts/USERNAME_`), crea token UUID, setea cookie `tremen_share` httpOnly con TTL de 1 año
- `POST /api/share/recover` -- Publico. Recupera token existente por LinkedIn username (para usuarios que limpiaron cookies)
- `GET /api/share/status` -- Retorna `{shared: bool, username?: string}` segun cookie

**Validacion del URL:** regex acepta formatos `linkedin.com/posts/...`, `linkedin.com/feed/update/urn:li:activity:...` y `linkedin.com/pulse/...`. Strips tracking params (UTM, rcm) para deduplicacion.

**Seguridad:** el token UUID se genera con `secrets.token_urlsafe(32)`. El mismo URL de post retorna el mismo token (idempotente). La cookie es httpOnly + SameSite=Lax.

### 4.4 Marketplace (descargas)

- `GET /api/catalog` -- Catalogo publico de 346 comunas
- `GET /api/availability` -- Comunas con datos disponibles en S3 (342 de 345)
- `GET /api/comuna-stats` -- **Nuevo**. Stats por comuna: cobertura_pct, urbano_pct, agricola_pct, geojson_mb (peso del archivo)
- `GET /api/secure-download/:comuna_id` -- URLs prefirmadas S3 (15 min). Requiere cookie `tremen_share`. Incrementa `downloads_count` del share_token
- `GET /api/my-purchases` -- Legacy: compras del usuario autenticado

**Formatos de descarga por comuna:**
- **CSV** -- Tabla plana con todos los atributos del predio
- **GeoJSON** -- Geometria vectorial de poligonos prediales + atributos
- **GeoPackage (GPKG)** -- Formato geoespacial binario, compatible con QGIS y PostGIS

Estos archivos corresponden al output de la Fase 6 del pipeline `sii_vectorizer` (consolidacion final), que integra los datos tabulares de la API del SII con los poligonos vectorizados del WMS, unidos mediante el concepto de `rol_base`.

### 4.5 Administracion

- `GET/POST/DELETE /api/admin/grants` -- Legacy: accesos manuales a usuarios
- `GET/POST/DELETE /api/admin/domain-grants` -- Legacy: acceso por dominio email
- `GET /api/admin/purchases` -- Legacy: historial de compras
- `GET/DELETE /api/admin/users` -- Gestion de usuarios admin
- `GET /api/admin/shares` -- **Nuevo**. Lista todos los LinkedIn shares con stats (total shares, total downloads, promedio)
- `DELETE /api/admin/shares/:id` -- Revoca un share (remueve acceso de ese navegador)

---

## 5. Frontend (React SPA)

La interfaz implementa 10 paginas que cubren los tres niveles de acceso definidos en la tesis: exploracion gratuita, descargas historicas gratuitas, y datos enriquecidos de pago.

### 5.1 Paginas publicas

| Ruta | Pagina | Descripcion |
|------|--------|-------------|
| `/` | Landing | Hero animado SVG, metricas principales (9.4M predios, 343 comunas) |
| `/explorar` | Explorador | Busqueda por direccion/rol con mapa Leaflet interactivo. Muestra **poligonos vectoriales** coloreados por destino + marcadores puntuales. Cascada de fallbacks: busqueda espacial -> texto -> nacional |
| `/buscar` | Busqueda avanzada | Filtros por superficie, avaluo, destino, comuna |
| `/predio/:c/:m/:p` | Detalle de predio | Mapa con **poligono del predio** (auto-zoom al bounds), metadatos SII completos, grafica de evolucion historica (Recharts), visualizacion 3D isometrica del edificio |
| `/estadisticas` | Dashboard | Graficas por region, destino y comuna |
| `/descargas` | Descargas historicas | 16 CSVs semestrales gratuitos (2018-2025, ~22.8 GB total) |
| `/metodologia` | Acceso libre | Documentacion del pipeline y metodologia |
| `/tienda` | Catalogo de descargas | Directorio de 343 comunas con stats por comuna (C=Cobertura%, U=Urbano%, A=Agricola%, Peso del archivo). Sort por cobertura y peso. Share gate en LinkedIn para desbloquear descargas |

### 5.2 Pagina admin (privada)

| Ruta | Pagina | Descripcion |
|------|--------|-------------|
| `/admin` | Panel admin | Tabs: LinkedIn Shares (nuevo), Accesos, Dominios, Compras Flow (legacy), Usuarios |

El admin accede via URL directa `/admin` -- no hay boton de login en la navegacion publica. El modal OTP se abre automaticamente si no hay sesion activa.

### 5.3 Componentes clave

- **AddressSearch** -- Busqueda con geocoding dual (Nominatim + HERE), mapa Leaflet con marker arrastrable, autocompletado SII, deteccion de ROL, capa de poligonos vectoriales
- **PropertyPolygons** -- Componente react-leaflet `<GeoJSON>` que renderiza poligonos con color por destino (H, C, I, O, etc.), tooltips y click-to-navigate
- **ShareToDownload** -- Flujo de LinkedIn share con tabs "Compartir ahora" / "Ya compartiste? Recupera". Muestra texto sugerido con @crishernandezco y @tremen-tech, boton de copiar, deteccion automatica de username del URL, campo manual si no se detecta
- **EvolutionChart** -- Grafica Recharts de evolucion de avaluo fiscal (16 semestres)
- **Building3D** -- Visualizacion isometrica en Canvas del edificio y sus unidades
- **LoginModal** -- Flujo OTP en 2 pasos (email -> codigo, solo para admin)

### 5.4 Estado y autenticacion

- **AuthContext** -- Estado global: `user` (admin, email+rol), `hasShared` (share token valido), `linkedinUsername` (del share). `hasShared` se consulta en mount con `/api/share/status` sin requerir login
- **API client** -- 40+ funciones en `services/api.js`, base URL `/api` (proxy Vite en dev, Nginx en produccion)

---

## 6. Catalogo de comunas

El catalogo (`backend/data/comunas.json`) contiene 346 comunas. El archivo `backend/data/comuna_sizes.json` contiene el peso de cada GeoJSON en S3, precalculado para mostrar en la UI.

### Cobertura actual

- **342 comunas** con archivos CSV + GeoJSON + GeoPackage disponibles en S3
- **343 comunas** mostradas en la plataforma (con at least un formato disponible)
- **3 comunas sin datos**: Antartica (12202), Isla de Pascua (05201), Juan Fernandez (05104)
- **Archivos en S3**: ruta uniforme `s3://siipredios/2025ss_bcn/fase6/comuna={COD_SII}.{csv,geojson,gpkg}`
- **Tamano de archivos GeoJSON**: desde 0.7 MB (Torres del Paine) hasta 11 GB (Las Condes). Mediana ~184 MB

### Stats por comuna expuestas en la tienda

Cada fila del catalogo muestra:
- **C** (Cobertura) -- % de predios con poligono vectorizado
- **U** (Urbano) -- % de predios urbanos
- **A** (Agricola) -- % de predios agricolas
- **Peso** -- tamano del archivo GeoJSON (MB si < 1000, GB si >= 1000)

Todos los headers tienen tooltips custom con CSS. Cobertura y Peso son ordenables.

### Mapeo SII CONARA <-> CUT (INE)

Los archivos en S3 usan codigos SII (CONARA, 4-5 digitos). El catalogo y frontend usan codigos CUT (INE, 5 digitos). El archivo `sii_cut_mapping.json` traduce entre ambos sistemas. Las regiones con diferencias significativas son las creadas post-2007: Nuble (split de Biobio, 2018), Los Rios (split de Los Lagos, 2007) y la Region Metropolitana (SII 13xxx-16xxx -> CUT 13xxx).

---

## 7. Infraestructura y operaciones

### Servidor (VPS plataforma)

- **Proveedor**: Hetzner (Helsinki)
- **IP**: 46.62.214.65
- **SO**: Ubuntu 24.04
- **Recursos**: 32 cores, 122 GB RAM, 150 GB disco
- **PostgreSQL**: Docker (port 5435), 4 CPUs, 8 GB RAM asignados, `shared_buffers=2GB`, `work_mem=64MB`

### Deploy

```bash
cd catastro/infra
./deploy.sh   # Build frontend + rsync backend/pipeline/dist + restart
```

El script:
1. Ejecuta `npm run build` en el frontend (Vite)
2. Sincroniza backend, pipeline y frontend/dist al VPS via rsync
3. Instala dependencias Python si hay cambios
4. Reinicia el servicio `catastro-api` (systemd)
5. Valida y recarga configuracion Nginx

### Nginx

- Reverse proxy: `/api/` -> `http://127.0.0.1:8000` (FastAPI)
- Frontend: sirve archivos estaticos desde `/var/www/catastral.cl/frontend/dist`
- SPA routing: fallback a `/index.html`
- Cache de assets estaticos: 30 dias
- Real IP desde Cloudflare (13 rangos configurados)

### Servicio systemd

```ini
[Service]
User=www-data
WorkingDirectory=/var/www/catastral.cl/backend
ExecStart=/var/www/catastral.cl/venv/bin/uvicorn main:app --host 127.0.0.1 --port 8000 --workers 1
Restart=always
RestartSec=5
```

### Warmup de caches

Al iniciar, la API precalienta:
- Cache de estadisticas (resumen + comunas) -- ~27 segundos
- Total startup: ~30 segundos

### Actualizacion semestral

Cuando `sii_vectorizer` genera nuevos datos:

```bash
# 1. Cargar datos nuevos a PostgreSQL (~25 min)
ssh root@46.62.214.65
cd /var/www/catastral.cl/pipeline
source ../venv/bin/activate
python3 05_run_all.py

# 2. Restart API
systemctl restart catastro-api

# 3. Regenerar estadisticas estaticas
./scripts/refresh-stats.sh --remote

# 4. Rebuild y deploy frontend
cd frontend && npm run build
rsync -avz --delete dist/ root@46.62.214.65:/var/www/catastral.cl/frontend/dist/
```

---

## 8. Performance

| Endpoint | Primera carga | Cacheado |
|----------|--------------|----------|
| `/api/health` | 15ms | -- |
| `/api/predios?direccion=condell` | ~200ms | -- |
| `/api/predios/nearby` | ~50ms | -- |
| `/api/estadisticas/resumen` | ~10s | 14ms (1h TTL) |
| `/api/estadisticas/comunas` | ~11s | 13ms (1h TTL) |
| `/api/availability` | instantaneo | -- |

---

## 9. Relacion con el pipeline sii_vectorizer

La plataforma consume directamente los productos de cada fase del pipeline:

| Fase del pipeline | Producto | Uso en la plataforma |
|-------------------|----------|---------------------|
| Fase 0 (Extraccion API) | CSVs por comuna (7.7 GB) | Cargados en `catastro_actual` via pipeline ETL. Alimentan busqueda, detalle y estadisticas |
| Fase 0 (TXT semestral) | Datos historicos | Cargados en `catastro_historico`. Alimentan graficas de evolucion |
| Fases 1-3 (WMS + vectorizacion) | GeoTIFFs y GeoPackages de poligonos | Integrados en los archivos finales de Fase 6 |
| Fase 4-5 (Spatial join) | GeoJSON con atributos + geometria | Integrados en los archivos finales de Fase 6 |
| **Fase 6 (Consolidacion)** | **CSV + GeoJSON + GPKG por comuna** | **Producto de venta en la Tienda. 342 comunas disponibles para descarga** |
| Fase 7 (QA) | Reporte de alertas | Validacion interna de calidad |

### Que contiene cada archivo descargable

Los archivos de la Fase 6 que se venden en la tienda contienen, por cada predio:

**Identificacion:**
- `v` -- clave unica (comuna|manzana|predio)
- `rol` -- manzana-predio
- `rol_base` -- rol del terreno/edificio padre

**Atributos del TXT semestral (SII):**
- `txt_direccion` -- direccion oficial
- `txt_avaluo_total` -- avaluo fiscal total (CLP)
- `txt_cod_destino` -- codigo de destino (1 digito)

**Atributos de la API `getPredioNacional`:**
- `lat`, `lon` -- coordenadas WGS84
- `valorTotal`, `valorAfecto`, `valorExento` -- avaluos fiscales
- `supTerreno`, `supConsMt2` -- superficies (m2)
- `valorComercial_clp_m2` -- valor comercial de suelo por Area Homogenea
- `destinoDescripcion` -- destino en texto (HABITACIONAL, COMERCIO, etc.)
- `ah`, `ah_valorUnitario` -- Area Homogenea y valor unitario

**Atributos del poligono vectorizado (WMS):**
- `geometry` -- poligono vectorial del lote (EPSG:4326, ~30 cm precision)
- `pol_area_m2` -- area del poligono calculada
- `pol_tipo_predio` -- lote_simple o multi_unidad
- `pol_n_roles_unitarios` -- cantidad de unidades en el edificio

---

## 10. Modelo de acceso: "pay with LinkedIn share"

La plataforma es **100% gratuita** y usa un modelo de "pago con difusion social" en lugar de cobrar dinero. La filosofia: maximizar alcance y posicionar a Tremen como empresa que libera datos publicos.

### Nivel 1: acceso sin fricciones

Todo esto esta disponible sin login ni share:
- Busqueda y consulta de cualquier predio de Chile (9.4M predios)
- Detalle completo con 39+ variables por predio
- Poligonos vectoriales en el mapa (colores por destino, click-to-navigate)
- Series historicas de avaluo (2018-2025)
- Estadisticas nacionales y por comuna
- Descarga de 16 CSVs historicos tabulares (2018-2025, 22.8 GB)

### Nivel 2: descargas por comuna (pay with LinkedIn share)

Para descargar los archivos CSV + GeoJSON + GPKG por comuna, el usuario debe:

1. **Compartir Catastral.cl en LinkedIn** con un post que mencione @crishernandezco y @tremen-tech (texto sugerido con boton copiar)
2. **Pegar el URL del post** que acaba de crear
3. Backend valida el formato del URL (regex sobre dominios de LinkedIn)
4. Si el URL contiene username (`posts/USERNAME_...`), se extrae automaticamente; si no, se pide manualmente
5. Se crea un `share_token` UUID, guardado como cookie httpOnly de 1 año
6. Descargas ilimitadas a las 343 comunas

**Recovery:** si el usuario limpia cookies, puede recuperar su acceso ingresando su LinkedIn username. El backend busca en `share_tokens.linkedin_username` y regresa el mismo token.

**Seguridad:** el sistema es honor-based pero con fricción real. El usuario debe crear efectivamente un post (o reusar uno propio) para obtener un URL valido. No se puede simular sin compartir algo.

### Metricas del admin

El panel admin `/admin` expone en el tab "LinkedIn Shares":
- Total shares acumulados
- Total descargas acumuladas
- Descargas promedio por share
- Tabla con cada share: @usuario (link a perfil), URL del post (link al post), descargas, fecha
- Boton para revocar shares individuales

---

## 11. Seguridad

- **Admin auth**: JWT en cookie httpOnly `tremen_session` (24h). Determinado por email match con variable de entorno `ADMIN_EMAIL`
- **Share token**: UUID en cookie httpOnly `tremen_share` (1 año). Generado con `secrets.token_urlsafe(32)`. Validacion del URL de LinkedIn por regex
- **Rate limiting**: geocoding (10/min, 200/dia por IP), descargas (10/hora por IP), OTP admin (5/15min por IP)
- **SSL**: Cloudflare Origin Certificate, Full Strict mode
- **CORS**: solo origenes autorizados (catastral.cl + localhost:5173 en dev)
- **S3**: URLs prefirmadas con expiracion de 15 minutos

---

## 12. Estructura del proyecto

```
catastro/
+-- backend/
|   +-- main.py                   # FastAPI app + cache warmup
|   +-- config.py                 # Variables de entorno
|   +-- db.py                     # Connection pool (psycopg, 3-10 conexiones)
|   +-- routers/
|   |   +-- predios.py            # Busqueda, detalle, edificios, nearby
|   |   +-- geojson.py            # NEW: Poligonos vectoriales (predio individual + nearby)
|   |   +-- estadisticas.py       # Stats con cache de 1 hora
|   |   +-- descargas.py          # CSVs historicos (presigned URLs)
|   |   +-- geocode.py            # HERE API proxy con rate limiting
|   |   +-- auth.py               # OTP login admin (Resend email)
|   |   +-- sharing.py            # NEW: LinkedIn share/confirm/recover/status
|   |   +-- payments.py           # Legacy: Flow.cl (desmontado, no registrado)
|   |   +-- marketplace.py        # Catalogo, availability, comuna-stats, secure-download
|   |   +-- admin.py              # Grants, users, domains, shares
|   |   +-- health.py
|   +-- lib/
|   |   +-- s3.py                 # S3 client (fase6 catalog, presigned URLs)
|   |   +-- comunas.py            # Catalogo JSON (346 comunas + bundles)
|   |   +-- flow.py               # Flow.cl HMAC client
|   |   +-- email.py              # Resend client
|   |   +-- discounts.py          # Descuentos por volumen
|   +-- middleware/
|   |   +-- auth.py               # JWT desde httpOnly cookie
|   +-- data/
|   |   +-- comunas.json          # Catalogo de 346 comunas
|   |   +-- sii_cut_mapping.json  # Mapeo SII CONARA <-> CUT (INE)
|   |   +-- fase6_catalogo.json   # Rutas S3 de 345 comunas (CSV/GeoJSON/GPKG)
|   |   +-- comuna_sizes.json     # NEW: Peso precalculado de archivos GeoJSON por comuna
|   +-- requirements.txt
+-- frontend/
|   +-- src/
|   |   +-- App.jsx               # Router (10 rutas)
|   |   +-- pages/                # Home, Explorar, Buscar, DetallePredio, Estadisticas, Descargas, Tienda, MisCompras, Admin, AccesoLibre
|   |   +-- components/           # Header, Footer, LoginModal, AddressSearch, RolSearch, EvolutionChart, Building3D, HeroAnimation
|   |   +-- context/AuthContext.jsx
|   |   +-- services/api.js       # 35+ funciones API
|   |   +-- data/comunas.json
|   +-- vite.config.js            # Proxy /api -> localhost:8000
|   +-- package.json              # React 19, Vite 7, Leaflet, Recharts, Lucide
+-- pipeline/                     # ETL desde S3 a PostgreSQL (9 scripts: 01-08 + 09_load_geometries)
+-- infra/
|   +-- nginx.conf                # Reverse proxy + Cloudflare IPs
|   +-- catastro-api.service      # Systemd unit (1 worker)
|   +-- deploy.sh                 # rsync + restart
+-- scripts/
|   +-- refresh-stats.sh          # Regenera JSON estatico de estadisticas
+-- docker-compose.yml            # PostgreSQL 16 + PostGIS
+-- fase6_catalogo.json           # Catalogo maestro de archivos S3
```

---

## 13. Variables de entorno

| Variable | Servicio |
|----------|----------|
| `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASS` | PostgreSQL |
| `JWT_SECRET` | Autenticacion |
| `ADMIN_EMAIL` | Rol admin (cris@tremen.tech) |
| `S3_ENDPOINT`, `S3_REGION`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_BUCKET` | Hetzner S3 |
| `RESEND_API_KEY` | Resend (email OTP admin) |
| `HERE_API_KEY` | HERE (geocoding) |

*Nota: `FLOW_API_KEY` / `FLOW_SECRET_KEY` ya no son necesarios — el modelo de pago con Flow.cl fue reemplazado por el sistema de LinkedIn share.*

---

## 14. Changelog de cambios recientes (2026-04-05)

### Feature 1: Poligonos vectoriales en el mapa

- Columna `geom geometry(Geometry, 4326)` agregada a `catastro_actual` con indice GIST
- Script `pipeline/09_load_geometries.py` carga poligonos desde GeoJSONs de S3:
  - Usa `boto3.download_file` (multipart con reintentos) para archivos hasta 11 GB
  - Parsea con `ijson` en streaming para no cargar archivos enormes en RAM
  - Descarga a `/tmp/geojson_staging/`, elimina tras procesar
- Proceso completo tomo ~5 horas para cargar 5.57M geometrias
- 2 nuevos endpoints: `/api/geojson/predio/:c/:m/:p` y `/api/geojson/nearby`
- Query nearby optimizado con bounding box GIST + `ST_DWithin` (1.7s para 50 resultados)
- Frontend: componente `PropertyPolygons` renderiza poligonos con color por destino en los mapas de `/explorar` y `/predio/:c/:m/:p`

### Feature 2: Modelo de pago con LinkedIn share

- Flow.cl removido como modelo de pago (router desmontado, mantenido en codigo por compat)
- Nueva tabla `share_tokens` (token UUID, post_url, linkedin_username, downloads_count)
- Nuevo router `sharing.py`: endpoints publicos `/share/confirm`, `/share/recover`, `/share/status`
- `secure-download/:comuna_id` ahora valida cookie `tremen_share` (anonimo)
- Frontend: componente `ShareToDownload` con texto sugerido, menciones a @crishernandezco y @tremen-tech, deteccion automatica de username del URL, recovery por username
- Login button removido de navegacion publica — admin accede via URL directa `/admin`
- Tienda reescrita sin carrito ni precios, solo descargas directas
- Admin tab nuevo "LinkedIn Shares" con estadisticas y lista de posts

### Feature 3: Stats por comuna en la tienda

- Endpoint `/api/comuna-stats` retorna cobertura_pct, urbano_pct, agricola_pct, geojson_mb por comuna
- Tabla de tienda muestra columnas C / U / A / Peso con tooltips custom
- Sort clickeable en Cobertura y Peso
- Formato de peso: MB si < 1000, GB si >= 1000

### Open Graph para LinkedIn preview

- Meta tags og:title, og:description, og:image, og:url en `index.html`
- Preview en LinkedIn incluye mencion a Tremen (tremen.tech)
