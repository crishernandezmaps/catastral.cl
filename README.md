# Catastral.cl — Plataforma Unificada de Datos Catastrales

Plataforma que unifica la extraccion, exploracion y venta de datos catastrales del SII de Chile.

**URL:** https://catastral.cl

## Arquitectura

```
VPS 46.224.221.33 (extractor)        VPS 46.62.214.65 (plataforma)
┌─────────────────────┐              ┌──────────────────────────────────┐
│  sii_vectorizer     │              │  Nginx (SSL via Cloudflare)      │
│  (Python)           │──── S3 ────→│  FastAPI (uvicorn, 1 worker)     │
│                     │              │  PostgreSQL 16 + PostGIS (Docker)│
└─────────────────────┘              │  React SPA (Vite build)         │
                                     └──────────────────────────────────┘
```

- **Dominio:** catastral.cl (NIC Chile)
- **DNS/CDN:** Cloudflare (proxied, Full Strict SSL con Origin Certificate)
- **Backend:** FastAPI / Python 3.12 / psycopg3 / boto3
- **Frontend:** React 19 / Vite 7 / Leaflet / Recharts
- **Base de datos:** PostgreSQL 16 + PostGIS 3.5 (Docker)
- **Pagos:** Flow.cl (HMAC-SHA256)
- **Email:** Resend API (OTP passwordless auth)
- **Storage:** Hetzner S3 (bucket `siipredios`)

## Estructura del proyecto

```
unified/
├── backend/
│   ├── main.py                 # FastAPI app + cache warmup
│   ├── config.py               # Variables de entorno
│   ├── db.py                   # Connection pool (psycopg)
│   ├── routers/
│   │   ├── predios.py          # Busqueda, detalle, edificios, nearby
│   │   ├── estadisticas.py     # Stats con cache de 1 hora
│   │   ├── descargas.py        # CSVs historicos (presigned URLs)
│   │   ├── geocode.py          # HERE API proxy con rate limiting
│   │   ├── auth.py             # OTP login (Resend email)
│   │   ├── payments.py         # Flow.cl (create, webhook, cart)
│   │   ├── marketplace.py      # Catalogo, availability, S3 downloads
│   │   ├── admin.py            # Grants, users, domain whitelisting
│   │   └── health.py
│   ├── lib/
│   │   ├── flow.py             # Flow.cl HMAC client
│   │   ├── s3.py               # boto3 S3 con cache de availability
│   │   ├── email.py            # Resend client
│   │   ├── comunas.py          # Catalogo JSON (346 comunas + bundles)
│   │   └── discounts.py        # Descuentos por volumen (5/10/15%)
│   ├── middleware/
│   │   └── auth.py             # JWT desde httpOnly cookie
│   ├── data/
│   │   ├── comunas.json        # Catalogo de 346 comunas + 5 bundles
│   │   └── sii_cut_mapping.json # Mapeo SII CONARA <-> CUT (INE)
│   └── requirements.txt
├── frontend/
│   ├── src/
│   │   ├── App.jsx             # Rutas
│   │   ├── pages/              # 10 paginas
│   │   ├── components/         # Header, Footer, LoginModal, HeroAnimation, etc.
│   │   ├── context/AuthContext.jsx
│   │   ├── services/api.js     # 35+ funciones API
│   │   └── data/comunas.json
│   ├── public/tremen.svg       # Logo/favicon
│   └── vite.config.js          # Proxy /api -> localhost:8000
├── pipeline/                   # ETL desde S3 a PostgreSQL
│   ├── 01_download_csvs.py
│   ├── 02_load_latest.py       # catastro_actual (9.4M rows)
│   ├── 03_load_historical.py   # catastro_historico (136M rows)
│   ├── 04_build_indexes.py     # trigram, spatial, btree
│   ├── 05_run_all.py           # Orchestrador
│   ├── 06_load_coordinates.py  # lat/lon desde CSVs de sii_vectorizer
│   └── sql/
│       ├── schema.sql          # 7 tablas (3 catastral + 4 commerce)
│       └── indexes.sql
├── infra/
│   ├── nginx.conf              # Reverse proxy + Cloudflare IPs
│   ├── catastro-api.service    # systemd unit (1 worker)
│   └── deploy.sh               # rsync + restart
├── scripts/
│   └── refresh-stats.sh        # Regenera JSON estatico de estadisticas
├── docker-compose.yml          # PostgreSQL 16 + PostGIS
└── .env.example
```

## Base de datos

### Tablas catastrales
| Tabla | Rows | Descripcion |
|-------|------|-------------|
| `catastro_actual` | 9,407,339 | Periodo actual (2025-S2), 39 columnas |
| `catastro_historico` | 136,630,730 | 16 semestres (2018-2025), 13 columnas |
| `comunas_lookup` | 347 | Nombre y region por codigo |

### Tablas de comercio
| Tabla | Descripcion |
|-------|-------------|
| `users` | Usuarios (email, passwordless OTP) |
| `purchases` | Compras y grants (entitlements) |
| `otp_codes` | Codigos OTP (TTL 10 min) |
| `domain_grants` | Whitelist de dominios email |

### Indices
- `idx_actual_rol` — UNIQUE (comuna, manzana, predio)
- `idx_actual_direccion` — GIN trigram (busqueda texto fuzzy)
- `idx_actual_coords` — GIST spatial (PostGIS, busqueda por coordenadas)
- `idx_actual_comuna`, `idx_actual_destino`, `idx_actual_avaluo`, `idx_actual_sup`

## Rutas del frontend

| Ruta | Pagina | Acceso |
|------|--------|--------|
| `/` | Landing con hero animado SVG | Publico |
| `/explorar` | Busqueda por direccion/rol/mapa | Publico |
| `/buscar` | Filtros avanzados | Publico |
| `/predio/:c/:m/:p` | Detalle + evolucion + 3D edificio | Publico |
| `/estadisticas` | Graficas por region/destino/comuna | Publico |
| `/descargas` | 16 CSVs historicos gratuitos | Publico |
| `/tienda` o `/comunas` | Catalogo + carrito + pago Flow | Publico (pago requiere login) |
| `/mis-compras` | Dashboard de compras del usuario | Auth |
| `/admin` | Panel admin (grants, purchases, users) | Admin |
| `/metodologia` | Documentacion de acceso libre | Publico |

## API endpoints

### Publicos
- `GET /api/health` — Health check
- `GET /api/predios?direccion=&comuna=&page=` — Busqueda
- `GET /api/predios/:c/:m/:p` — Detalle (39+ columnas)
- `GET /api/predios/:c/:m/:p/evolucion` — Historico 16 semestres
- `GET /api/predios/:c/:m/:p/edificio` — Contexto edificio
- `GET /api/predios/:c/:m/:p/edificio3d` — Datos 3D
- `GET /api/predios/nearby?lat=&lon=&radius=` — Busqueda espacial
- `GET /api/predios/nearby/markers` — Markers livianos
- `GET /api/predios/autocomplete?q=` — Typeahead
- `GET /api/comunas` — Lista de 347 comunas
- `GET /api/comunas/resolve?nombre=` — Nombre -> codigo
- `GET /api/destinos` — Codigos de destino
- `GET /api/estadisticas/resumen` — Stats nacionales (cacheado 1h)
- `GET /api/estadisticas/comunas` — Stats por comuna (cacheado 1h)
- `GET /api/descargas` — Lista de CSVs historicos
- `GET /api/descargas/:id/url` — Presigned URL (15 min)
- `GET /api/geocode?q=` — HERE geocoding proxy
- `GET /api/revgeocode?lat=&lon=` — Reverse geocoding
- `GET /api/catalog` — Catalogo de comunas con precios
- `GET /api/availability` — Comunas con datos en S3 (cacheado 10 min)
- `GET /api/metadata/:id/content` — Metadata de poligonos

### Auth
- `POST /api/auth/request-code` — Enviar OTP por email
- `POST /api/auth/verify-code` — Verificar OTP, emitir JWT cookie
- `POST /api/auth/logout` — Limpiar cookie

### Pagos (requiere auth)
- `POST /api/payment/create` — Pago individual Flow.cl
- `POST /api/payment/create-cart` — Pago carrito (descuentos volumen)
- `POST /api/payment/return` — Redirect post-pago
- `POST /api/webhook/flow` — Webhook confirmacion Flow
- `GET /api/download/:paymentId` — URLs post-pago

### Admin
- `GET/POST/DELETE /api/admin/grants` — Accesos manuales
- `GET /api/admin/purchases` — Compras Flow
- `GET/DELETE /api/admin/users` — Usuarios
- `GET/POST/DELETE /api/admin/domain-grants` — Dominios whitelist

## Mapeo SII CONARA <-> CUT

Los archivos en S3 usan codigos SII (CONARA, 4-5 digitos). El catalogo y frontend usan codigos CUT (INE, 5 digitos). El archivo `sii_cut_mapping.json` traduce entre ambos.

Regiones con diferencias significativas:
- **Region Metropolitana:** SII 13xxx-16xxx -> CUT 13xxx
- **Nuble:** SII 8101-8121 -> CUT 16xxx (split de Biobio 2018)
- **Los Rios:** SII 10101-10112 -> CUT 14xxx (split de Los Lagos 2007)
- **Los Lagos:** SII 10201-10504 -> CUT 10xxx (renumerados)

2 comunas sin datos: Antartica (12202) y Treguaco (16207).

## Operaciones

### Deploy
```bash
cd unified/infra
./deploy.sh  # rsync backend + frontend build + restart
```

### Restart API
```bash
ssh root@46.62.214.65 'systemctl restart catastro-api'
# Tarda ~60s en arrancar (warmup de caches)
```

### Actualizacion semestral (cada 6 meses)

Cuando sii_vectorizer genera nuevos datos para un semestre:

```bash
# 1. Cargar datos nuevos a PostgreSQL (~25 min)
ssh root@46.62.214.65
cd /var/www/catastral.cl/pipeline
source ../venv/bin/activate
source <(grep -v '^#' ../.env | sed 's/^/export /')
python3 05_run_all.py

# 2. Restart API para limpiar caches
systemctl restart catastro-api

# 3. Regenerar JSON estatico de estadisticas (desde tu maquina local)
cd catastro/unified
./scripts/refresh-stats.sh --remote

# 4. Rebuild y deploy del frontend
cd frontend
npm run build
rsync -avz --delete dist/ root@46.62.214.65:/var/www/catastral.cl/frontend/dist/
```

El archivo `frontend/public/stats-resumen.json` contiene las estadisticas
de la cabecera (cards + graficas region/destino). Se sirve como archivo
estatico por Nginx, sin llamar a la API. Debe regenerarse cada semestre.

### PostgreSQL
```bash
# Acceso via docker
ssh root@46.62.214.65 'docker exec -it catastro-db psql -U catastro_app -d catastro'

# O desde host
PGPASSWORD=xxx psql -h 127.0.0.1 -p 5435 -U catastro_app -d catastro
```

### Logs
```bash
ssh root@46.62.214.65 'journalctl -u catastro-api -f'
```

## Credenciales (.env)

| Variable | Servicio | Donde obtenerlo |
|----------|----------|-----------------|
| `DB_PASS` | PostgreSQL | Generada en setup |
| `JWT_SECRET` | Auth | Generada en setup |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | Hetzner S3 | Panel Hetzner |
| `FLOW_API_KEY` / `FLOW_SECRET_KEY` | Flow.cl | Panel Flow |
| `RESEND_API_KEY` | Resend | Panel Resend |
| `HERE_API_KEY` | HERE Geocoding | developer.here.com |
| `ADMIN_EMAIL` | Admin role | cris@tremen.tech |

## Performance

| Endpoint | Primera carga | Cacheado |
|----------|--------------|----------|
| `/api/health` | 15ms | — |
| `/api/predios?direccion=condell` | ~200ms | — |
| `/api/predios/nearby` | ~50ms | — |
| `/api/estadisticas/resumen` | ~10s | 14ms (1h TTL) |
| `/api/estadisticas/comunas` | ~11s | 13ms (1h TTL) |
| `/api/availability` | ~40s | 12ms (10min TTL) |

Caches se precalientan al startup del API (warmup bloqueante ~60s).
