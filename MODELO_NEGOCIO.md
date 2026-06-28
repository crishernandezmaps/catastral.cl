# Catastral.cl — Modelo de negocio (one-pager)

_Documento estratégico. Estado objetivo, no foto del presente._

## Qué somos

El **source of truth predial de Chile como servicio recurrente**. Un único motor de
datos catastrales (SII + CBR + oferta inmobiliaria, ~9,4M predios) que se vende en
tres formas según cuánto quiera construir el cliente. El dolor que resolvemos:
**tener toda la información predial en un solo lugar, fresca y de la forma más expedita.**

## El motor único alimenta todo

```
   PIPELINE CANÓNICO  →  master_<periodo>.duckdb  (versionado, en S3)
                                 │
        ┌─────────────────────────┼──────────────────────────┐
        ▼                         ▼                            ▼
   apiv2 sirve lookups      réplica descargable          shell + cabezales
   = Producto 1 (API)       = base OLAP del cliente       = Productos 2 y 3
```

Regla: el master se construye **una vez** y se distribuye como artefacto. Las apps de
cliente NO reconstruyen su propia copia (eso genera divergencia de frescura). El build
se hace en VPS propia y se traspasa al cliente — eso asegura el cobro del setup.

## Los tres productos

| | **1 · API** | **2 · Módulos de App** | **3 · Llave en mano** |
|---|---|---|---|
| **Para quién** | El que construye su propia app (equipos con devs) | El que arma su app con piezas pre-hechas | El que no quiere construir nada |
| **Qué entrega** | apiv2 + MCP por API key (lookups en vivo) | Shell (visor + ficha + export) + cabezales + réplica del master | Plataforma completa, brandeada y hosteada |
| **Libertad** | Total | Media (ensambla) | Baja (la usa) |
| **Cobro** | Free → empresarial negociado | Setup + recurrente modular (ver abajo) | Setup + recurrente (techo de precio) |

## Estructura de precios (en UF)

**Recurrente = base 30 UF + 10 UF por cabezal activo.**

**Setup = 5 × el recurrente mensual del paquete** (≈ 42% del ARR, bajo el techo "sano"
de 50%). Se cobra repartido en los 3 meses de implementación.

| Cabezales | Recurrente/mes | ARR (×12) | Setup (5×) | Setup como UF/mes × 3 | Setup / ARR |
|---|---|---|---|---|---|
| 1 | 40 | 480 | 200 | ~67 | 42% |
| **2 (típico)** | **50** | **600** | **250** | **~83** | **42%** |
| 3 | 60 | 720 | 300 | 100 | 42% |
| 4 | 70 | 840 | 350 | ~117 | 42% |

**Por qué la fórmula:** atar el setup al paquete (no a un número fijo) mantiene la
relación setup:ARR sana de forma automática y escala con el tamaño del cliente. Un
setup plano de 360 UF solo es "sano" para clientes de 3+ cabezales; para 1–2 cabezales
violaría la regla del 50% y daría sabor consultoría.

**Regla de oro:** el setup es termómetro, no termostato. El recurrente se fija por
**valor + costo**, no para cuadrar contra el setup. La relación sana es consecuencia.

## Clientes actuales: grandfathering

Los clientes vigentes (25 UF/mes "todo incluido") **no suben a 50 de golpe** (+100% es
excusa para irse o renegociar a la baja). Mantienen condición o suben escalonado con
aviso. El modelo modular (base 30 + cabezales) aplica a **clientes nuevos y a upsells**.

## Embudo de la API: free self-service → ventas empresariales

El free **no genera soporte**: es autoservicio puro y su único trabajo es captar y
**calificar** leads sin gastar horas. Al topar el límite, el lead se deriva a venta
empresarial (única instancia con contacto humano). **No hay tier de pago self-service.**

| Nivel | Acceso | Soporte | Cobro |
|---|---|---|---|
| **Free** | Descarga datos (catastral.cl) + API/MCP 100/día | Ninguno | $0 (lead magnet) |
| **Empresarial** | Volumen, datos completos/frescos, SLA | Humano | Negociado |

**Dónde poner la línea del free (para no canibalizar):** muestra/1 comuna/snapshot con
desfase gratis; nacional completo + frescura continua + OLAP + SLA = pago. La vieja
venta de datasets por comuna deja de ser negocio y pasa a ser anzuelo gratis.

### Pricing de la API: suscripción plana, NO precio por consulta

La competencia cobra ~**0,02 UF por consulta** (1 UF / 50 búsquedas ≈ USD 0,83 c/u),
modelo transaccional. **No competimos en esa métrica — la cambiamos.** Cobramos
**suscripción plana** (acceso amplio + frescura), piso ~40 UF/mes. Bajar nuestro precio
por consulta sería autogol: nos comoditiza, dispara guerra de precios y ancla el mercado
abajo. En cambio, a cualquier volumen real el modelo por-consulta del competidor explota
frente a nuestra estructura free + plano. **El competidor es nuestro folleto de ventas:**

| Consultas/mes | Competidor (0,02 UF c/u) | Catastral |
|---|---|---|
| 3.000 | 60 UF/mes | **$0** (free tier, 100/día) |
| 10.000 | 200 UF/mes | suscripción plana (~40–60 UF) |
| 100.000 | 2.000 UF/mes | misma suscripción plana |

Dato clave: el free tier (100/día = 3.000/mes) **ya regala lo que el competidor cobra a
60 UF/mes**. El free es arma de penetración (mata al competidor abajo, sin canibalizar el
negocio que es volumen/OLAP), no una rebaja del unitario. Para análisis masivo el
por-consulta es absurdo (cruzar 100K predios = 2.000 UF en consultas): _"ellos te cobran
por gota; nosotros te damos la llave del estanque."_ Argumento de venta: _"a tu volumen
N, con ellos pagas X; con nosotros fijo, sin tope, y con la base completa para OLAP."_

## Cabezales activables (motor de expansión de cuenta)

Cada cabezal es un switch on/off por cliente, a **10 UF/mes** cada uno. Habilita
**land-and-expand**: entra barato, crece sin nuevo setup. Objetivo: **Net Revenue
Retention > 100%**.

Catálogo (del inventario real de las apps): Workbench SQL + Flujos · Valoración UF +
comparables · Scoring seguros / anomalías · Optimizador de rutas A* · Detección
satelital de invasión · CBR histórico · Oferta inmobiliaria.

_Nota: los cabezales de alto valor (p. ej. scoring que genera leads de ahorro
tributario) pueden revisarse a un precio premium o por valor más adelante; hoy todos a 10 UF._

## Economía: por qué el recurrente manda

| | Hoy (25 UF, setup 255) | Modelo (típico 2 cabezales) |
|---|---|---|
| Setup | 255 UF | 250 UF |
| ARR | 300 UF | 600 UF |
| **LTV a 3 años** | ~1.155 UF | **~2.050 UF** (+78%) |
| Peso del recurrente en el LTV | 78% | **88%** |
| Costo de producir el setup N | alto (se reconstruye) | decreciente (se ensambla) |

El setup se mantiene donde está; lo que cambia es **modularizar y subir el recurrente**,
atándolo a frescura del dato + cabezales encendidos. La infraestructura es **costo
propio a optimizar**, no una línea que se pasa al cliente.

## Lo que hay que construir para que el modelo funcione

1. **Pipeline canónico → artefacto master versionado en S3.** Un solo build distribuido
   como réplica. (Base ya existente: `evans_build/predios_chile_<periodo>.duckdb`.)
2. **Feature flags por tenant en el shell.** Cobrar por cabezal activable exige que cada
   módulo sea un switch por cliente, no un desarrollo.
3. **Shell estándar único** (base de Póliza Gestión: MapLibre + Workbench + Flujos +
   Export + master read-only + capa privada por cliente).
4. **Aislamiento de clientes** (sobre todo seguros) respecto de proyectos internos.

## Decisiones abiertas (pendientes de cerrar)

- **Piso de costo de infra por cliente** — para confirmar que 50 UF/mes deja margen sano.
- **Piso exacto de la suscripción API** — modelo definido (plana, ~40 UF/mes); falta
  fijar el número final (idealmente contra el volumen real de Renta Nacional).
- **Calibrar el límite del free** — 100/día = 3.000/mes es generoso; confirmar que ningún
  caso de uso empresarial real quepa dentro del free.
- **Cabezales premium por valor** — si scoring u otros se cobran sobre el valor generado.

## Métrica #1 a vigilar

**Net Revenue Retention** (¿renuevan la licencia de 12 meses y activan más cabezales?).
Negocio iniciado ~marzo 2026, aún en implementación — todavía sin datos de renovación.
