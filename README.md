# Cartografia Predial SII Chile

Pipeline de extraccion, vectorizacion y distribucion de los datos catastrales del Servicio de Impuestos Internos de Chile. Produce un dataset georreferenciado de 9.5 millones de predios con geometria vectorial para las 343 comunas del pais.

**Datos disponibles en:** [catastral.cl](https://catastral.cl)

## El problema

El SII administra el catastro fiscal de todos los predios de Chile: avaluos, superficies, destinos, coordenadas y geometrias poligonales. Estos datos son formalmente publicos (Ley 20.285), pero en la practica son inutilizables para analisis masivo debido a tres barreras tecnicas:

1. **Archivo de ancho fijo.** El Rol Semestral de Contribuciones (`BRTMPNACROL`) es un texto plano de 3 GB sin cabeceras ni delimitadores, con codificacion `latin-1`. Formato de sistemas mainframe de los 80.

2. **API sin consulta masiva.** La API `getPredioNacional` acepta un predio a la vez y bloquea IPs que consultan en volumen. Obtener los 9.5M de predios requiere millones de llamadas individuales.

3. **Geometrias atrapadas en imagenes.** El visor cartografico del SII entrega poligonos solo como imagenes PNG via WMS. No hay formato vectorial descargable.

El costo de esta inaccesibilidad supera los USD 2 millones anuales en transferencias hacia intermediarios cuyo principal valor es superar las barreras tecnicas de formato.

## La solucion

Un pipeline de 5 fases que supera las tres barreras:

| Fase | Que hace | Output | Tiempo |
|------|----------|--------|--------|
| **0** | Extraccion masiva: parseo TXT + 9.5M llamadas API via 30 tuneles WireGuard | CSV por comuna (~90 cols) | ~15 dias |
| **1** | Descarga cartografica WMS: tiles PNG zoom 19 (~0.3 m/pixel), 30 workers | GeoTIFF RGBA por comuna | ~10 dias |
| **2** | Vectorizacion raster: polygonize GDAL, filtrado DN, relleno hoyos WMS | GeoPackage con poligonos | ~2 dias |
| **3** | Join espacial: 6 metodos en cascada (point-in-polygon, nearest, herencia, manzana neighbor, getFeatureInfo, fallback) | CSV + GPKG con datos + geometria | ~3 horas |
| **4** | Enriquecimiento: join con catastro semestral (pisos, materiales, calidades, bienes comunes) | CSV 112 cols + GPKG + CSV raw | ~50 min |

## Resultado

| Metrica | Valor |
|---------|-------|
| Total filas CSV | 11,306,273 |
| Total poligonos | 9,128,582 |
| Predios con datos + poligono | 7,292,537 |
| Comunas cubiertas | 343 / 346 |
| Variables | 112 |
| Precision geometrica | ~30 cm |
| Periodo | 2do semestre 2025 |

### Tres capas por comuna

Para cada una de las 343 comunas se producen tres archivos:

- **GeoPackage (.gpkg)** — Poligonos vectorizados EPSG:4326 con atributos tabulares. Para QGIS, PostGIS, GeoPandas.
- **CSV procesado (.csv)** — Todas las filas (con y sin poligono): 112 columnas incluyendo API SII, catastro semestral, areas homogeneas, observatorio de suelo urbano.
- **CSV crudo (_raw.csv)** — Las 39 columnas originales del archivo `BRTMPNACROL` parseadas.

## Codigo

```
code/
├── 0_get_sii.py              # Fase 0: extraccion datos API SII (30 tuneles)
├── batch_tif_30ns.sh          # Fase 1: orquestador descarga WMS
├── download_chunk.py          # Fase 1: worker de descarga por tunel
├── prepare_tif_queue.py       # Fase 1: genera cola de trabajo
├── comunas.py                 # Lista de comunas + nombres WMS
├── 2_vectorizar.py            # Fase 2: vectorizacion raster
├── batch_vectorize.sh         # Fase 2: batch para 347 comunas
├── 3_join_mejorado.py         # Fase 3: join espacial (6 metodos)
├── featureinfo_worker.py      # Fase 3: worker getFeatureInfo SII
├── batch_join_v2.py           # Fase 3: batch para 347 comunas
├── 4_enrich_catastro.py       # Fase 4: enriquecimiento catastro semestral
├── 5_generate_catalog.py      # Fase 4: genera catalogo JSON
└── METODOLOGIA_PIPELINE_v2.md # Documentacion tecnica completa
```

## Variables principales

| Variable | Fuente | Descripcion |
|----------|--------|-------------|
| `comuna, manzana, predio` | SII | Identificacion unica (rol catastral) |
| `lat, lon` | API SII + BCN | Coordenadas WGS84 |
| `valorTotal` | API SII | Avaluo fiscal total (CLP) |
| `supTerreno` | API SII | Superficie del terreno (m2) |
| `supConsMt2` | API SII | Superficie construida (m2) |
| `valorComercial_clp_m2` | API SII | Valor comercial de suelo por m2 |
| `destinoDescripcion` | API SII | Destino (Habitacional, Comercial, etc.) |
| `ah, ah_valorUnitario` | API SII | Area homogenea y valor unitario |
| `pisos_max` | TXT SII | Pisos construidos |
| `materiales` | TXT SII | Materialidad (A=Acero, B=Hormigon, C=Albanileria, E=Madera) |
| `calidades` | TXT SII | Calidad constructiva (1=Superior a 5=Inferior) |
| `dc_contribucion_semestral` | TXT SII | Contribucion semestral (CLP) |
| `dc_bc1_*` | TXT SII | Bien comun del edificio (lote padre) |
| `pol_area_m2` | Pipeline | Area del poligono vectorizado (m2) |
| `geometry` | Pipeline | Poligono predial (EPSG:4326) |

Ver [METODOLOGIA_PIPELINE_v2.md](code/METODOLOGIA_PIPELINE_v2.md) para la documentacion tecnica completa.

## Justificacion

Este proyecto nace de la tesis *"Datos publicos como catalizador de la industria: el caso de los datos catastrales del Servicio de Impuestos Internos en Chile"* (Universidad de Chile / MIT Sloan School of Management, Magister en Analitica de Negocios, 2026).

La tesis documenta que:

- La brecha entre disponibilidad formal y acceso efectivo genera asimetria de informacion en el mercado inmobiliario chileno.
- Los intermediarios que comercializan estos datos cobran USD 2,000-6,000 por dataset comunal, sin agregar inteligencia — solo superan la barrera tecnica.
- El costo agregado supera USD 2 millones anuales, y concentra la capacidad analitica en los actores mas grandes.
- La liberacion efectiva de datos catastrales permite que PyMEs, investigadores, municipios y emprendedores compitan en igualdad informacional.

Los casos de uso documentados incluyen analisis de plusvalia, prospeccion por destino y superficie, identificacion de suelo subutilizado, due diligence objetivo, e inteligencia de localizacion.

## Marco legal

Los datos catastrales del SII son informacion publica por naturaleza: registros administrativos del patrimonio inmobiliario, sin datos personales protegidos. La Ley 20.285 de Transparencia garantiza el derecho de acceso. Este proyecto estructura y redistribuye datos publicos.

## Infraestructura

- **VPS extractor**: Hetzner, 8 vCPU, 128 GB RAM
- **VPN**: 30 tuneles Mullvad WireGuard en network namespaces Linux
- **Storage**: Hetzner Object Storage S3-compatible (`siipredios` bucket)
- **Distribucion**: [catastral.cl](https://catastral.cl) (FastAPI + React, VPS separado)

## Contacto

Cristian Hernandez — [cris@tremen.tech](mailto:cris@tremen.tech) — [tremen.tech](https://tremen.tech)
