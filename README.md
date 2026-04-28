# Cartografia Predial SII Chile

Pipeline de extraccion, vectorizacion y distribucion de los datos catastrales del Servicio de Impuestos Internos de Chile. Produce un dataset georreferenciado de 9.5 millones de predios con geometria vectorial para las 346 comunas del pais.

**Datos disponibles en:** [catastral.cl](https://catastral.cl)

## El problema

El SII administra el catastro fiscal de todos los predios de Chile: avaluos, superficies, destinos, coordenadas y geometrias poligonales. Estos datos son formalmente publicos (Ley 20.285), pero en la practica son inutilizables para analisis masivo debido a tres barreras tecnicas:

1. **Archivo de ancho fijo.** El Rol Semestral de Contribuciones (`BRTMPNACROL`) es un texto plano de 3 GB sin cabeceras ni delimitadores, con codificacion `latin-1`. Formato de sistemas mainframe de los 80.

2. **API sin consulta masiva.** La API `getPredioNacional` acepta un predio a la vez y bloquea IPs que consultan en volumen. Obtener los 9.5M de predios requiere millones de llamadas individuales.

3. **Geometrias atrapadas en imagenes.** El visor cartografico del SII entrega poligonos solo como imagenes PNG via WMS. No hay formato vectorial descargable.

El costo de esta inaccesibilidad supera los USD 2 millones anuales en transferencias hacia intermediarios cuyo principal valor es superar las barreras tecnicas de formato.

## La solucion

Un pipeline integrado que ejecuta descarga tabular, descarga cartografica WMS, vectorizacion y match espacial en un solo paso por comuna:

| Paso | Que hace | Output |
|------|----------|--------|
| **1. Datos tabulares** | 9.5M llamadas API SII via 70 tuneles WireGuard con flock queue + work stealing | CSV por comuna (~90 cols) |
| **2. Supercells WMS** | Descarga tiles agrupados en supercells 1024x1024 px (4x4 tiles z19) del poligono BCN | PNGs en disco |
| **3. Vectorizacion** | Bloques 16384x16384 px con overlap 1024 px, polygonize Python, merge costuras | Poligonos en memoria |
| **4. Match espacial** | Point-in-polygon + nearest + herencia coord + manzana neighbor + OCR recovery | GeoParquet + CSV |
| **5. Optimize** | Simplificacion de geometrias (1m matched, 5m orphans), make_valid | GeoParquet final |

### Diferencias vs pipeline v1

El pipeline anterior descargaba un GeoTIFF completo por comuna (tiles individuales z19 → ensamblaje rasterio → GDAL polygonize). Esto presentaba tres problemas:

- **Grid lines visibles**: las costuras entre tiles generaban artefactos en la vectorizacion.
- **Archivos de 500MB+**: GeoTIFFs completos consumian disco y RAM innecesariamente.
- **Fases separadas**: descarga, vectorizacion y match eran scripts independientes que requerian coordinacion manual.

El pipeline actual elimina el GeoTIFF intermedio. Descarga solo supercells (1024px) dentro del poligono BCN de cada comuna, vectoriza en bloques grandes con overlap de 1024px que eliminan las grid lines, y ejecuta todo en un orquestador integrado.

## Resultado

| Metrica | Valor |
|---------|-------|
| Total predios CSV | 9,500,000+ |
| Comunas cubiertas | 346 |
| Variables | ~90 |
| Precision geometrica | ~30 cm |
| Periodo | 2do semestre 2025 |

### Dos capas por comuna

Para cada comuna se producen dos archivos:

- **GeoParquet (.parquet)** — Poligonos vectorizados EPSG:4326 con atributos tabulares y columna `geometry`. Para QGIS, PostGIS, GeoPandas.
- **CSV (.csv)** — Todos los predios con datos tabulares (~90 columnas): avaluos, superficies, destinos, coordenadas, areas homogeneas.

## Codigo

```
code_v3/                          # Pipeline v3 (actual)
├── fase0_orchestrator.py         # Orquestador: descarga tabular + WMS + vectorize + match
├── fase0_config.py               # Configuracion: tuneles, S3, paths, WMS overrides
├── fase0_worker.py               # Worker descarga datos tabulares (flock queue)
├── sc_worker.py                  # Worker descarga supercells WMS (flock queue)
├── fase0_selective_tif.py        # Descarga supercells + vectorizacion por bloques
├── vectorize_simple.py           # Vectorizacion de TIF pre-construido (alternativa)
├── fase0_match.py                # Match espacial: point-in-polygon + nearest + herencia
├── fase0_recovery.py             # OCR recovery para predios sin match
├── fase0_merge.py                # Merge JSONs → CSV normalizado
├── fase0_normalize.py            # Normalizacion de columnas API SII
├── optimize_parquet.py           # Simplificacion de geometrias post-pipeline
├── batch_etapa1.py               # Batch etapa 1: descarga secuencial (usa tuneles VPN)
└── batch_etapa2.py               # Batch etapa 2: procesamiento paralelo (3 workers CPU)

code/                             # Pipeline v1 (legacy, referencia)
├── 0_get_sii.py                  # Fase 0 original
├── batch_tif_30ns.sh             # Fase 1 original: descarga WMS → GeoTIFF
├── 2_vectorizar.py               # Fase 2 original: GDAL polygonize
├── 3_join_mejorado.py            # Fase 3 original: join espacial
├── 4_enrich_catastro.py          # Fase 4 original: enriquecimiento catastro
└── ...
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
| `_match_method` | Pipeline | Metodo de match (contains, nearest, inherit, manzana, ocr, unmatched_polygon) |
| `geometry` | Pipeline | Poligono predial (EPSG:4326) |

Ver [METODOLOGIA_PIPELINE_v3.md](METODOLOGIA_PIPELINE_v3.md) para la documentacion tecnica completa.

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

- **VPS extractor**: Hetzner Cloud, 8 vCPU, 128 GB RAM
- **VPN**: 70 tuneles Mullvad WireGuard en network namespaces Linux
- **Storage**: Hetzner Object Storage S3-compatible (`siipredios` bucket)
- **Distribucion**: [catastral.cl](https://catastral.cl) (FastAPI + React, VPS separado)

## Autor

**Cristian Hernandez** — Fundador de [Tremen](https://tremen.tech), empresa chilena especializada en ciencia de datos geoespaciales e inteligencia de ubicacion. Tremen desarrolla soluciones de analisis territorial, optimizacion de redes y location intelligence para la industria inmobiliaria, retail y energia en Chile.

Este proyecto fue desarrollado como parte del Magister en Analitica de Negocios de la Universidad de Chile y MIT Sloan School of Management (2026).

[cris@tremen.tech](mailto:cris@tremen.tech) · [tremen.tech](https://tremen.tech) · [LinkedIn](https://www.linkedin.com/in/crishernandezco)
