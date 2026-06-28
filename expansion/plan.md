Para facilitar tu expansión estratégica con **Terminal Catastral**, he estructurado este documento técnico que servirá como hoja de ruta para la obtención de datos y el despliegue del modelo de *embeddings* y señales urbanas en los mercados más dinámicos de la región.

---

# 📍 Estrategia de Expansión: Terminal Catastral LatAm

Este documento detalla la unidad mínima de análisis, las fuentes de datos y las consideraciones críticas para replicar el modelo de inteligencia inmobiliaria en **Colombia, México y Perú**.

## 1. 🇨🇴 Colombia: El Mercado de Mayor Viabilidad

Colombia posee la infraestructura de datos espaciales más madura de la región (modelo LADM-COL), lo que permite una transición casi directa desde el modelo chileno.

| Ciudad | Fuentes de Datos Clave | Unidad Mínima (Geometría) | Atributos Disponibles |
| --- | --- | --- | --- |
| **Bogotá** | [IDECA](https://www.ideca.gov.co/) (Infraestructura de Datos Espaciales) | **Predio / Lote** | Estrato socioeconómico, avalúo, uso de suelo, área construida, materialidad. |
| **Medellín** | [Catastro Medellín](https://www.medellin.gov.co/geomedellin/) / GeoMedellín | **Predio** | Información predial detallada, capas de riesgo y servicios. |

* **Consideración de Precios:** El equivalente al CBR es la **SNR (Superintendencia de Notariado y Registro)**. Aunque es menos abierta, la "Ventanilla Única de Registro" (VUR) es el punto de entrada para datos transaccionales.
* **Señales Urbanas:** Bogotá es ideal para el modelo *Diff-in-Diff* por la construcción en curso de la **Línea 1 del Metro** y la expansión de troncales de TransMilenio.

---

## 2. 🇲🇽 México: El Mercado de Mayor Volumen

México es el mercado más grande, pero la información está fragmentada. La clave aquí es la **imputación de datos** desde niveles estadísticos hacia el predio.

| Ciudad | Fuentes de Datos Clave | Unidad Mínima (Geometría) | Atributos Disponibles |
| --- | --- | --- | --- |
| **CDMX** | [SIG CDMX](https://sig.cdmx.gob.mx/) / [Portal Datos Abiertos](https://datos.cdmx.gob.mx/) | **Predio / Cuenta Catastral** | Año de construcción, niveles, superficie, valor unitario de suelo. |
| **Monterrey** | Visores Urbanos municipales (San Pedro Garza García) | **Lote** | Zonificación de alta gama, densidades, usos de suelo permitidos. |

* **Unidad Mínima Detallada:** INEGI provee el "Marco Geoestadístico". Si el dato predial es escaso, la unidad de análisis para los *embeddings* debe ser la **Manzana** o el **AGEB** (Área Geoestadística Básica).
* **El Reto del "On-Market":** Debido a la opacidad del Registro Público de la Propiedad (RPP), el modelo deberá entrenarse fuertemente con **web scraping histórico** de portales como Inmuebles24 o Lamudi para suplir la falta de un "CBR" abierto.

---

## 3. 🇵🇪 Perú: El Mercado de Concentración Urbana

En Perú, el mercado inmobiliario formal está extremadamente concentrado en Lima, lo que simplifica el enfoque geográfico pero complica el técnico por la fragmentación distrital.

| Ciudad | Fuentes de Datos Clave | Unidad Mínima (Geometría) | Atributos Disponibles |
| --- | --- | --- | --- |
| **Lima Metropolitana** | [IMP](https://www.imp.gob.pe/) / [SISMET](https://www.google.com/search?q=http://sismet.imp.gob.pe/) | **Lote Catastral** | Zonificación, parámetros urbanísticos, áreas de tratamiento. |
| **Distritos Top** | Catastros municipales (Miraflores, San Isidro, Surco) | **Lote** | Datos físicos más actualizados que en el resto de la ciudad. |

* **Consideración de Informalidad:** Fuera de "Lima Top y Moderna", el catastro puede no reflejar la realidad construida.
* **Fuentes Transaccionales:** La **SUNARP** maneja los registros. Al igual que en México, la estrategia más escalable es el cruce con datos de portales (Urbania / Adondevivir) para inferir precios de mercado.

---

## 4. Consideraciones Técnicas y de Modelo

### A. La Unidad de Análisis para Embeddings

Para que tu modelo de 128 dimensiones sea consistente, en estos países deberás aplicar una **Estructura de Datos Jerárquica**:

1. **Nivel Predio:** Geometría base y atributos físicos básicos (m2, frente, fondo).
2. **Nivel Manzana (Enriquecimiento):** Inyectar variables sociodemográficas del censo (INEGI/INEI/DANE) como ingreso estimado, nivel educativo y calidad de servicios.
3. **Nivel Barrio (Contexto):** Señales urbanas y densidad comercial (puntos de interés).

### B. El Desafío del Modelo Causal (Diff-in-Diff)

Tu modelo en Chile brilla por los 520k datos del CBR. Para replicarlo en estos países sin un CBR abierto:

* **Estrategia de Proxies:** Utilizar el histórico de **precios de lista (asking price)** capturado mediante scraping. Aunque tiene un sesgo (normalmente un 5-10% sobre el precio de cierre), las curvas de impacto (tendencia) del Metro o infraestructura siguen siendo válidas para medir la plusvalía relativa.

### C. Señales Urbanas Críticas para el Modelo

* **Bogotá:** Metro Línea 1, Regiotram, Corredor Verde Séptima.
* **CDMX:** Ampliaciones del Metro, líneas de Cablebús (impacto en zonas de regeneración), Tren Interurbano.
* **Lima:** Línea 2 del Metro (actualmente en construcción/operación parcial).

---

**Recomendación de inicio:**
Comienza por **Bogotá**. La estructura de datos de **IDECA** es la más compatible con la del **SII** chileno, lo que reducirá drásticamente el tiempo de desarrollo de tu primer MVP fuera de Chile
.
