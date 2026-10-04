# Playbook — muestra rápida para un prospecto

_Creado el 2026-10-04 a partir del caso MJAA (inmobiliaria, Providencia). Rutas y hosts en
`CATASTRAL/.claude/infra.md`; aquí va el método._

Cuando alguien escribe «¿puedo probarlo?», la respuesta es una **muestra concreta en el día**:
3 manzanas de su zona (o de una comuna representativa), todas las capas cruzadas por rol,
entregadas como **Excel + PDF**. Nada de sitio web.

## 1. Elegir las manzanas (read-only)

Criterios, en orden:
1. **TGR ya en caché** (`tgr-db.certificados`, ≥ 80 % de los roles): la muestra no gasta.
2. Casas o edificación baja (`pisos_max ≤ 2`): son las candidatas a redesarrollo.
3. Al menos 2 **sociedades** propietarias: se pueden mostrar sin ofuscar.
4. **Normas distintas** entre manzanas: así se ve que el potencial cambia de una cuadra a otra.
5. Transacciones recientes (`escrituras`, desde 2018).

Mostrar las candidatas al usuario antes de construir.

## 2. Extraer (read-only, en la VPS de trabajo)

Un script que imprime JSON a stdout (no deja archivos en la VPS): `predios` 2026S1,
`valor_comercial` 2026S1, `geometria_v4` (con WKB), `escrituras` desde 2018, serie de avalúo y
`certificados` TGR. Modelo: `cotizaciones/mjaa_providencia_2026-10-04/extrae_vps.py`.

## 3. Normativa: Plan Regulador + Ordenanza

- **Zona por predio:** polígonos PRC del OCUC (ArcGIS, `PRC_<Comuna>/FeatureServer/0`, códigos
  `uso/edificación`). Asignar por **mayor solapamiento** del polígono del predio; si toca otra zona
  en más del 10 %, anotarlo.
- **Normas:** `ordenanzas.duckdb.ordenanza_normas` por la parte de **edificación** del código.
- **Verificar cada zona contra el PDF de la ordenanza** y citar artículo y cuadro. Trampas
  conocidas: la densidad extraída por VLM puede venir mal («+20 %»); bajo el código comunal se
  cuelan normas del PRMS de otras comunas; la capa MINVU local puede ser de un PRC anterior.
- Zonas `IP` (patrimonial): sin potencial de redesarrollo.

## 4. Armar el Excel

Hojas: Resumen · Predios (una fila por rol) · Transacciones · Normas por zona · Fuentes.
- **Personas naturales: nombre y RUT ofuscados.** Sociedades completas (RUT ≥ 50 M o razón social).
- `val_com_uf` es **UF/m² de suelo** → mostrar UF/m² y el total (× terreno).
- m² construibles = terreno × constructibilidad (indicativo; decirlo).
- Deuda TGR = cuotas impagas con vencimiento anterior a la fecha del certificado.
- Fechas con formato `dd-mm-yyyy` (minúsculas; en mayúsculas Numbers no lo entiende).
- Transacciones de roles que no están en el catastro = **unidades de proyecto nuevo**: rotularlas,
  son una señal de actividad.

## 5. Mapas y PDF

- Dos mapas sobre **Esri World Imagery**: (1) zonas PRC + deslindes; (2) tipo de propietario,
  deuda y transacciones. Revisar contra la imagen que los polígonos calcen con los lotes.
- PDF (WeasyPrint): qué hacemos · la muestra · mapas · capas · cobertura · modelo de negocio ·
  precios · siguiente paso · bloque Tremen · contacto.

## 6. Qué decir de cada canal (no prometer de más)

| Canal | Qué trae |
|---|---|
| Datos por comuna | **Solo SII + transacciones** |
| Plataforma | Todas las capas (normas, valor de suelo, TGR y CBR como módulos) |
| API | Catastro, polígonos, transacciones y ofertas. **Sin normas ni TGR** |
| Contrato | Lo que el negocio requiera |

- Las transacciones vienen del **F2890 del SII**, no del CBR. El CBR da foja/número/año + titular, a pedido.
- Contacto del propietario: «proveedor externo, solo damos acceso». **No decir «no almacenamos»**
  (la app cachea por usuario); sí «no redistribuimos ni incorporamos a nuestras bases».

## 7. Precios vigentes (2026-10-04)

| Servicio | Precio |
|---|---|
| CBR a pedido (incluye contacto vía proveedor externo) | $800 por consulta |
| TGR masivo | 1 UF por 1.000 roles; 0,90 (≤2.500), 0,80 (≤5.000), 0,70 (≤7.500), 0,60 (≤10.000) UF por mil; tramo aplica a todo el pedido; solo certificados obtenidos; >10.000 se cotiza |
| Resto | ver `MODELO_NEGOCIO.md` |

## 8. Envío

**Solo con OK explícito.** Remitente `cris@tremen.tech`, CC a Cris, adjuntos PDF + Excel. Guardar
todo en `CATASTRAL/cotizaciones/<cliente>_<comuna>_<fecha>/` (fuera de git; archivos 600).
