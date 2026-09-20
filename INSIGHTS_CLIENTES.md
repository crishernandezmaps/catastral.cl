# Catastral.cl — Qué quieren los clientes

_Documento vivo. Cada entrada dice **de qué evidencia sale**, para poder discutirla o
descartarla cuando llegue dato nuevo. Lo alimenta `/mercadoCatastral`._

> **Sin datos personales.** Acá no van correos, nombres ni RUT de clientes — el mismo
> criterio que dejó `leads_after_closing/` fuera de git. Los hallazgos se describen por
> comportamiento («el cliente de 28 días activos»); quien necesite la identidad la saca
> corriendo la consulta contra `acceso.sqlite`, que es donde corresponde que viva.

---

## 1. El producto no es el dato: es el join

**El hallazgo más importante, y lo dijo un cliente, no nosotros.** Un desarrollador de
plataforma territorial escribió, pidiendo acceso:

> «Ya dispongo del archivo oficial de Detalle Catastral del SII del primer semestre de
> 2026, con roles, superficies, construcciones, avalúos y contribuciones.
> **El dato que me falta es la relación espacial.**»

Los atributos del predio son públicos y gratis: el SII los publica en el archivo de ancho
fijo. Lo que no existe en ninguna parte en forma usable es **el vínculo entre el rol y el
polígono**. Eso es lo que vendemos, y es lo único que un tercero no puede conseguir por
otro lado.

**Implicancia de marketing:** comunicar «10 millones de predios» es hablar de volumen, y
el volumen no convence a quien ya tiene el archivo del SII. El mensaje que sí muerde es
*el SII te da los números, nosotros te decimos dónde están*.

---

## 2. Las cinco preguntas que le hacen al predio

De la lectura de las **47 solicitudes con texto libre** (campo `necesidad`/`proyecto` de
`solicitudes`, al 2026-09-19). Casi nadie quiere «el predio»: lo usan como eslabón.

| Pregunta | Cómo la dicen | Quién la hace |
|---|---|---|
| **Dónde está el borde exacto** | «los polígonos y límites exactos de cada vivienda», un KMZ para Google Earth, la capa para QGIS/ArchiCAD | arquitectura, SIG, municipios |
| **Quién es el dueño** | «saber los propietarios», «identificación de los propietarios» (MOP, concesiones), cómo «se atomiza la propiedad indígena» | legal, expropiaciones, inmobiliario |
| **Cuánto vale y qué dice del territorio** | el avalúo como «indicador económico de la comuna», «variación de la tasación fiscal por manzana», valores hedónicos | academia, política pública |
| **Qué se puede hacer en ese suelo** | estudio de riesgo del plan regulador, «áreas de oportunidad», catastro de uso de suelos | urbanismo, inversión |
| **Qué hay alrededor** | «los roles cercanos a un proyecto bajo estudio ambiental», buffer de 500 m a los parques, «el área aledaña al Parque Cerro Castillo» | ambiental, academia |

**La asimetría que importa:** los académicos (30 de 47) quieren el predio como unidad de
análisis **agregado** — promedios por manzana, patrones por sector — y no pagan. Las
empresas (12) lo quieren como unidad de **acción**: a quién le compro, a quién expropio,
dónde desarrollo. El predio individual solo importa cuando hay que ir a tocar una puerta.

---

## 3. Stock contra flujo: por qué la descarga no tiene recompra

**La evidencia más accionable del 2026-09-19.**

El catastro del SII es **stock**: un retrato que cambia dos veces al año. Se compra una vez
y sirve para el proyecto entero. La falta de recompra en la tienda **no es un defecto del
empaque** — no se arregla bajando el precio ni vendiendo por polígono. Es la naturaleza
del dato.

Las **ofertas del mercado** y las **escrituras del CBR** son flujo: cambian cada semana y
la respuesta de hoy no sirve el mes próximo. Por eso la API retiene donde la descarga no.

Lo confirma el uso real (`usage_daily`, ~2 meses):

| Endpoint | Requests | Clientes distintos |
|---|---|---|
| `predio` (ficha por rol) | 2.569 | 40 |
| `comuna` | 2.519 | 16 |
| **`cbr.cerca`** (escrituras cercanas) | **1.754** | **13** |
| `buscar` | 1.065 | 30 |
| `oferta.cerca` | 354 | 10 |

El **tercer endpoint más usado es el registro del Conservador**. El predio se usa como
llave para llegar al titular y a la transacción.

Caso testigo: el cliente de una consultora jurídica declaró en su solicitud querer
«extraer información predial a lo largo de Chile», pero su endpoint más usado **no es
`predio`** sino `oferta.cerca` (259 req), seguido de `predio` (179) y `cbr.cerca` (124).
Su pregunta real es *«¿qué se vende cerca de este punto y quién es dueño alrededor?»*.
Eso es prospección inmobiliaria, no catastro. **Lo que el cliente declara y lo que
consulta no coinciden — la conducta manda.**

👉 **Estamos cobrando por el producto que por definición se compra una sola vez, y
regalando el que genera hábito.**

---

## 4. Hay recurrencia, hay disposición a pagar, y el checkout se las come

Al 2026-09-19, **12 clientes con actividad en varios días distintos**, el mayor con **28
días repartidos en casi dos meses**. Once de los doce, en tier `free`.

Uno acumuló 1.328 requests en 14 días ≈ **95 por día, con el límite en 100**: está
calibrando su uso para no pasarse del borde. Eso no es un usuario casual, es alguien
trabajando con la herramienta todos los días sin pagar.

**Y hay intención de compra que no se concreta.** El embudo de API Pro (2 UF/mes) tiene
**8 de 9 intentos en `pendiente_pago`** — 16 UF sin cobrar en diez días. Entre los que no
completaron está **el cliente de 28 días activos**, que inició la compra el 13 de
septiembre.

⚠️ **Es abandono, no falla técnica — verificado.** Las diez órdenes, incluidas todas las
pendientes, tienen su `flow_token` de 40 caracteres: el sistema creó la orden, Flow la
aceptó y la persona llegó a la pasarela. El carro funciona. **Lo que la base NO distingue
es si dentro de Flow hubo rechazo de tarjeta o simplemente cerraron la pestaña** — eso solo
se ve en el panel de Flow y **queda por mirar**. Hasta entonces, llamarlo «no pudieron
pagar» es una afirmación sin respaldo.

Sumado a la tienda, el negocio **deja en el camino 1,29 UF por cada UF que cobra** (29,18
abandonadas contra 22,65 cobradas).

**Dos lecturas más del embudo Pro:** siete de los nueve que abandonaron **nunca habían
usado la API** — llegan a pagar sin haber probado, lo que sugiere que el flujo los empuja
antes de tiempo o que venían a comprar otra cosa. Y el único que sí pagó **quedó servido
como `free` por un hardcode del portal**, tuvo que regularizarse a mano y reintentó el pago
creyendo que no había funcionado: casi se le cobra dos veces.

**Implicancia y qué se hizo:** con el carro funcionando, lo que faltaba era una razón más
fuerte para completar el pago. El **2026-09-19 la cuota free bajó de 100 a 50/día**: con
100 el techo casi no mordía (6 días-cliente lo superaron en 60 días, contra 50 días-cliente
en el tramo 51-100) y nueve clientes distintos quedaban clavados en 100 exactos, la firma
de alguien que quiso seguir. Queda por medir si eso convierte o solo ahuyenta.

Siguiente paso natural: separar la cuota **por tipo de pregunta** y no solo por volumen —
la ficha del predio como gancho, y lo que es flujo (CBR y ofertas) con cuota corta en free.

---

## 5. Señales sueltas que conviene no perder

- **El mínimo de 3 comunas funcionó como techo, no como piso:** 6 de las primeras 10
  ventas fueron de exactamente 3 comunas. Corregido en septiembre con precio por tamaño
  (1 / 0,7 / 0,5 UF) y sin mínimo — queda por medir el efecto.
- **La mitad de los carros no se paga:** 5 de 10 ventas quedaron en `pendiente_pago`.
  Ningún producto nuevo arregla eso; si la fuga está en el checkout, se arrastra a lo que
  venga.
- **La newsletter no es canal:** de 604 contactos con relación previa —gente que descargó
  datos—, solo 4 dieron opt-in explícito tras la campaña de re-permiso. Un catastro se
  compra cuando hay proyecto; entre proyectos no hay nada que conversar. Es una
  característica del negocio, no un fallo del correo.
- **Pregunta frecuente sin producto propio:** *¿qué se puede construir aquí?* Aparece en
  planes reguladores, áreas de oportunidad y uso de suelos. `ordenanzas.duckdb` ya tiene
  280 comunas y la zonificación resuelta por point-in-polygon: el cruce predio × normativa
  está casi armado y no figura en el catálogo con nombre propio.

---

## Preguntas abiertas

- [ ] De los que pagaron, ¿cuántos vuelven? **Al 2026-09-19 la tienda lleva 2 semanas: es
      muy pronto para concluir.** Volver a medirlo con 3 meses de historia.
- [ ] ¿Convierten los que rozan el techo del free si se les ofrece el plan Pro, o
      simplemente se van?
- [ ] ¿Cuánto del uso de `cbr.cerca` es exploración y cuánto trabajo facturable del
      cliente?
