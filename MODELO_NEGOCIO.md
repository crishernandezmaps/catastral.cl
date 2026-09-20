# Catastral.cl — Modelo de negocio

_Reescrito el **2026-09-19** contra los números reales. La versión anterior describía un
negocio de autoservicio recurrente que no es el que factura: trataba la consultoría como
riesgo a evitar («daría sabor consultoría») mientras esa línea sostenía el 99% de los
ingresos, y fijaba precios de lista por encima de los que efectivamente se cobran. Lo que
sigue parte de la evidencia; donde hay una decisión pendiente queda marcada como tal._

---

## Qué somos

**Los únicos que tienen el catastro predial de Chile con geometría vectorial ligada al
rol** — y construimos sobre eso para quien lo necesita.

El SII publica los atributos (avalúos, superficies, destinos) en un archivo de ancho fijo
que cualquiera baja, y publica las geometrías **solo como imagen** vía WMS. El vínculo
entre el rol y el polígono no existe en forma usable en ninguna otra parte. **Ese join es
el producto**; el resto del catálogo es commodity que acompaña.

Lo dijo un lead que ya tenía el archivo oficial completo:

> «Ya dispongo del Detalle Catastral del SII con roles, superficies, construcciones,
> avalúos y contribuciones. **El dato que me falta es la relación espacial.**»

Y lo confirman los tres contratos vigentes: los tres se cerraron **al enterarse de que
existían los polígonos**, no después de comparar plataformas.

---

## De dónde viene la plata (2026-09-19)

| Cliente | Mensual | Plazo | Qué compró |
|---|---|---|---|
| **Póliza Gestión** | $3,5M (~85,5 UF) | 24 meses | catálogo completo **+ desarrollo de plataforma** |
| **MINVU** | $3,5M (~85,5 UF) | 12 meses | **desarrollo de un servicio** sobre los datos |
| **Grant Thornton** | 25 UF | 12 meses | catálogo completo, sin desarrollo |

**MRR ~196 UF · ARR ~2.350 UF · backlog contratado ~3.376 UF (~$138M CLP).**
Antes: consultorías cerradas por $18M, $16M y dos de ~$5M.

**Todo el autoservicio —tienda de comunas + API Pro— lleva 22,65 UF desde que existe.**
El backlog contratado es **149 veces** eso.

> La conclusión operativa incomoda pero es la que manda: **toda la superficie autoservida
> gobierna ~1% de los ingresos.** No es razón para apagarla (ver «Qué papel cumple el
> autoservicio»), sí para no confundirla con el negocio.

### El precio revelado

Grant Thornton compra **catálogo completo sin desarrollo a 25 UF/mes**. Ese es el precio
que el mercado le pone al dato. Todo lo que Póliza y MINVU pagan por encima es desarrollo:

| Componente | UF/mes |
|---|---|
| Dato (licencia de catálogo) | 50 — 25 % del MRR |
| **Desarrollo** | **146 — 75 % del MRR** |

**El cliente paga 2,4× más por lo que construimos sobre el dato que por el dato.** Esa es
la frase que debe ordenar el pricing, la propuesta comercial y qué se construye.

---

## Cómo llegan (el canal, medido)

**Sitio como vitrina → LinkedIn como alcance → una prueba que demuestra el polígono.**

- **Póliza** vio catastral.cl, ya conocía al equipo, y contrató al saber de los polígonos.
- **MINVU** igual, y entró por **contratación directa**.
- **Grant Thornton** los descubrió **por LinkedIn**, hicieron pruebas, les gustó, firmaron.

**Ninguno de los tres evaluó a la competencia antes de contratar.** No se gana la
comparación: no se participa en ella, porque lo que compran no existe como producto de
nadie. El corolario es que **nunca se ha ganado un cliente quitándoselo a un competidor** —
no es el mecanismo a escalar.

**Lo que funciona en LinkedIn son experimentos abiertos** que la gente comenta y republica.
Filtro para cada uno: *¿alguien podría rehacer esto sin nuestra base?* Si la respuesta es
sí, genera audiencia pero no demanda. **El post tiene que ser la demo del polígono.**

👉 **El free de la API, la tienda y la newsletter NO son el embudo de este negocio.**
Apretar la cuota gratuita no corta la entrada de un Póliza.

### La vía pública vale aparte

El MINVU entró por trato directo. En Chile esa modalidad exige causal justificada, y la
natural acá es **proveedor único**. Si fue esa —queda por confirmar en el acto
administrativo—, existe un precedente **formal, no comercial**, de que nadie más provee
esto. Es replicable con municipios, SERVIU y gobiernos regionales, y es un mercado donde
la competencia de plataformas no está compitiendo.

---

## El motor único alimenta todo

```
   PIPELINE CANÓNICO  →  master_<periodo>.duckdb  (versionado, en S3)
                                 │
        ┌─────────────────────────┼──────────────────────────┐
        ▼                         ▼                            ▼
   apiv2 sirve lookups      réplica descargable          shell + cabezales
   (API/MCP)                (base OLAP del cliente)      (desarrollo sobre el dato)
```

Regla que sigue vigente: el master se construye **una vez** y se distribuye como artefacto.
Las apps de cliente NO reconstruyen su propia copia (genera divergencia de frescura). El
build se hace en VPS propia y se traspasa — eso asegura el cobro del setup.

---

## Las tres líneas reales

| | **A · Licencia de datos** | **B · Datos + desarrollo** | **C · Desarrollo sobre el dato** |
|---|---|---|---|
| **Ejemplo** | Grant Thornton | Póliza Gestión | MINVU |
| **Qué entrega** | catálogo completo, acceso | catálogo + plataforma construida | un servicio hecho a medida |
| **Precio observado** | 25 UF/mes | 85,5 UF/mes | 85,5 UF/mes |
| **Escala** | alta (marginal ≈ 0) | media | baja (depende de horas) |
| **Renovación** | se renueva sola (el dato envejece) | ⚠️ mixta | ⚠️ **tiene final natural** |

**La tensión central del modelo, dicha sin adornos:** lo que escala vale 25 y lo que no
escala vale 85. El mercado paga 3,4× más por lo que consume capacidad.

### El problema de cobrar desarrollo como recurrente

Un contrato de datos se renueva porque el dato se pone viejo. **Un contrato de desarrollo
termina el día que lo desarrollado está listo.** MINVU son 12 meses; al mes 13 la
conversación natural es mantención, que vale una fracción. Puede pasarse de 85 UF/mes a 25,
o a cero, sin que nadie quede descontento.

**Qué lo convierte en recurrente de verdad:** que lo entregado necesite **dato fresco** para
seguir sirviendo. Un servicio que corre sobre un snapshot muerto no necesita renovación; uno
atado a la actualización semestral, sí. Diseñar los entregables así es una decisión de
arquitectura con consecuencia comercial directa.

---

## Precios

### Lista vigente para el dato

**Recurrente = base 30 UF + 10 UF por cabezal activo.** A catálogo completo (7 cabezales)
la lista da **100 UF/mes**.

⚠️ **Los contratos reales están por debajo de la lista propia:** Grant Thornton tiene el
catálogo completo a 25 UF (grandfathered) y Póliza a 85,5 **con desarrollo incluido**. Antes
de cotizar al cliente siguiente hay que decidir si la lista se sostiene o se baja a lo que
el mercado ya paga. **Cotizar con una lista que uno mismo no cobra ancla la negociación
hacia abajo.**

**Setup = 5 × el recurrente mensual del paquete** (≈42% del ARR, bajo el techo sano de 50%),
repartido en los 3 meses de implementación. El setup es **termómetro, no termostato**: el
recurrente se fija por valor + costo, y la relación sana es consecuencia.

### El desarrollo se cotiza aparte, y no se regala dentro del paquete

Es el 75% del ingreso: merece línea propia en la propuesta, con alcance, plazo y
entregables. Meterlo dentro de un «todo incluido» hace invisible lo que más se valora y
vuelve imposible subir el precio del dato después.

### Grandfathering

Los clientes a 25 UF «todo incluido» **no suben a 50 de golpe** (+100% es excusa para irse
o renegociar a la baja). Mantienen condición o suben escalonado con aviso. El modelo modular
aplica a clientes nuevos y a upsells.

### Frente al competidor por-consulta

La competencia cobra ~0,02 UF por consulta. **No competimos en esa métrica — la cambiamos.**
A cualquier volumen real el por-consulta explota contra una plana:

| Consultas/mes | Competidor (0,02 UF c/u) | Catastral |
|---|---|---|
| 3.000 | 60 UF/mes | free / plana |
| 10.000 | 200 UF/mes | plana (~40–60 UF) |
| 100.000 | 2.000 UF/mes | misma plana |

_«Ellos te cobran por gota; nosotros te damos la llave del estanque.»_ Para segmentos
Empresa y Educacional rige cobro por llamada con tramos decrecientes y **techo** USD
1.800/mes ≈ 40 UF (al alcanzarlo pasa a ilimitado): ningún cliente paga más que la plana.
Detalle en `mcp_catastral/docs/PLANES.md`.

---

## Cabezales activables

Cada cabezal es un switch on/off por cliente, a **10 UF/mes**. Habilita land-and-expand:
entra barato, crece sin nuevo setup. Objetivo: **NRR > 100%**.

Catálogo: Workbench SQL + Flujos · Valoración UF + comparables · Scoring seguros /
anomalías · Optimizador de rutas A* · Detección satelital de invasión · CBR histórico ·
Oferta inmobiliaria.

⚠️ **Los cabezales son la parte barata del negocio (el dato), no la cara (el desarrollo).**
El upsell real que han pagado los clientes es más desarrollo, que no escala. Si los
cabezales van a ser el motor de expansión, el catálogo tiene que crecer con lo que se
construye a medida — ver propiedad intelectual.

**El competidor ya tiene este modelo en producción.** Inciti vende módulos activables
(prospección, análisis, reportería, inteligencia de mercado). No somos los primeros en
llegar ahí, y no es donde tenemos ventaja: la ventaja es el polígono.

---

## Propiedad intelectual: la decisión que define si esto escala

**Los datos son propios. El software desarrollado se cede al cliente** (salvo Grant
Thornton).

⚠️ **«Le cambio dos líneas y tengo otro» no resuelve el problema.** Modificar una obra
cedida produce una **obra derivada** del original, y el derecho de transformación se va con
la cesión (Ley 17.336). Lo que sí queda propio y nadie puede reclamar es la **arquitectura,
el método y el know-how** — las ideas no se protegen, solo la expresión. Se puede volver a
resolver el problema; no se puede partir del código entregado.

**En MINVU pesa doble**: al ser sector público, lo desarrollado puede llegar a otros
organismos por transparencia o por decisión del propio ministerio. El mercado natural
siguiente —municipios y servicios públicos— es exactamente el que podría recibirlo sin
pagar.

**Cláusula objetivo para contratos nuevos** (la que ya rige con Grant Thornton): el
proveedor **retiene** componentes preexistentes y genéricos; el cliente recibe propiedad del
**entregable específico** o licencia de uso perpetua. Así el cliente queda cubierto y las
piezas quedan para el siguiente.

**De esto depende todo lo demás:** si cada desarrollo alimenta el catálogo, los contratos
son I+D pagada por el cliente y el cliente N+1 cuesta menos que el N. Si cada uno muere
donde nace, esto es una fábrica de software con un dataset muy bueno como diferenciador.

---

## Riesgos estructurales

- **Concentración.** Póliza y MINVU son el **87% del MRR**; Póliza sola, el 44%.
- **Vencimientos alineados.** MINVU y Grant Thornton vencen el mismo mes (~sept 2027): el
  **56% del MRR** en renovación simultánea. Conviene desfasarlos y abrir la conversación de
  continuidad antes del mes 11, no en el 12.
- **Techo de capacidad.** Si el 75% del MRR son horas, el límite de crecimiento es la
  capacidad de desarrollo, no el mercado. El cliente cuatro no entra hasta que haya tiempo.
- **El foso tiene fecha.** Si la única razón de compra es la exclusividad del polígono, el
  día que el SII publique vectores o alguien replique el pipeline, los contratos quedan
  sostenidos solo por lo construido encima y por la relación. **Los 24 meses de pista
  contratada sirven justamente para volver prescindible esa exclusividad.**

---

## Qué papel cumple el autoservicio

No es el negocio y no es el embudo de los contratos. Pero **sí es la vitrina**: Póliza y
MINVU llegaron habiendo visto catastral.cl. Su trabajo es **demostrar que el dato existe y
es bueno**, no facturar.

| Pieza | Papel |
|---|---|
| Sitio + landing | vitrina; debe gritar el polígono en la primera pantalla |
| API/MCP free | prueba de que el dato es real y consultable |
| Tienda de comunas | degustación pagada; ticket chico y autoservido |
| API Pro (2 UF/mes) | el único self-service de pago que existe |
| Newsletter | ⚠️ no es canal: 4 opt-in de 604 tras la campaña de re-permiso |

**Cuota free: 50 consultas/día** (bajada de 100 el 2026-09-19; las 136 keys free activas
heredan del tier). Con 100 el techo casi no mordía.

⚠️ **El mensaje del sitio no está alineado con lo que cierra contratos.** Hoy comunica «10
millones de predios» y cuatro formas de entrar. Lo que hizo firmar a Póliza y al MINVU es
una sola frase: *tenemos los polígonos del SII, que nadie más tiene*. Esa debería ser el
titular.

---

## Tienda de comunas: precio por tamaño (decidido 2026-09-16)

Reemplaza el «1 UF plana + mínimo 3». Razón: el valor del dato es la **cantidad de
predios**, no la unidad «comuna», y el mínimo de 3 era la barrera de entrada — los carritos
abandonados eran todos de exactamente 3 comunas.

| Tamaño | Corte (predios z16) | UF + IVA |
|---|---|---|
| Chica | < 10.000 | 0,5 |
| Mediana | 10.000 – 25.000 | 0,7 |
| Grande | > 25.000 | 1,0 |

- **Sin mínimo** (1 comuna chica ≈ $24.000: la degustación pagada). Tope 50 comunas; sobre
  eso, cotización directa.
- El corte de tamaño es el **mismo** de los créditos del Plan Datos (`creditos.tier`).
- **Descuento por cantidad**: 3–4 −5% · 5–9 −10% · 10–19 −15% · 20–29 −20% · 30–50 −25%.
- ⚠️ Deuda: la chica suelta (0,5 UF) queda más barata por unidad que el crédito del Plan
  Datos. Recalibrar con ventas reales.
- Implementación: `app_catastral/backend/app/ventas.py`; el frontend de `catastralV2` pinta
  desde `/ventas/precios`.

### Plan Datos como billetera (2026-09-16)

**3 UF/mes = 4 UF de tienda al mes** («pagas 3, descargas 4») al precio por tamaño vigente
(fuente única `creditos.PRECIO_UF_TIER`), goteo mensual no acumulable, la repetida no se
re-cobra. El diferencial es la **actualización semestral del acumulado** mientras esté
vigente. Mínimo 3 meses, trimestre up front −10% (8,1 UF). Sin suscriptores al reformar
(`subscriptions` vacía): sin migración.

### Cómo se cobra, en la práctica

**El 73% de lo facturado no pasó por el carro.** De $1.102.570 cobrados, $292.108 entraron
por Flow y **$810.462 por transferencia** — y los dos tickets más grandes (9 UF y 7,65 UF)
fueron los de transferencia. **El carro sirve para la compra chica; sobre cierto monto la
gente conversa y transfiere.** Conviene tratar la transferencia como camino de primera
clase, con instructivo y conciliación, y no como excepción que se regulariza a mano
escribiendo `transferencia` en el campo de la orden de Flow.

---

## Decisiones abiertas

- **¿La lista de 100 UF se sostiene o se baja a lo que el mercado paga?** Hoy se cotiza con
  precios que no se cobran.
- **¿El desarrollo se cotiza como línea separada del dato en la próxima propuesta?**
  Recomendado: sí. Es el 75% del ingreso y hoy va implícito.
- **Cláusula de propiedad intelectual en contratos nuevos** — adoptar el molde de Grant
  Thornton (retener componentes genéricos). Y revisar el alcance real de la cesión en los
  contratos ya firmados.
- **Confirmar la causal del trato directo del MINVU.** Si fue proveedor único, es argumento
  formal replicable con el resto del sector público.
- **¿Qué se hace con la tienda?** La versión anterior de este documento decía que la venta
  por comuna «deja de ser negocio y pasa a ser anzuelo gratis», pero la tienda cobra. Hay
  que cerrar la contradicción: o es anzuelo gratis, o es producto.
- **Correo de seguimiento de la newsletter** — la campaña prometió «si no haces nada, no
  vuelves a recibir correos nuestros» y solo 4 dieron opt-in. Ver `catastralV2/TODO.md`.
- **Cumplimiento Ley 21.719** (vigente 2026-12-01): desaparece la excepción de «fuente
  accesible al público»; documentar base de licitud y EIPD. Ver
  `mcp_catastral/docs/MARCO_LEGAL.md`.
- **Cabezales premium por valor** — si scoring u otros se cobran sobre el valor generado.

### Cerradas

- ~~Piso de costo de infra por cliente~~ — **2026-08-27:** USD 60/mes fijos (VPS Hetzner 50
  + S3 10) para toda la operación, no por cliente. Margen sobre 50 UF/mes >95%; el límite
  real es capacidad de servidor, no costo.
- ~~Piso exacto de la suscripción API~~ — **2026-08-27:** piso USD 200/mes (Empresa), techo
  USD 1.800/mes ≈ 40 UF.
- ~~Calibrar el límite del free~~ — **2026-09-19:** bajado a 50/día.

---

## Métricas a vigilar

1. **Renovación de los contratos grandes.** Es el 99% del ingreso y todavía no hay un solo
   dato de renovación: el primero llega ~sept 2027, con dos contratos venciendo juntos.
2. **Conversaciones comerciales originadas en LinkedIn.** Es el canal probado y hoy no se
   mide. Registrar qué publicación originó cada conversación (la tabla
   `seguimiento_contactos` ya existe) diría en tres meses qué experimento trae clientes y
   cuál solo trae aplausos.
3. **Proporción dato/desarrollo en el MRR.** Si el desarrollo sigue creciendo como
   porcentaje, el negocio se aleja de la escala; si el dato crece, se acerca.
4. **NRR** — ¿renuevan y activan más cabezales? Sin datos aún.

_Los hallazgos con su evidencia, y de dónde sale cada cifra de este documento, están en
`INSIGHTS_CLIENTES.md`. Los reportes periódicos los genera `/mercadoCatastral`._
