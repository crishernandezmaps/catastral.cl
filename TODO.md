# TODO — catastral.cl

_Cabecera reordenada el **2026-09-19**. Arriba, lo que hay que decidir; abajo, lo técnico._

---

# 🔴 DECISIONES — cerradas el 2026-09-20

Las cinco se decidieron el **2026-09-20**. Resolución y lo que cada una deja vivo:

### 1. Lista de precios → SE MANTIENE, reinterpretada
Las **100 UF/mes son una meta agregada de MRR** del catálogo completo, no un precio a
cobrarle a un cliente. La lista sigue vigente en ese sentido.
⚠️ Lo que esto NO resuelve: el ancla de negociación (Grant Thornton a 25, Póliza a 85,5)
sigue ahí. Al cotizar al **cliente n.º 4** igual hay que fijar el precio de ESE contrato.

### 2. Desarrollo → SE MANTIENE en el paquete; explorar módulos pre-hechos
No se separa como línea propia. En su lugar, explorar **módulos ya construidos y
activables** (TGR, Diario Oficial, CBR) como forma de productizar lo que hoy se vende
como desarrollo a medida.
- [ ] Diseñar el catálogo de módulos pre-hechos (TGR / Diario Oficial / CBR): qué incluye
      cada uno, precio de activación, y qué horas de desarrollo reemplaza. Conecta con
      «Feature flags por tenant» de Productización técnica.

### 3. Propiedad intelectual → SE RETIENE SIEMPRE
Regla sin excepciones para todo contrato nuevo: TREMEN retiene la PI; el cliente recibe
licencia de uso (o cesión solo del entregable específico).
- [ ] Sigue abierto lo de los **contratos ya firmados**: revisar con abogado si la cesión
      alcanza a todo el software o solo a los desarrollos específicos. La regla nueva no
      retroactúa sola.

### 4. Tienda y API → SE DEJAN COMO ESTÁN
Se observa cómo se mueve el mercado antes de tocar nada. La tensión anzuelo/producto se
tolera a propósito; se revisita con los datos de los próximos `/mercadoCatastral`.

### 5. Newsletter → cola cerrada, SIN correo de seguimiento
Todos los correos ya se enviaron (589 de 604; los 15 restantes son `invalido` a propósito).
No hay tanda de seguimiento: la promesa de re-permiso se respeta.
- [x] **Revisión de la lista — adelantada y CERRADA el 2026-09-28** (estaba para el 10-04).
      Conteo en `reportes/newsletter_lista_2026-09-28.md`. **Conclusión: el correo no es canal.**
      La campaña dejó **4 opt-in sobre 589 (0,7%)**, todos en los primeros 4 días y ninguno
      después. Los otros 6 opt-in llegaron por el **formulario de solicitud de licencias**
      (6 en una semana): ese es el canal que suma consentimiento, y es de gente con intención.
      No hay newsletter recurrente que sostener con 10 opt-in. El cron del Mac y la rutina
      cloud de respaldo quedaron desactivados.

---

## 🟠 Verificaciones que abren oportunidad

- [ ] **Confirmar la causal del trato directo del MINVU.** Si fue *proveedor único*, es un
      precedente **formal** replicable con municipios, SERVIU y gobiernos regionales — el
      mercado donde la competencia de plataformas no compite.
- [ ] **El hardcode del portal**: un cliente pagó API Pro y quedó servido como `free`. Se
      regularizó a mano el 15-09; falta confirmar que está corregido **de raíz** y no solo
      en ese caso.
- [x] **Cruzar los `pendiente_pago` con `seguimiento_contactos`** — HECHO el 2026-10-03.
      **La hipótesis de la transferencia era falsa**: ninguno de los 21 pendientes (8 tienda,
      13 API Pro) pagó por otra vía. El 73% por transferencia son los *contratos*, otro canal.
      Cruce contra las tres tablas de cobro, `portal_planes`, `seguimiento_contactos`,
      `api_keys`, `solicitudes` (Postgres) y el panel de Flow (las 10 órdenes Webpay del
      13-09 al 02-10 calzan 1:1 con `entregada`; no hay pagos huérfanos).
      | Clase | Carros | UF |
      |---|---|---|
      | Reintento de alguien que sí pagó (IDVIA 25-26→27; Más Recursos 28→29) | 3 | 8,2 |
      | Duplicado de cliente ya pagado (Vial y Cía.) | 1 | 2 |
      | Prueba interna | 1 | 2 |
      | **Fuga real** (13 personas: 11 API Pro, 3 tienda, una en ambas) | 16 | 35,2 |
      ⇒ el 26% del «abandono» era ruido: la cifra del 09-19 (1,38 UF perdidas por UF
      cobrada) estaba inflada. Un caso probable más: `aruz@dpp.cl` abandonó API Pro el 09-22
      y una hora después `aaruzf@gmail.com` compró 1 comuna — misma persona, otro correo.
      Patrón: varios abandonos de API Pro ocurren 1-2 min después de crear la cuenta
      (mirar el precio, no compra frustrada).
- [ ] **Llamar a los que abandonaron el pago y siguen usando la API free**: Axity
      (abandonó 09-30, usa la API a diario), regu.cl (09-13) y `jlulloaa` (10-01).
- [ ] **Correo pendiente desde el 09-10** al estudiante U. de Chile (compra 3 UF vs licencia
      académica): su solicitud sigue en `antecedentes`.
- [ ] **Factura de Sustentable S.A. (venta 9, $372.648)**: `seguimiento_contactos` la tiene
      `pendiente` desde el 16-09. Confirmar si se emitió.
- [x] **API Pro se corta solo al vencer** — RESUELTO y desplegado el 2026-10-03 (apiV2 `425298a`).
      El `chequeo-planes.timer` diario avisa al cliente 3 días antes, y si venció sin renovar
      baja su key a free y se lo comunica. Vial y Cía. pasó al flujo normal (regularización
      anulada, key free+2000, MCP free): recordatorio automático el 07-10, corte el 10-10.
- [x] Correo de activación de API Pro sin «100/día» — desplegado el 2026-10-03 (app `b17de06`, md5 verificado en host y ambas réplicas).
- [x] **Punteros de `portal_users` a keys revocadas** — RESUELTO el 2026-10-03 (apiV2 `7bc8809`):
      `aplicar()` y `discrepancias()` ahora miran todas las keys activas del correo, no el
      puntero. Había 3 casos (Vial MCP; plomolex API+MCP), reapuntados. Además, bajar a free
      conserva las 2000/día si el API Pro de la app sigue vigente.
- [x] ~~Key MCP de `cris@tremen.tech` en `pro` sin plan~~ — verificado el 2026-10-04: la corrida de
      `chequeo-planes` de ese día reporta `descalces plan ↔ tier: 0`. Nada que hacer.

- [x] ⚖️ **Convenio de la licencia gratuita ampliado** a gestión pública y tercer sector —
      publicado el 2026-10-03 (catastralV2 `HEAD`), junto con los Términos (cl. 5).
- [ ] ⚖️ **Para el abogado:** (1) revisar la redacción nueva de la cláusula 5 del convenio y de
      los Términos; (2) **Ley 20.285**: un órgano del Estado licenciatario podría verse obligado
      a entregar los datos por una solicitud de transparencia, lo que choca con la prohibición
      de redistribuir (6.2). Falta una cláusula que lo resuelva (p. ej. remitir a la fuente
      pública original o al acceso vía Tremen).

- [ ] **13 pedidos comerciales cotizados sin cierre** (12 cotizados el 17-09 con precio por
      comuna, ninguno compró; + gabriel.martinez 09-10). Decidir seguimiento: segundo correo,
      llamada, o cerrarlos. **1 sin responder:** geotax.cl (09-29) pregunta por índices de
      conservadores / CBR y cómo probar la plataforma — candidato al módulo CBR.
- [x] Fechas de los contratos cargadas el 2026-10-03 (aprox., día 1 del mes): Póliza 2026-05-01
      (24 m → may-2028), MINVU y Grant Thornton 2026-10-01 (12 m → oct-2027).
- [ ] **Lunes 2026-10-05: revisar respuestas** a los 12 correos del 10-03 (Evolutiva, Axity,
      Regu, mtbarrav, Ñuñoa y 7 académicos). geotax.cl lo lleva Cris en persona.
- [x] **MJAA (Matías Jarpa, inmobiliaria, llegó por LinkedIn el 2026-10-04)** pidió probar el servicio.
      Muestra enviada el mismo día (Resend `delivered`, desde cris@tremen.tech): 3 manzanas de
      Providencia (2303, 857, 7003; 64 roles) con SII + PRC/Ordenanza + TGR + transacciones, Excel + PDF
      con mapas. Método en `PLAYBOOK_MUESTRAS.md`; archivos en `CATASTRAL/cotizaciones/mjaa_providencia_2026-10-04/`.
- [ ] **MJAA: seguimiento.** Se propuso una llamada de 30 min la semana del 2026-10-05. Si no responde
      en ~5 días hábiles, segundo correo. Siguiente paso comercial: muestra sobre SU zona.
- [x] **MJAA registrado en `seguimiento_contactos`** (id 263, motivo `muestra_prospecto`, estado `pendiente`,
      origen LinkedIn en la nota) el 2026-10-04. Respaldo previo: `188:/root/bak-acceso-20261004-pre-mjaa.sqlite`.
- [ ] **Opción B del contacto del propietario** (decidida 2026-10-04): el CBR a $800 incluye el contacto
      vía proveedor externo y el cliente no ve a Inciti. Hoy `app_catastral/backend/app/ext.py` consulta
      con la llave del cliente → hay que pasarlo a la llave de TREMEN **antes de que MJAA contrate**.
      ⚠️ Margen ≈ 0 cuando se pide el contacto (Inciti ≈ 0,02 UF/consulta) y revisar con el abogado que
      el contrato con Inciti permita revender.
- [x] **Precio TGR masivo fijado (2026-10-04):** 1 UF por 1.000 roles, escalera 1,00/0,90/0,80/0,70/0,60
      UF por mil hasta 10.000 (tramo aplica a todo el pedido; solo certificados obtenidos; >10.000 se
      cotiza). Costo medido ≈ US$0,0034/rol → margen ≥ 7× en el tramo más barato. Llevarlo a `MODELO_NEGOCIO.md`.
- [ ] **Módulo «Efecto OGUC» en la plataforma** (decidido 2026-10-04: el efecto del decreto MINVU —toma
      de razón 01-10-2026, publicación en el D.O. esperada la semana del 05-10— se vende SOLO como módulo
      recurrente, nunca por comuna). **Incluido en la base del plan Plataforma (5 UF/mes)**, no como módulo
      aparte; acceso mientras la suscripción esté pagada (Cris, 2026-10-04). No existe aún en `app_catastral`. Reutilizar `pse3/oguc/capacidad.py` (18 comunas calculadas;
      `SIN_GUARISMO` = delta ~0). Ajustar al texto publicado. Borrador v2 de la propuesta MJAA en
      `cotizaciones/mjaa_providencia_2026-10-04/muestra_catastral_providencia_v2_BORRADOR.pdf` (no enviado).
- [ ] **Newsletter: Cris decidió (2026-10-04) seguir enviando.** Definir a quién: la promesa de
      re-permiso excluye a los 574 `sin_respuesta`; lo coherente es opt-in (~10) + nuevos.

## 🟡 Marketing: el canal que sí funciona

- [x] **El titular del sitio** — PUBLICADO el 2026-10-03: H1 «El catastro de Chile, listo para usar» + bajada con rol↔mapa y cartografía SII vectorizada (respaldo `188:/root/bak-landing-20261003.tgz`). Medir en el próximo `/mercadoCatastral` si cambia la conversión de la landing. Antecedente: lo que hizo firmar a Póliza y al MINVU es *tenemos los
      polígonos del SII, que nadie más tiene*. Al 2026-10-03 el H1 ya no dice «10 millones de
      predios» sino «Expertos en datos públicos» — más genérico todavía. Decisión del 10-03:
      cambiarlo, pero **sin ser literal** con el foso; se están buscando opciones. Cuidado: el
      polígono SII es huella edificada, no deslinde — no prometer «el polígono de tu predio».
- [ ] **Primer experimento de LinkedIn**: el mapa vectorial contra la imagen WMS del SII.
      Es la demo del foso en una imagen. Material y consultas ya identificadas.
- [ ] **Registrar qué publicación origina cada conversación** (la tabla
      `seguimiento_contactos` ya existe). Sin eso, LinkedIn es una racha y no un canal.

## 🟢 Medir en el próximo `/mercadoCatastral`

- [ ] Efecto de bajar el free de 100 a 50/día (aplicado el 19-09 a las 136 keys activas).
- [ ] Efecto del precio por tamaño sin mínimo en la tienda.
- [ ] ¿Convirtió alguno de los 12 clientes recurrentes que están en `free`?
- [ ] Recompra en la tienda — **recién tiene sentido preguntarlo con 3 meses de historia**.
- [ ] Resolver el hueco de `export_log` (no tiene columna de fecha usable).

## ⚪ Deuda técnica menor

- [x] **`apiV3_catastral/scripts/deploy.sh` nunca se ha corrido de verdad.** _(Hecho 2026-09-22: rsync + rebuild + smoke OK.)_ Se reescribió a
      deploy por copia el 19-09 y el dry-run pasó, pero la primera corrida real conviene
      hacerla con un cambio trivial y mirando `docker logs apiv3-api`.
- [x] Borrar la deploy key `vps-46.62.214.65` de `mcp_catastral` en GitHub (máquina fuera de
      nuestro control). _(Verificado 2026-09-28: ya no existe. Ojo: `apiV3_catastral` tenía otra igual, read-write.)_
- [x] ~25 archivos en los repos todavía nombran las IPs dadas de baja (`46.62.214.65`,
      `167.233.70.166`). _Limpiado 2026-09-28: comandos operativos → 188; históricos con nota; `sync_oferta.sh` ya no tiene default a la 46.62._

## 📅 Con fecha dura

- [ ] **Ley 21.719, vigente 2026-12-01.** Desaparece la excepción de «fuente accesible al
      público»: hay que documentar base de licitud y EIPD. Ver `mcp_catastral/docs/MARCO_LEGAL.md`.
- [ ] **~sept 2027: MINVU y Grant Thornton vencen el mismo mes** — el 56% del MRR en
      renovación simultánea. Desfasar renovaciones y abrir la conversación en el mes 11.

---

## Decisiones ya cerradas

- [x] ~~Piso de costo de infra por cliente~~ — **2026-08-27:** USD 60/mes fijos para toda la
      operación, no por cliente. Margen sobre 50 UF/mes >95%.
- [x] ~~Piso exacto de la suscripción API empresarial~~ — **2026-08-27:** piso USD 200/mes,
      techo USD 1.800/mes ≈ 40 UF.
- [x] ~~Calibrar el límite del free tier~~ — **2026-09-19:** bajado a 50/día.
- [x] ~~Precio de la tienda~~ — **2026-09-16:** por tamaño (0,5 / 0,7 / 1 UF), sin mínimo.
- [ ] **Cabezales premium por valor** — sigue abierto: decidir si scoring u otros se cobran
      sobre el valor generado en vez de 10 UF planos.
- [ ] Aplicar pricing nuevo: grandfathering a los actuales; nuevos con base 30 + 10/cabezal
      y setup = 5× recurrente. **Ligado a la decisión 1.**

## Productización técnica (habilita el modelo)

- [ ] **Pipeline canónico → artefacto master versionado en S3.** Promover `evans_build/predios_chile_2025ss.duckdb` a master oficial; matar las ~5 copias divergentes como builds independientes (pasan a réplicas descargables).
- [ ] **Feature flags por tenant** en el shell — requisito para cobrar cabezales activables como switch, no como desarrollo.
- [ ] **Shell estándar único** sobre la base de Póliza Gestión (MapLibre + Workbench + Flujos + Export + master read-only + capa privada por cliente).
- [ ] **Aislar clientes** (sobre todo seguros) de los proyectos internos en las VPS compartidas.
- [~] ~~Rebalancear RAM entre VPS (46.62.214.65 ociosa, 46.224.221.33 al límite; apiv2 rozando OOM).~~ Obsoleto: la consolidación dejó dos VPS.
- [ ] Construir Nialem desde el inicio sobre el shell estándar (no como silo CSV divergente).
