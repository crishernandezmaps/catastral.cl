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
- [ ] **2026-10-04** (dos semanas después del cierre): revisar quién quedó en la lista —
      conteo por estado (`opt_in` / `opt_out` / `sin_respuesta`) filtrando por remitente
      `news.catastral.cl`, y con eso decidir qué canal queda de verdad.

---

## 🟠 Verificaciones que abren oportunidad

- [ ] **Confirmar la causal del trato directo del MINVU.** Si fue *proveedor único*, es un
      precedente **formal** replicable con municipios, SERVIU y gobiernos regionales — el
      mercado donde la competencia de plataformas no compite.
- [ ] **El hardcode del portal**: un cliente pagó API Pro y quedó servido como `free`. Se
      regularizó a mano el 15-09; falta confirmar que está corregido **de raíz** y no solo
      en ese caso.
- [ ] **Cruzar los `pendiente_pago` con `seguimiento_contactos`.** El 73% de lo facturado
      entró por transferencia, así que parte del «abandono de carro» puede ser gente que
      terminó pagando por banco. Sin ese cruce no se puede llamar fuga.

## 🟡 Marketing: el canal que sí funciona

- [ ] **El titular del sitio.** Hoy comunica «10 millones de predios». Lo que hizo firmar a
      Póliza y al MINVU es *tenemos los polígonos del SII, que nadie más tiene*. Esa frase
      debería estar en la primera pantalla.
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

- [ ] **`apiV3_catastral/scripts/deploy.sh` nunca se ha corrido de verdad.** Se reescribió a
      deploy por copia el 19-09 y el dry-run pasó, pero la primera corrida real conviene
      hacerla con un cambio trivial y mirando `docker logs apiv3-api`.
- [ ] Borrar la deploy key `vps-46.62.214.65` de `mcp_catastral` en GitHub (máquina fuera de
      nuestro control).
- [ ] ~25 archivos en los repos todavía nombran las IPs dadas de baja (`46.62.214.65`,
      `167.233.70.166`). Ya no quedan scripts ejecutables entre ellos: es ruido documental.

## 📅 Con fecha dura

- [ ] **2026-10-04: revisar quién quedó en la lista de la newsletter** tras la ventana de
      re-permiso (ver decisión 5 arriba).
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
- [ ] Rebalancear RAM entre VPS (46.62.214.65 ociosa, 46.224.221.33 al límite; apiv2 rozando OOM).
- [ ] Construir Nialem desde el inicio sobre el shell estándar (no como silo CSV divergente).
