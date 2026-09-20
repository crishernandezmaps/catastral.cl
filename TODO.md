# TODO — catastral.cl

_Cabecera reordenada el **2026-09-19**. Arriba, lo que hay que decidir; abajo, lo técnico._

---

# 🔴 DECISIONES PARA LA PRÓXIMA SESIÓN

Las cinco primeras son de Cris: no las puede cerrar nadie más y bloquean lo demás.
Contexto y evidencia en `MODELO_NEGOCIO.md` e `INSIGHTS_CLIENTES.md`.

### 1. ¿La lista de precios se sostiene o se baja a lo que el mercado paga?
A precio de lista (base 30 + 7 cabezales) el catálogo completo vale **100 UF/mes**. Pero
Grant Thornton lo tiene a **25** y Póliza a **85,5 con desarrollo incluido**. Hoy se cotiza
con una lista que no se cobra, y eso ancla la negociación hacia abajo.
**Decidir antes de cotizar al cliente número cuatro.**

### 2. ¿El desarrollo se cotiza como línea separada del dato?
Es el **75% del MRR** y hoy va implícito dentro del paquete. Mientras siga invisible, no se
puede subir el precio del dato ni mostrar lo que el cliente realmente valora.
_Recomendación: sí, línea propia con alcance, plazo y entregables._

### 3. Propiedad intelectual en los contratos
- **Nuevos**: adoptar el molde de Grant Thornton — retener componentes preexistentes y
  genéricos, ceder solo el entregable específico (o licencia de uso perpetua).
- **Firmados**: revisar con abogado si la cesión alcanza a todo el software o solo a los
  desarrollos específicos. De esto depende si cada proyecto alimenta el catálogo o muere
  donde nace. «Cambiar dos líneas» **no** resuelve el problema: es obra derivada.

### 4. ¿Qué se hace con la tienda de comunas?
El modelo decía que la venta por comuna «deja de ser negocio y pasa a ser anzuelo gratis»,
pero la tienda cobra. **O es anzuelo gratis, o es producto.** Hoy son las dos cosas y se
contradicen.

### 5. El correo de seguimiento de la newsletter
La campaña prometió por escrito «si no haces nada, no vuelves a recibir correos nuestros».
Hay **574 en `sin_respuesta` y solo 4 en `opt_in`**. Escribirle a los 574 contradice lo
dicho, y con ese volumen **dos quejas de spam pasan el umbral de Gmail**. Detalle en
`catastralV2/TODO.md`.

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
