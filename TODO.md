# TODO — catastral.cl

## Modelo de negocio (ver `MODELO_NEGOCIO.md`)

Definido en sesión 2026-06-27. Decisiones abiertas pendientes de datos del fundador:

- [ ] **Piso de costo de infra por cliente** — cuánto cuesta servir a un cliente (su VPS/réplica), para confirmar que el recurrente de 50 UF/mes deja margen sano.
- [ ] **Piso exacto de la suscripción API empresarial** — modelo definido (plana, ~40 UF/mes); fijar el número final contra el volumen real de Renta Nacional.
- [ ] **Calibrar el límite del free tier** — 100/día = 3.000/mes es generoso; confirmar que ningún caso de uso empresarial real quepa dentro del free.
- [ ] **Cabezales premium por valor** — decidir si scoring (genera leads de ahorro tributario) u otros se cobran sobre el valor generado, en vez de 10 UF planos.
- [ ] Aplicar pricing nuevo: clientes actuales con **grandfathering**; nuevos con base 30 + 10/cabezal y setup = 5× recurrente.

## Productización técnica (habilita el modelo)

- [ ] **Pipeline canónico → artefacto master versionado en S3.** Promover `evans_build/predios_chile_2025ss.duckdb` a master oficial; matar las ~5 copias divergentes como builds independientes (pasan a réplicas descargables).
- [ ] **Feature flags por tenant** en el shell — requisito para cobrar cabezales activables como switch, no como desarrollo.
- [ ] **Shell estándar único** sobre la base de Póliza Gestión (MapLibre + Workbench + Flujos + Export + master read-only + capa privada por cliente).
- [ ] **Aislar clientes** (sobre todo seguros) de los proyectos internos en las VPS compartidas.
- [ ] Rebalancear RAM entre VPS (46.62.214.65 ociosa, 46.224.221.33 al límite; apiv2 rozando OOM).
- [ ] Construir Nialem desde el inicio sobre el shell estándar (no como silo CSV divergente).
