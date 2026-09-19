# catastro — Instrucciones para agentes

Repositorio **histórico y metodológico** del proyecto catastral: tesis, metodología del pipeline v3,
modelo de negocio, catálogos de comunas/fases, material de expansión y visualizaciones.

## ⚠️ Este repo NO es la plataforma de producción (corregido 2026-09-19)

Hasta esta fecha este archivo afirmaba ser «la plataforma de producción catastral.cl» y reclamaba
autorización exclusiva para escribir en `/var/www/catastral.cl/`. **Es falso y era peligroso**: un
agente que lo leyera desplegaría una copia vieja sobre el sitio en vivo.

**El sitio catastral.cl lo despliega `catastralV2`**, no este repo. Verificado por md5:
`catastralV2/backend/main.py` (`04dd8a82…`) es el que corre en producción; el `backend/main.py` de
este repo es otro (`95614cad…`), una copia desactualizada y divergente.

- **No editar** `backend/` ni `frontend/` de este repo esperando que lleguen a producción.
- **No correr** `infra/deploy.sh`: quedó neutralizado el 2026-09-19 (aborta con un aviso). Hacía
  `rsync --delete` y sobrescribía el nginx de catastral.cl contra una VPS dada de baja.
- `infra/nginx.conf` y `infra/catastro-api.service` son de la topología vieja.

## Dónde vive de verdad cada cosa

| Servicio | VPS | Repo que lo despliega |
|---|---|---|
| `catastral.cl` | `188.245.241.255` : `/var/www/catastral.cl` | **catastralV2** |
| `catastral.cl/app` | `188.245.241.255` : `/opt/app_catastral` | **app_catastral** |
| `apiv2.catastral.cl` | `188.245.241.255` (contenedor `apiv2-api`) | **apiV2_catastral** |
| `apiv3.catastral.cl` | `188.245.241.255` : `/opt/apiV3_catastral` | **apiV3_catastral** |
| `mcp.catastral.cl` | `188.245.241.255` : `/root/mcp_catastral` | **mcp_catastral** |
| fábrica z16 / builds / TGR | `46.224.221.33` (`simi-process`) | pse3 / tgr |

⚠️ **VPS dadas de baja en la consolidación de septiembre de 2026:** `46.62.214.65` y
`167.233.70.166`. No hacer ssh/scp/rsync contra ellas — esas IPs ya no son nuestras (la `46.62`
responde hoy con otra host key).

## Qué sí se trabaja acá
`MODELO_NEGOCIO.md`, `METODOLOGIA_PIPELINE_v3.md`, `TODO.md`, `pipeline/`, `expansion/`, `viz/`.

## Datos personales
`leads_after_closing/` está en `.gitignore`: son leads comerciales reales con nombre y correo. No
versionarlos ni moverlos al repo.

## Convenciones de trabajo
- **Confirmar antes de acciones que cambian estado** (deploys, borrados, recursos facturables).
  Solo-lectura (ls, cat, SELECT, git status) sin preguntar.
- Reportar resultados con fidelidad; si algo falla, decirlo con el output real.
- En operaciones de riesgo, dejar siempre nombrado el camino de rollback.
