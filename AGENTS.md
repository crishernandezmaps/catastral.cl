# catastral.cl — Instrucciones para agentes

Este repositorio es la plataforma de producción **catastral.cl**.

## Despliegue

- **Frontend dist:** `/var/www/catastral.cl/frontend/dist/` en VPS `188.245.241.255`
- **Backend:** `/var/www/catastral.cl/backend/` en VPS `188.245.241.255`
- **Script de deploy:** `infra/deploy.sh` (rsync + restart)

⚠️ **Actualizado 2026-09-28: este repo YA NO despliega catastral.cl.** Su copia de backend/frontend está
desactualizada; el deploy real sale del repo `catastralV2`. No correr `infra/deploy.sh` desde aquí.

## PROHIBIDO

- Nunca copiar ni rsync archivos de `ai_catastral`, `catastral_street` ni ningún otro proyecto a `/var/www/catastral.cl/`.
- Nunca modificar `/etc/nginx/sites-available/catastral.cl` ni `/etc/nginx/sites-enabled/catastral.cl` desde otro proyecto.
- Nunca hacer deploy del frontend sin hacer `npm run build` primero en `frontend/`.
