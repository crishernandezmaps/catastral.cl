# catastral.cl — Instrucciones para agentes

Este repositorio es la plataforma de producción **catastral.cl**.

## Despliegue

- **Frontend dist:** `/var/www/catastral.cl/frontend/dist/` en VPS `46.62.214.65`
- **Backend:** `/var/www/catastral.cl/backend/` en VPS `46.62.214.65`
- **Script de deploy:** `infra/deploy.sh` (rsync + restart)

Solo este repositorio (`catastro/`) tiene autorización para escribir en `/var/www/catastral.cl/`.

## PROHIBIDO

- Nunca copiar ni rsync archivos de `ai_catastral`, `catastral_street` ni ningún otro proyecto a `/var/www/catastral.cl/`.
- Nunca modificar `/etc/nginx/sites-available/catastral.cl` ni `/etc/nginx/sites-enabled/catastral.cl` desde otro proyecto.
- Nunca hacer deploy del frontend sin hacer `npm run build` primero en `frontend/`.
