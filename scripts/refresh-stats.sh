#!/usr/bin/env bash
set -euo pipefail

# Regenera el JSON estatico de estadisticas para el frontend.
# Ejecutar despues de cargar nuevos datos semestrales con el pipeline.
#
# Uso local:  ./scripts/refresh-stats.sh
# Uso en VPS: ./scripts/refresh-stats.sh --remote

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
VPS="root@46.62.214.65"
API_URL="https://catastral.cl/api"
OUT_FILE="$PROJECT_DIR/frontend/public/stats-resumen.json"

if [[ "${1:-}" == "--remote" ]]; then
    echo "Generando stats desde VPS..."
    ssh "$VPS" 'curl -s http://127.0.0.1:8000/api/estadisticas/resumen' > "$OUT_FILE"
else
    echo "Generando stats desde API publica..."
    curl -s "$API_URL/estadisticas/resumen" > "$OUT_FILE"
fi

# Validar JSON
python3 -c "
import json, sys
d = json.load(open('$OUT_FILE'))
print(f'OK: {d[\"total_predios\"]:,} predios, {d[\"total_comunas\"]} comunas, {len(d[\"por_region\"])} regiones')
print(f'    Mediana avaluo: \${d[\"mediana_avaluo\"]:,.0f}')
print(f'    Archivo: $OUT_FILE')
"

echo ""
echo "Siguiente paso: rebuild y deploy del frontend"
echo "  cd $PROJECT_DIR/frontend && npm run build"
echo "  rsync -avz --delete dist/ $VPS:/var/www/catastral.cl/frontend/dist/"
