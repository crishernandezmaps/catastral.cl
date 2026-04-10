#!/usr/bin/env bash
set -euo pipefail

# Deploy script for Catastro Chile unified platform
# Target: VPS 46.62.214.65

VPS="root@46.62.214.65"
REMOTE_DIR="/var/www/catastral.cl"

echo "=== Building frontend ==="
cd "$(dirname "$0")/../frontend"
npm run build

echo "=== Syncing backend ==="
rsync -avz --delete \
  ../backend/ \
  "$VPS:$REMOTE_DIR/backend/" \
  --exclude '__pycache__' \
  --exclude '.env' \
  --exclude '*.pyc'

echo "=== Syncing pipeline ==="
rsync -avz --delete \
  ../pipeline/ \
  "$VPS:$REMOTE_DIR/pipeline/" \
  --exclude '__pycache__' \
  --exclude '*.pyc'

echo "=== Syncing frontend dist ==="
rsync -avz --delete \
  dist/ \
  "$VPS:$REMOTE_DIR/frontend/dist/"

echo "=== Syncing docker-compose ==="
rsync -avz ../docker-compose.yml "$VPS:$REMOTE_DIR/"

echo "=== Syncing nginx config ==="
rsync -avz ../infra/nginx.conf "$VPS:/etc/nginx/sites-available/catastral.cl"

echo "=== Restarting services on VPS ==="
ssh "$VPS" bash -s <<'EOF'
cd /var/www/catastral.cl

# Ensure virtualenv exists
if [ ! -d "venv" ]; then
    python3 -m venv venv
fi
source venv/bin/activate
pip install -q -r backend/requirements.txt

# Restart backend
systemctl restart catastro-api || true

# Enable nginx site
ln -sf /etc/nginx/sites-available/catastral.cl /etc/nginx/sites-enabled/
nginx -t && systemctl reload nginx

echo "Deploy complete!"
EOF
