#!/bin/bash
# Detiene el navegador remoto de Citify (deja instalado todo y el perfil con la sesión). Volver a levantar: levanta.sh
for pat in "conductor_vps.py" "serve_cors_vps.py" "websockify --web /usr/share/novnc" "x11vnc -display :99" "user-data-dir=/root/citify_vps/citify-chrome" "Xvfb :99"; do
  for pid in $(pgrep -f -- "$pat"); do [ "$pid" != "$$" ] && kill "$pid" 2>/dev/null; done
done
sleep 3
ss -ltn | grep -E ":(5900|6080|8765|9222)\b" || echo "puertos libres"
