#!/bin/bash
# Navegador para subir capas a Citify desde la VPS. Todo escucha solo en localhost:
#   :99 Xvfb · 9222 CDP de Chrome · 5900 VNC · 6080 noVNC (entrar con túnel SSH -L 6080:localhost:6080)
export DISPLAY=:99
pgrep -f "Xvfb :99" >/dev/null || (Xvfb :99 -screen 0 1440x900x24 >/root/citify_vps/xvfb.log 2>&1 &)
sleep 2
pgrep -f "citify-chrome" >/dev/null || (google-chrome --user-data-dir=/root/citify_vps/citify-chrome --no-sandbox --no-first-run --no-default-browser-check \
  --remote-debugging-address=127.0.0.1 --remote-debugging-port=9222 --disable-dev-shm-usage --window-size=1440,900 --window-position=0,0 \
  https://app.citify.cl/plataforma/capas >/root/citify_vps/chrome.log 2>&1 &)
pgrep -f "x11vnc -display :99" >/dev/null || (x11vnc -display :99 -localhost -rfbport 5900 -forever -shared -nopw -quiet >/root/citify_vps/vnc.log 2>&1 &)
pgrep -f "websockify.*6080" >/dev/null || (websockify --web /usr/share/novnc 127.0.0.1:6080 127.0.0.1:5900 >/root/citify_vps/novnc.log 2>&1 &)
