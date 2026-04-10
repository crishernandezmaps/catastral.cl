#!/bin/bash
# =============================================================================
# batch_tif_30ns.sh — Descarga TIFs del SII con 30 tuneles WireGuard
#
# Lee la queue generada por prepare_tif_queue.py y para cada comuna:
#   1. Lanza 30 chunk downloaders (2 workers c/u) en network namespaces
#   2. Monitorea progreso, detecta stalls, rota tuneles quemados
#   3. Ensambla GeoTIFF desde los tiles descargados
#   4. Sube a S3 y limpia tiles locales
#
# Comunas con >50K tiles se procesan solas (no en paralelo con otras).
#
# Uso:
#   bash batch_tif_30ns.sh [queue_file]
#
# Pre-requisitos:
#   - 30 namespaces WireGuard activos (setup_tunnels.sh)
#   - Queue generada: python3 prepare_tif_queue.py
#   - mullvad_relays.json en /tmp/
#
# Mecanismos de resiliencia:
#
#   STALL DETECTION — Cada 15s el monitor revisa el ok count de cada túnel
#     via CHUNK_LOGS[$i] (que apunta al log activo, sea original o steal).
#     Si ok no avanza en 4 checks (60s), se marca como BURNED: se mata el
#     proceso, se rota el túnel a un relay spare, y se relanza el chunk.
#     Máximo NUM_TUNNELS*2 rotaciones por comuna.
#
#   WORK STEALING — Cuando un túnel termina su chunk, roba trabajo del
#     chunk con más tiles restantes. Calcula remaining desde CHUNK_LOGS[$j]
#     del túnel candidato. Solo roba si quedan >20 tiles.
#
#   STEAL LOOP PREVENTION — Si un steal termina en <5s (tiles ya en disco),
#     marca el chunk en CHUNK_COMPLETED para que nadie vuelva a robarlo.
#     Sin esto, steals instantáneos generan un loop infinito de re-steals
#     al mismo chunk, desperdiciando rotaciones de IP.
#
#   TIMEOUT POR COMUNA — MAX_COMUNA_SECS (3h). Si una comuna excede este
#     límite, mata todos los workers y ensambla con lo descargado. Safety
#     net para casos no cubiertos por stall detection.
#
#   WMS LAYER PROBE — Antes de descargar, prueba que el layer WMS exista.
#     Si no, intenta variantes del nombre (preposiciones concatenadas, etc).
#     Si ninguna funciona, defiere la comuna a tif_deferred.tsv.
# =============================================================================
set -uo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
VENV="/root/carto_predios/venv/bin/python3"
CHUNK_SCRIPT="${DIR}/download_chunk.py"
ASSEMBLE_SCRIPT="${DIR}/1_descargar.py"
TIF_DIR="/tmp/tif_work"

QUEUE="${1:-/tmp/tif_queue.tsv}"
NUM_TUNNELS=30
WORKERS_PER_TUNNEL=2
RELAY_JSON="/tmp/mullvad_relays.json"
LARGE_THRESHOLD=50000

S3_BUCKET="s3://siipredios/2025ss_bcn/TIFs"
S3_ENDPOINT="https://nbg1.your-objectstorage.com"
export AWS_ACCESS_KEY_ID="YOUR_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="YOUR_SECRET_KEY"
AWS="/root/carto_predios/venv/bin/aws"

SPARE_RELAYS=(
    "gb-man" "de-ber" "nl-ams" "fr-par" "se-sto"
    "jp-tyo" "au-syd" "ie-dub" "es-mad" "it-rom"
    "at-vie" "ch-zur" "no-osl" "dk-cph" "fi-hel"
    "pl-war" "ro-buc" "bg-sof" "hr-zag" "cz-prg"
    "hu-bud" "ca-van" "nz-akl" "us-phx" "us-atl"
    "us-slc" "us-den" "us-bos"
)
SPARE_IDX=0

rotate_tunnel() {
    local IDX=$1
    local NS="vpn${IDX}"
    local WG="wg${IDX}"
    local RELAY="${SPARE_RELAYS[$SPARE_IDX]}"
    SPARE_IDX=$(( (SPARE_IDX + 1) % ${#SPARE_RELAYS[@]} ))

    [ ! -f "$RELAY_JSON" ] && return 1

    local RINFO
    RINFO=$($VENV -c "
import json
for r in json.load(open('$RELAY_JSON'))['wireguard']['relays']:
    if r['hostname'].startswith('$RELAY'):
        print(r['ipv4_addr_in']); print(r['public_key']); break
" 2>/dev/null)
    local NEW_EP=$(echo "$RINFO" | head -1)
    local NEW_PK=$(echo "$RINFO" | tail -1)
    [ -z "$NEW_EP" ] && return 1

    local OLD_PK=$(ip netns exec "$NS" wg show "$WG" peers 2>/dev/null | head -1)
    ip netns exec "$NS" wg set "$WG" peer "$OLD_PK" remove 2>/dev/null
    ip netns exec "$NS" wg set "$WG" peer "$NEW_PK" endpoint "${NEW_EP}:51820" allowed-ips 0.0.0.0/0 persistent-keepalive 25 2>/dev/null
    sleep 2
    echo "  [ROTATE] T${IDX} → $RELAY"
}

DEFERRED_FILE="/tmp/tif_deferred.tsv"

# =============================================================================
# Probe WMS layer name: returns 0 if layer exists, 1 if LayerNotDefined
# =============================================================================
probe_wms_name() {
    local NS=$1 COD=$2 NOMBRE=$3 BBOX=$4
    # Pick center tile
    local CENTER_BBOX
    CENTER_BBOX=$($VENV -c "
import math
bbox = [float(x) for x in '${BBOX}'.split(',')]
zoom = 19
def ll2t(lat, lon, z):
    lr = math.radians(lat)
    nn = 2**z
    return int((lon+180)/360*nn), int((1-math.asinh(math.tan(lr))/math.pi)/2*nn)
sx, sy = ll2t(bbox[3], bbox[0], zoom)
mx, my = ll2t(bbox[1], bbox[2], zoom)
cx, cy = (sx+mx)//2, (sy+my)//2
ws = 20037508.34*2; ts = ws/(2**zoom)
minx = -20037508.34 + cx*ts; maxy = 20037508.34 - cy*ts
print(f'{minx},{maxy-ts},{minx+ts},{maxy}')
" 2>/dev/null)
    [ -z "$CENTER_BBOX" ] && return 1

    local RESP
    RESP=$(ip netns exec "$NS" curl -s --max-time 15 \
        "${BASE_WMS}?service=WMS&request=GetMap&layers=sii:BR_CART_${NOMBRE}_WMS&styles=PREDIOS_WMS_V0&format=image/png&transparent=true&version=1.1.1&comuna=${COD}&eac=0&eacano=0&height=256&width=256&srs=EPSG:3857&bbox=${CENTER_BBOX}" \
        2>/dev/null)
    if echo "$RESP" | grep -q "LayerNotDefined"; then
        return 1
    fi
    return 0
}
BASE_WMS="https://www4.sii.cl/mapasui/services/ui/wmsProxyService/call"

# =============================================================================
# Generate name variants for a WMS layer name
# =============================================================================
generate_name_variants() {
    local NOMBRE=$1
    $VENV -c "
import re
name = '${NOMBRE}'
seen = {name}
results = []

# Split concatenated prepositions: PEDRODELA -> PEDRO_DE_LA, DIEGODE -> DIEGO_DE
parts = name.split('_')
new_parts = []
for part in parts:
    modified = part
    for suffix, repl in [('DELAPAZ','DE_LA_PAZ'),('DELAS','DE_LAS'),('DELOS','DE_LOS'),
                          ('DELA','DE_LA'),('DEL','DEL'),('DE','DE')]:
        if modified.endswith(suffix) and len(modified) > len(suffix):
            modified = modified[:-len(suffix)] + '_' + repl
            break
    new_parts.append(modified)
v1 = '_'.join(new_parts)
if v1 not in seen:
    seen.add(v1)
    results.append(v1)

# Also try removing apostrophes
v2 = name.replace(\"'\", '')
if v2 not in seen:
    seen.add(v2)
    results.append(v2)

# Try fully joined (no underscores except prepositions)
v3 = name.replace('_', '')
if v3 not in seen:
    seen.add(v3)
    results.append(v3)

# Try with all underscores between each word-like segment
v4 = re.sub(r'([A-Z])([A-Z])', lambda m: m.group(0), name)
if v4 not in seen:
    seen.add(v4)
    results.append(v4)

for r in results:
    print(r)
" 2>/dev/null
}

# =============================================================================
# Ensamblado de GeoTIFF desde tiles descargados
# =============================================================================
assemble_geotiff() {
    local COD=$1
    local BBOX=$2
    local ZOOM=$3
    local WORK_DIR=$4
    local OUT_FILE=$5

    $VENV -c "
import math, os, sys
import numpy as np
from PIL import Image
import rasterio
from rasterio.crs import CRS
from rasterio.transform import Affine

bbox = [float(x) for x in '${BBOX}'.split(',')]
zoom = ${ZOOM}

def ll2t(lat, lon, z):
    lr = math.radians(lat)
    nn = 2**z
    return int((lon+180)/360*nn), int((1-math.asinh(math.tan(lr))/math.pi)/2*nn)

def t2b(x, y, z):
    ws = 20037508.34*2; ts = ws/(2**z)
    minx = -20037508.34 + x*ts; maxy = 20037508.34 - y*ts
    return (minx, maxy-ts, minx+ts, maxy)

sx, sy = ll2t(bbox[3], bbox[0], zoom)
mx, my = ll2t(bbox[1], bbox[2], zoom)
if sx>mx: sx,mx=mx,sx
if sy>my: sy,my=my,sy
tx = mx-sx+1; ty = my-sy+1

btl = t2b(sx, sy, zoom)
bbr = t2b(mx, my, zoom)
minx=btl[0]; maxy=btl[3]; maxx=bbr[2]; miny=bbr[1]
tw = tx*256; th = ty*256
psx = (maxx-minx)/tw; psy = (maxy-miny)/th
transform = Affine(psx, 0, minx, 0, -psy, maxy)

tiles_dir = os.path.join('${WORK_DIR}', 'tiles')
out = '${OUT_FILE}'
print(f'  Ensamblando {tw}x{th} px ({tw*th/1e6:.0f}M px)...')

with rasterio.open(out, 'w', driver='GTiff',
    height=th, width=tw, count=4, dtype=np.uint8,
    crs=CRS.from_epsg(3857), transform=transform,
    compress='deflate', tiled=True, blockxsize=256, blockysize=256,
) as dst:
    written = 0
    for ty_idx in range(ty):
        for tx_idx in range(tx):
            x = sx + tx_idx; y = sy + ty_idx
            tf = os.path.join(tiles_dir, f'tile_{x}_{y}.png')
            if os.path.exists(tf):
                try:
                    img = Image.open(tf).convert('RGBA')
                    arr = np.array(img)
                    w = rasterio.windows.Window(tx_idx*256, ty_idx*256, 256, 256)
                    for b in range(4):
                        dst.write(arr[:,:,b], b+1, window=w)
                    written += 1
                except: pass

sz = os.path.getsize(out)/(1024*1024)
print(f'  GeoTIFF: {sz:.1f} MB ({written} tiles escritos)')
" 2>&1
}

# =============================================================================
# Main
# =============================================================================
if [ ! -f "$QUEUE" ]; then
    echo "ERROR: Queue file not found: $QUEUE"
    echo "Run: python3 prepare_tif_queue.py"
    exit 1
fi

DONE=0; FAIL=0
TOTAL=$(tail -n +2 "$QUEUE" | wc -l | tr -d ' ')
BATCH_START=$(date +%s)

echo "============================================"
echo "BATCH TIF 30-NS - $(date)"
echo "Queue: $QUEUE ($TOTAL comunas)"
echo "30 tunnels × $WORKERS_PER_TUNNEL workers"
echo "Large threshold: $LARGE_THRESHOLD tiles"
echo "============================================"

while IFS=$'\t' read -r COD NOMBRE BBOX TIER TX TY TILES RAM; do
    [ "$COD" = "cod" ] && continue
    [ -z "$COD" ] && continue

    # Skip if already in S3
    if $AWS s3 ls "${S3_BUCKET}/comuna=${COD}.tif" \
        --endpoint-url "$S3_ENDPOINT" > /dev/null 2>&1; then
        DONE=$((DONE + 1))
        echo "[SKIP] $COD (already in S3) — $DONE/$TOTAL"
        continue
    fi

    IS_LARGE=0
    [ "$TILES" -gt "$LARGE_THRESHOLD" ] && IS_LARGE=1

    # --- Pre-flight: probe WMS layer name ---
    if ! probe_wms_name "vpn0" "$COD" "$NOMBRE" "$BBOX"; then
        echo "  [PROBE] Layer '$NOMBRE' not found for $COD, trying variants..."
        FOUND_NAME=""
        for VARIANT in $(generate_name_variants "$NOMBRE"); do
            echo "  [PROBE] Trying: $VARIANT"
            if probe_wms_name "vpn0" "$COD" "$VARIANT" "$BBOX"; then
                echo "  [PROBE] Found working name: $VARIANT"
                FOUND_NAME="$VARIANT"
                break
            fi
        done
        if [ -z "$FOUND_NAME" ]; then
            echo "[DEFER] $COD $NOMBRE (layer not found, no variant worked)"
            echo -e "${COD}\t${NOMBRE}\t${BBOX}\t${TIER}\t${TX}\t${TY}\t${TILES}\t${RAM}" >> "$DEFERRED_FILE"
            continue
        fi
        NOMBRE="$FOUND_NAME"
    fi

    echo ""
    echo "[START] $COD $NOMBRE (${TX}x${TY}=$TILES tiles, ${RAM}GB, tier=$TIER$([ $IS_LARGE -eq 1 ] && echo ' LARGE')) - $(date '+%H:%M:%S')"
    T_START=$(date +%s)

    WORK_DIR="${TIF_DIR}/${COD}"
    mkdir -p "${WORK_DIR}/tiles"

    # Launch 30 chunk downloaders
    declare -A CHUNK_PIDS CHUNK_LOGS
    for i in $(seq 0 $((NUM_TUNNELS - 1))); do
        rm -f "/tmp/tif_${i}_${COD}.log" "/tmp/tif_steal_${i}_${COD}.log"
        CHUNK_LOGS[$i]="/tmp/tif_${i}_${COD}.log"
        ip netns exec "vpn${i}" \
            $VENV -u "$CHUNK_SCRIPT" \
                --comuna "$COD" --nombre "$NOMBRE" \
                --bbox="$BBOX" --zoom 19 \
                --chunk $i --total-chunks $NUM_TUNNELS \
                --out-dir "$WORK_DIR" \
                --workers $WORKERS_PER_TUNNEL \
                --progress-every 15 \
            2> "/tmp/tif_${i}_${COD}.log" &
        CHUNK_PIDS[$i]=$!
    done

    # Monitor loop
    declare -A MON_LAST_OK MON_FAIL_CHECKS
    for i in $(seq 0 $((NUM_TUNNELS - 1))); do
        MON_LAST_OK[$i]=0; MON_FAIL_CHECKS[$i]=0
    done
    ROTATIONS=0
    MAX_ROTATIONS=$((NUM_TUNNELS * 2))
    LAYER_MISSING=0
    # Safety timeout: 3h max per comuna (even the largest ~140K tiles finish in ~2h)
    MAX_COMUNA_SECS=10800
    # Track completed chunks so steals don't re-target them
    declare -A CHUNK_COMPLETED STEAL_START_TIME

    while true; do
        ALIVE=0
        for i in $(seq 0 $((NUM_TUNNELS - 1))); do
            PID=${CHUNK_PIDS[$i]:-0}
            [ "$PID" -gt 0 ] && kill -0 "$PID" 2>/dev/null && ALIVE=$((ALIVE + 1))
        done
        [ "$ALIVE" -eq 0 ] && break

        # Safety timeout: kill all remaining if comuna takes too long
        COMUNA_EL=$(( $(date +%s) - T_START ))
        if [ "$COMUNA_EL" -gt "$MAX_COMUNA_SECS" ]; then
            echo "  [TIMEOUT] Comuna $COD exceeded ${MAX_COMUNA_SECS}s, killing $ALIVE remaining workers"
            for i in $(seq 0 $((NUM_TUNNELS - 1))); do
                PID=${CHUNK_PIDS[$i]:-0}
                [ "$PID" -gt 0 ] && kill -9 "$PID" 2>/dev/null
            done
            sleep 2
            break
        fi

        sleep 15

        # Check for LAYER_NOT_FOUND (chunks exit with code 10)
        LNF_COUNT=$(grep -l "LAYER_NOT_FOUND" /tmp/tif_*_${COD}.log 2>/dev/null | wc -l | tr -d ' ')
        if [ "$LNF_COUNT" -ge 3 ]; then
            echo "  [LAYER_NOT_FOUND] $LNF_COUNT chunks report layer missing, aborting"
            for i in $(seq 0 $((NUM_TUNNELS - 1))); do
                PID=${CHUNK_PIDS[$i]:-0}
                [ "$PID" -gt 0 ] && kill -9 "$PID" 2>/dev/null
            done
            sleep 1
            echo "[DEFER] $COD $NOMBRE (layer not found at runtime)"
            echo -e "${COD}\t${NOMBRE}\t${BBOX}\t${TIER}\t${TX}\t${TY}\t${TILES}\t${RAM}" >> "$DEFERRED_FILE"
            rm -rf "$WORK_DIR"
            rm -f /tmp/tif_*_${COD}.log /tmp/tif_steal_*_${COD}.log
            LAYER_MISSING=1
            break
        fi

        # Aggregate progress
        TOK=0; TFL=0
        for i in $(seq 0 $((NUM_TUNNELS - 1))); do
            L=$(grep "PROGRESS\|DONE" /tmp/tif_${i}_${COD}.log 2>/dev/null | tail -1)
            OK=$(echo "$L" | grep -oE "ok=[0-9]+" | cut -d= -f2)
            FL=$(echo "$L" | grep -oE "fail=[0-9]+" | cut -d= -f2)
            [ -n "$OK" ] && TOK=$((TOK + OK))
            [ -n "$FL" ] && TFL=$((TFL + FL))
        done
        EL=$(( $(date +%s) - T_START ))
        RATE=$(echo "scale=1; $TOK / ($EL + 1)" | bc 2>/dev/null || echo "?")
        echo "  $(date '+%H:%M:%S') [$ALIVE alive] ok=$TOK fail=$TFL (${RATE} t/s, ${EL}s)"

        # Detect stalls (60s = 4 checks)
        for i in $(seq 0 $((NUM_TUNNELS - 1))); do
            PID=${CHUNK_PIDS[$i]:-0}
            [ "$PID" -gt 0 ] && kill -0 "$PID" 2>/dev/null || continue

            ACTIVE_LOG="${CHUNK_LOGS[$i]}"
            COK=$(grep "PROGRESS" "$ACTIVE_LOG" 2>/dev/null | tail -1 | grep -oE "ok=[0-9]+" | cut -d= -f2)
            [ -z "$COK" ] && continue

            if [ "$COK" -eq "${MON_LAST_OK[$i]}" ]; then
                MON_FAIL_CHECKS[$i]=$((${MON_FAIL_CHECKS[$i]} + 1))
            else
                MON_FAIL_CHECKS[$i]=0
                MON_LAST_OK[$i]=$COK
            fi

            if [ "${MON_FAIL_CHECKS[$i]}" -ge 4 ] && [ "$ROTATIONS" -lt "$MAX_ROTATIONS" ]; then
                echo "  [BURNED] T${i} (ok stuck at ${COK})"
                kill -9 "$PID" 2>/dev/null; sleep 2
                rotate_tunnel "$i"

                CHUNK_LOGS[$i]="/tmp/tif_${i}_${COD}.log"
                ip netns exec "vpn${i}" \
                    $VENV -u "$CHUNK_SCRIPT" \
                        --comuna "$COD" --nombre "$NOMBRE" \
                        --bbox="$BBOX" --zoom 19 \
                        --chunk $i --total-chunks $NUM_TUNNELS \
                        --out-dir "$WORK_DIR" \
                        --workers $WORKERS_PER_TUNNEL \
                        --progress-every 15 \
                    2>> "/tmp/tif_${i}_${COD}.log" &
                CHUNK_PIDS[$i]=$!
                MON_FAIL_CHECKS[$i]=0; MON_LAST_OK[$i]=0
                ROTATIONS=$((ROTATIONS + 1))
            fi
        done

        # Work stealing: re-launch idle tunnels with busiest chunk
        for i in $(seq 0 $((NUM_TUNNELS - 1))); do
            PID=${CHUNK_PIDS[$i]:-0}
            # Skip if still alive
            [ "$PID" -gt 0 ] && kill -0 "$PID" 2>/dev/null && continue

            # If this tunnel had a steal, check if it finished instantly (tiles on disk)
            if [ -n "${STEAL_START_TIME[$i]:-}" ]; then
                DONE_LINE=$(grep "^DONE chunk=" "${CHUNK_LOGS[$i]}" 2>/dev/null | tail -1)
                if [ -n "$DONE_LINE" ]; then
                    # Check elapsed reported by the process itself (not wall clock)
                    PROC_ELAPSED=$(echo "$DONE_LINE" | grep -oE "elapsed=[0-9]+" | cut -d= -f2)
                    if [ -n "$PROC_ELAPSED" ] && [ "$PROC_ELAPSED" -lt 5 ]; then
                        # Steal finished instantly — chunk tiles already on disk
                        LAST_CHUNK=$(echo "$DONE_LINE" | grep -oE "chunk=[0-9]+" | cut -d= -f2)
                        if [ -n "$LAST_CHUNK" ]; then
                            CHUNK_COMPLETED[$LAST_CHUNK]=1
                            echo "  [STEAL_DONE] Chunk $LAST_CHUNK fully on disk, skipping future steals"
                        fi
                    fi
                fi
                unset STEAL_START_TIME[$i]
            fi

            # Find busiest alive tunnel (most remaining tiles), skip completed chunks
            BEST_J=-1; BEST_REM=0
            for j in $(seq 0 $((NUM_TUNNELS - 1))); do
                [ "${CHUNK_COMPLETED[$j]:-0}" = "1" ] && continue
                JPID=${CHUNK_PIDS[$j]:-0}
                [ "$JPID" -gt 0 ] && kill -0 "$JPID" 2>/dev/null || continue
                ACTIVE_LOG_J="${CHUNK_LOGS[$j]}"
                JL=$(grep "PROGRESS\|DONE" "$ACTIVE_LOG_J" 2>/dev/null | tail -1)
                JOK=$(echo "$JL" | grep -oE "ok=[0-9]+" | cut -d= -f2)
                JTOT=$(echo "$JL" | grep -oE "total=[0-9]+" | cut -d= -f2)
                [ -z "$JOK" ] || [ -z "$JTOT" ] && continue
                JREM=$((JTOT - JOK))
                if [ "$JREM" -gt "$BEST_REM" ]; then
                    BEST_REM=$JREM; BEST_J=$j
                fi
            done

            # Only steal if there's meaningful work left (>20 tiles)
            [ "$BEST_J" -lt 0 ] || [ "$BEST_REM" -le 20 ] && continue

            echo "  [STEAL] T${i} → stealing chunk $BEST_J ($BEST_REM tiles left)"
            rm -f "/tmp/tif_steal_${i}_${COD}.log"
            CHUNK_LOGS[$i]="/tmp/tif_steal_${i}_${COD}.log"
            STEAL_START_TIME[$i]=$(date +%s)
            ip netns exec "vpn${i}" \
                $VENV -u "$CHUNK_SCRIPT" \
                    --comuna "$COD" --nombre "$NOMBRE" \
                    --bbox="$BBOX" --zoom 19 \
                    --chunk $BEST_J --total-chunks $NUM_TUNNELS \
                    --out-dir "$WORK_DIR" \
                    --workers $WORKERS_PER_TUNNEL \
                    --progress-every 15 \
                2> "/tmp/tif_steal_${i}_${COD}.log" &
            CHUNK_PIDS[$i]=$!
            MON_LAST_OK[$i]=0; MON_FAIL_CHECKS[$i]=0
        done
    done

    # Skip post-processing if layer was missing
    [ "$LAYER_MISSING" -eq 1 ] && continue

    T_END=$(date +%s)
    ELAPSED=$((T_END - T_START))

    # Count tiles downloaded
    N_TILES=$(find "${WORK_DIR}/tiles" -name "*.png" 2>/dev/null | wc -l | tr -d ' ')
    echo "  Tiles descargados: $N_TILES"

    if [ "$N_TILES" -eq 0 ]; then
        FAIL=$((FAIL + 1))
        echo "[FAIL] $COD (${ELAPSED}s, no tiles)"
        rm -rf "$WORK_DIR"
        rm -f /tmp/tif_*_${COD}.log /tmp/tif_steal_*_${COD}.log
        continue
    fi

    # Assemble GeoTIFF
    echo "  Ensamblando GeoTIFF..."
    TIF_FILE="${WORK_DIR}/comuna=${COD}.tif"
    assemble_geotiff "$COD" "$BBOX" 19 "$WORK_DIR" "$TIF_FILE"

    # Upload to S3
    if [ -f "$TIF_FILE" ]; then
        echo "  Subiendo a S3..."
        if $AWS s3 cp "$TIF_FILE" "${S3_BUCKET}/comuna=${COD}.tif" \
            --endpoint-url "$S3_ENDPOINT" --quiet 2>/dev/null; then
            echo "  S3 OK"
        else
            echo "  [S3 FAIL] Upload failed, keeping local: $TIF_FILE"
            FAIL=$((FAIL + 1))
            rm -f /tmp/tif_*_${COD}.log
            continue
        fi
    fi

    # Cleanup tiles PNG + work dir
    rm -rf "$WORK_DIR"
    rm -f /tmp/tif_*_${COD}.log /tmp/tif_steal_*_${COD}.log

    DONE=$((DONE + 1))
    BATCH_EL=$(( $(date +%s) - BATCH_START ))
    AVG=$((BATCH_EL / (DONE > 0 ? DONE : 1)))
    ETA=$(( AVG * (TOTAL - DONE) ))
    RATE_EFF=$(echo "scale=1; $N_TILES / $ELAPSED" | bc 2>/dev/null || echo "?")
    echo "[DONE] $COD (${ELAPSED}s, ${N_TILES} tiles, ${RATE_EFF} t/s, ${ROTATIONS} rot) — $DONE/$TOTAL | ETA: $((ETA/3600))h$((ETA%3600/60))m"

done < "$QUEUE"

# =============================================================================
# Deferred comunas summary
# =============================================================================
if [ -f "$DEFERRED_FILE" ] && [ -s "$DEFERRED_FILE" ]; then
    DEFER_COUNT=$(wc -l < "$DEFERRED_FILE" | tr -d ' ')
    echo ""
    echo "============================================"
    echo "DEFERRED COMUNAS: $DEFER_COUNT (layer name mismatch)"
    echo "These need manual name correction in the queue and re-run."
    echo "============================================"
    while IFS=$'\t' read -r D_COD D_NOMBRE D_REST; do
        echo "  $D_COD  $D_NOMBRE"
    done < "$DEFERRED_FILE"
    echo ""
    echo "Deferred file: $DEFERRED_FILE"
fi

echo ""
echo "============================================"
BATCH_EL=$(( $(date +%s) - BATCH_START ))
echo "BATCH COMPLETE: $DONE done, $FAIL failed out of $TOTAL"
echo "Deferred: $([ -f "$DEFERRED_FILE" ] && wc -l < "$DEFERRED_FILE" | tr -d ' ' || echo 0)"
echo "Total time: $((BATCH_EL/3600))h $((BATCH_EL%3600/60))m"
echo "============================================"
