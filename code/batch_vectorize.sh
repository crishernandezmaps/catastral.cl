#!/bin/bash
# =============================================================================
# batch_vectorize.sh — Fase 2: Vectoriza TIFs de S3 para todas las comunas
#
# Procesa en 4 tiers por tamaño de TIF, con distintos niveles de paralelismo
# para no saturar RAM/disco:
#   HUGE   (>500MB):  2 workers  — ~75GB RAM c/u
#   LARGE  (200-500): 4 workers  — ~30GB RAM c/u
#   MEDIUM (50-200):  10 workers — ~12GB RAM c/u
#   SMALL  (<50MB):   20 workers — ~4GB RAM c/u
#
# Uso:
#   bash batch_vectorize.sh [--resume]
# =============================================================================
set -o pipefail

BASEDIR="/root/carto_predios/sii_vectorizer"
ENDPOINT="https://nbg1.your-objectstorage.com"
S3_TIFS="s3://siipredios/2025ss_bcn/TIFs"
S3_VECTORS="s3://siipredios/2025ss_bcn/vectors"
AWS="/root/carto_predios/venv/bin/aws"
LOG="/tmp/batch_vectorize.log"

export AWS_ACCESS_KEY_ID="YOUR_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="YOUR_SECRET_KEY"

cd "$BASEDIR"

echo "============================================================"
echo "FASE 2: VECTORIZACION - $(date)"
echo "============================================================"

# List all TIFs with sizes
echo "Listando TIFs en S3..."
$AWS --endpoint-url "$ENDPOINT" s3 ls "$S3_TIFS/" 2>/dev/null \
    | grep '\.tif' \
    | awk '{print $3, $4}' \
    | sed 's/comuna=//; s/\.tif//' \
    | sort -n > /tmp/tif_sizes.txt

TOTAL=$(wc -l < /tmp/tif_sizes.txt | tr -d ' ')
echo "TIFs en S3: $TOTAL"

# Check already vectorized (resume mode)
SKIP_SET=""
if [ "${1:-}" = "--resume" ]; then
    echo "Modo resume: verificando ya vectorizadas..."
    SKIP_SET=$($AWS --endpoint-url "$ENDPOINT" s3 ls "$S3_VECTORS/" 2>/dev/null \
        | grep '\.geojson' \
        | awk '{print $4}' \
        | sed 's/comuna=//; s/\.geojson//' \
        | sort -n || true)
    DONE_COUNT=$(echo "$SKIP_SET" | grep -c . 2>/dev/null || echo 0)
    echo "Ya vectorizadas: $DONE_COUNT"
fi

# Split into tiers
> /tmp/vec_huge.txt
> /tmp/vec_large.txt
> /tmp/vec_medium.txt
> /tmp/vec_small.txt

PENDING=0
while read SIZE COD; do
    # Skip if already done
    if [ -n "$SKIP_SET" ] && echo "$SKIP_SET" | grep -qx "$COD"; then
        continue
    fi

    MB=$((SIZE / 1048576))
    PENDING=$((PENDING + 1))

    if [ "$MB" -gt 500 ]; then
        echo "$COD" >> /tmp/vec_huge.txt
    elif [ "$MB" -gt 200 ]; then
        echo "$COD" >> /tmp/vec_large.txt
    elif [ "$MB" -gt 50 ]; then
        echo "$COD" >> /tmp/vec_medium.txt
    else
        echo "$COD" >> /tmp/vec_small.txt
    fi
done < /tmp/tif_sizes.txt

N_HUGE=$(wc -l < /tmp/vec_huge.txt | tr -d ' ')
N_LARGE=$(wc -l < /tmp/vec_large.txt | tr -d ' ')
N_MEDIUM=$(wc -l < /tmp/vec_medium.txt | tr -d ' ')
N_SMALL=$(wc -l < /tmp/vec_small.txt | tr -d ' ')

echo ""
echo "Pendientes: $PENDING"
echo "  HUGE   (>500MB):  $N_HUGE  — 2 workers"
echo "  LARGE  (200-500): $N_LARGE — 4 workers"
echo "  MEDIUM (50-200):  $N_MEDIUM — 10 workers"
echo "  SMALL  (<50MB):   $N_SMALL — 20 workers"
echo ""

# Initialize log
echo "Inicio: $(date)" > "$LOG"
echo "========================================" >> "$LOG"

BATCH_START=$(date +%s)
DONE=0
FAIL=0

# Process one comuna with logging
process_comuna() {
    local COD=$1
    local START=$(date +%s)
    local TMPLOG="/tmp/sii_log_vec_${COD}.log"

    bash "${BASEDIR}/vectorize_one.sh" "$COD" > "$TMPLOG" 2>&1
    local RC=$?

    local END=$(date +%s)
    local ELAPSED=$(( END - START ))

    if [ $RC -eq 0 ]; then
        local RESULT=$(tail -1 "$TMPLOG")
        echo "[DONE] $COD (${ELAPSED}s) $RESULT"
        echo "$(date '+%H:%M:%S') | ${ELAPSED}s | $RESULT" >> "$LOG"
    else
        local ERR=$(grep -i 'fail\|error' "$TMPLOG" | tail -1)
        echo "[FAIL] $COD (${ELAPSED}s) $ERR"
        echo "$(date '+%H:%M:%S') | ${ELAPSED}s | FAIL $COD: $ERR" >> "$LOG"
    fi

    rm -f "$TMPLOG"
}
export -f process_comuna
export BASEDIR LOG

# Process each tier
run_tier() {
    local TIER=$1
    local FILE=$2
    local WORKERS=$3
    local COUNT=$(wc -l < "$FILE" | tr -d ' ')

    if [ "$COUNT" -eq 0 ]; then return; fi

    echo "============================================================"
    echo "TIER $TIER: $COUNT comunas, $WORKERS workers — $(date '+%H:%M:%S')"
    echo "============================================================"

    cat "$FILE" | xargs -P"$WORKERS" -I{} bash -c 'process_comuna "$@"' _ {}

    TIER_DONE=$(grep -c "| OK\|DONE" "$LOG" 2>/dev/null || echo 0)
    echo "  Tier $TIER completado. Total acumulado: $TIER_DONE"
    echo ""
}

run_tier "HUGE"   /tmp/vec_huge.txt   2
run_tier "LARGE"  /tmp/vec_large.txt  4
run_tier "MEDIUM" /tmp/vec_medium.txt 10
run_tier "SMALL"  /tmp/vec_small.txt  20

# Summary
echo "========================================" >> "$LOG"
echo "Fin: $(date)" >> "$LOG"

BATCH_END=$(date +%s)
BATCH_EL=$(( BATCH_END - BATCH_START ))

OK=$(grep -c "| OK " "$LOG" 2>/dev/null || echo 0)
FAIL=$(grep -c "| FAIL " "$LOG" 2>/dev/null || echo 0)

echo ""
echo "============================================================"
echo "FASE 2 COMPLETA - $(date)"
echo "Tiempo total: $((BATCH_EL/3600))h $((BATCH_EL%3600/60))m"
echo "Exitosas: $OK"
echo "Fallidas: $FAIL"
echo "Log: $LOG"
echo "============================================================"
