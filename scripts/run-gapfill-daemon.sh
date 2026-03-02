#!/bin/bash
# Continuous near-real-time gap-fill loop for the core runtime container.

set -euo pipefail

INTERVAL_SECONDS="${REKTSLUG_GAP_FILL_INTERVAL_SECONDS:-300}"

log() { echo "[$(date -Iseconds)] $1"; }

while true; do
    if /app/scripts/run-ccxt-gap-fill.sh; then
        log "Gap-fill cycle complete"
    else
        log "Gap-fill cycle failed (continuing)"
    fi
    sleep "$INTERVAL_SECONDS"
done
