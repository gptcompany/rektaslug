#!/bin/bash
# Install LiquidationHeatmap systemd timers/services
# Usage: sudo ./install.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "Installing LiquidationHeatmap systemd units..."

cp "${SCRIPT_DIR}/lh-ingestion.service" /etc/systemd/system/
cp "${SCRIPT_DIR}/lh-ingestion.timer" /etc/systemd/system/
cp "${SCRIPT_DIR}/lh-ccxt-gap-fill.service" /etc/systemd/system/
cp "${SCRIPT_DIR}/lh-ccxt-gap-fill.timer" /etc/systemd/system/

systemctl daemon-reload
systemctl enable lh-ingestion.timer
systemctl start lh-ingestion.timer
systemctl enable lh-ccxt-gap-fill.timer
systemctl start lh-ccxt-gap-fill.timer

echo "Done. Timer status:"
systemctl status lh-ingestion.timer --no-pager
systemctl status lh-ccxt-gap-fill.timer --no-pager
