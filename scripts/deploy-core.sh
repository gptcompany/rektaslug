#!/bin/bash
# Deploy or refresh the rektslug core runtime from the local repo checkout.

set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
PROJECT_DIR="$(CDPATH= cd -- "${SCRIPT_DIR}/.." && pwd)"

cd "$PROJECT_DIR"

if [ ! -f ".env" ]; then
    echo "Missing .env in ${PROJECT_DIR}. Copy .env.example and configure runtime values first." >&2
    exit 1
fi

docker compose config >/dev/null
docker compose pull rektslug-api rektslug-sync
docker compose up -d rektslug-api rektslug-sync
docker compose ps
