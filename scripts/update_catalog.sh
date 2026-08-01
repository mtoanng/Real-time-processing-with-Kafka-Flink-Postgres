#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose -f infra/docker-compose.yml --profile core --profile catalog \
  exec -T postgres psql -v ON_ERROR_STOP=1 \
  -U "${POSTGRES_USER:-catalog}" -d "${POSTGRES_DB:-catalog}" \
  -f /fixtures/product_catalog_updates.sql
