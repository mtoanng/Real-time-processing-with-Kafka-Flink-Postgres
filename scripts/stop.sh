#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
runtime_profile="${RUNTIME_PROFILE:-core}"
docker compose -f infra/docker-compose.yml --profile "$runtime_profile" down
