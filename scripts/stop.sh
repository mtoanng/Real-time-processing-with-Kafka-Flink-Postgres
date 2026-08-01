#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose -f infra/docker-compose.yml --profile core --profile catalog --profile api down
