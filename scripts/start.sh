#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

runtime_profile="${RUNTIME_PROFILE:-core}"
export RUNTIME_PROFILE="$runtime_profile"
compose=(docker compose -f infra/docker-compose.yml --profile "$runtime_profile")
mvn -B -q package -DskipTests
python flink-sql-pipeline/render.py
"${compose[@]}" up -d
python -m scripts.register_schema
echo "$runtime_profile runtime is ready. Run 'make replay' to publish and execute the SQL job."
