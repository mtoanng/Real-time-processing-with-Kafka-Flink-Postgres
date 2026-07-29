#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

compose=(docker compose -f infra/docker-compose.yml --profile core)
mvn -B -q -pl flink-sql-pipeline -am package -DskipTests
"${compose[@]}" up -d --build \
  kafka schema-registry clickhouse redis redis-cart-materializer \
  flink-jobmanager flink-taskmanager
until curl -fsS http://localhost:8082/jobs/overview >/dev/null; do
  sleep 2
done
until curl -fsS http://localhost:8081/apis/ccompat/v7/subjects >/dev/null; do
  sleep 2
done
"${compose[@]}" run --rm kafka-init
bash scripts/register_schemas.sh
"${compose[@]}" run --rm flink-submit
echo "Core is running. Add --profile catalog or --profile api only for approved extensions."
