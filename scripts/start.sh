#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

mvn -B -q -pl flink-jobs/taobao-stream-job -am package -DskipTests
docker compose -f infra/docker-compose.yml up -d
until curl -fsS http://localhost:8081/apis/ccompat/v7/subjects >/dev/null; do
  sleep 2
done
bash scripts/register_schemas.sh
echo "Runtime services are ready. Run 'make replay' to publish both golden attempts and submit the bounded job."
