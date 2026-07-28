#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

runtime_profile="${RUNTIME_PROFILE:-core}"
export RUNTIME_PROFILE="$runtime_profile"
compose=(docker compose -f infra/docker-compose.yml --profile "$runtime_profile")
IFS=, read -ra run_ids <<<"${REPLAY_RUN_IDS:-golden-a,golden-b}"
for run_id in "${run_ids[@]}"; do
  PYTHONPATH=producer/src python -m taobao_replay publish \
    tests/fixtures/user_behavior_fixture.csv \
    --run-id "$run_id" \
    --batch-size "${REPLAY_BATCH_SIZE:-100}" \
    --speed "${REPLAY_SPEED_FACTOR:-0}" \
    --bootstrap-servers "${KAFKA_CLIENT_BOOTSTRAP_SERVERS:-localhost:9092}" \
    --schema-registry-url \
      "${SCHEMA_REGISTRY_CLIENT_URL:-http://localhost:8081/apis/ccompat/v7}"
done
"${compose[@]}" exec -T flink-jobmanager \
  ./bin/sql-client.sh \
  -D execution.target=remote \
  -D rest.address=flink-jobmanager \
  -D rest.port=8081 \
  -j /opt/flink/connectors/taobao-sql-connectors.jar \
  -f /opt/flink/sql/pipeline.sql
echo "SQL pipeline finished after ${#run_ids[@]} deterministic replay attempts."
