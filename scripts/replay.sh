#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

IFS=, read -ra run_ids <<<"${REPLAY_RUN_IDS:-golden-a,golden-b}"
for run_id in "${run_ids[@]}"; do
  PYTHONPATH=producer/src python -m taobao_replay publish \
    tests/fixtures/user_behavior_fixture.csv \
    --run-id "$run_id" \
    --batch-size "${REPLAY_BATCH_SIZE:-100}" \
    --speed "${REPLAY_SPEED_FACTOR:-0}" \
    --bootstrap-servers "${KAFKA_CLIENT_BOOTSTRAP_SERVERS:-localhost:9092}" \
    --schema-registry-url \
      "${SCHEMA_REGISTRY_CLIENT_URL:-http://localhost:8081}"
done
echo "Published ${#run_ids[@]} deterministic replay attempts."
