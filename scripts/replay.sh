#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

IFS=, read -ra run_ids <<<"${REPLAY_RUN_IDS:-golden-a,golden-b}"
for run_id in "${run_ids[@]}"; do
  PYTHONPATH=producer/src python -m taobao_replay publish \
    tests/fixtures/user_behavior_fixture.csv \
    --run-id "$run_id" \
    --batch-size "${REPLAY_BATCH_SIZE:-100}" \
    --speed "${REPLAY_SPEED_FACTOR:-0}"
done
docker compose -f infra/docker-compose.yml exec -T flink-jobmanager \
  flink run -d -c com.taobao.behavior.TaobaoStreamJob /opt/flink/usrlib/taobao.jar
echo "Published ${#run_ids[@]} replay attempts and submitted the bounded Flink job."
