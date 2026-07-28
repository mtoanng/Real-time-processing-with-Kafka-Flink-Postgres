#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
runtime_profile="${RUNTIME_PROFILE:-core}"
compose=(docker compose -f infra/docker-compose.yml --profile "$runtime_profile")

if [ "${RECOVERY_TEST_CONFIRM:-}" != "YES" ]; then
  echo "Set RECOVERY_TEST_CONFIRM=YES on a disposable runtime host." >&2
  exit 2
fi
: "${FLINK_JOB_ID:?Set FLINK_JOB_ID for the running unbounded experiment.}"
: "${BASELINE_SNAPSHOT:?Set BASELINE_SNAPSHOT from an uninterrupted run.}"

checkpoint_api="http://localhost:8082/jobs/$FLINK_JOB_ID/checkpoints"
checkpoint_id="$(curl -fsS "$checkpoint_api" | python -c \
  'import json,sys; print((json.load(sys.stdin).get("latest",{}).get("completed") or {}).get("id",""))')"
if [ -z "$checkpoint_id" ]; then
  echo "No completed checkpoint; leave the job running and retry." >&2
  exit 1
fi
"${compose[@]}" restart flink-taskmanager
deadline=$((SECONDS + 120))
until [ "$(curl -fsS "http://localhost:8082/jobs/$FLINK_JOB_ID" | python -c \
  'import json,sys; print(json.load(sys.stdin)["state"])')" = "RUNNING" ]; do
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "Flink job did not return to RUNNING within 120 seconds." >&2
    exit 1
  fi
  sleep 2
done
PYTHONPATH=producer/src python scripts/verify.py \
  --snapshot artifacts/recovered.json --snapshot-only
python -c \
  'import json,sys; a=json.load(open(sys.argv[1])); b=json.load(open(sys.argv[2])); assert a==b' \
  "$BASELINE_SNAPSHOT" artifacts/recovered.json
echo "Recovery output equals the uninterrupted baseline after checkpoint $checkpoint_id."
