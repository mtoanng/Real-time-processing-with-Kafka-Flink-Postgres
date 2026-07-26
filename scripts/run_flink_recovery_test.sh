#!/usr/bin/env bash
# Experiment B: fail a running TaskManager after a completed checkpoint.
set -euo pipefail

cd "$(dirname "$0")/.."

if [ "${RECOVERY_TEST_CONFIRM:-}" != YES ]; then
  echo "Refusing recovery test. Set RECOVERY_TEST_CONFIRM=YES on a disposable host." >&2
  exit 2
fi
: "${FLINK_JOB_ID:?FLINK_JOB_ID is required}"
: "${FLINK_REST_URL:?FLINK_REST_URL is required}"
: "${FLINK_FAILURE_COMMAND:?FLINK_FAILURE_COMMAND must restart/fail a TaskManager, not cancel the job}"
: "${BASELINE_SNAPSHOT:?BASELINE_SNAPSHOT from an uninterrupted fresh-database run is required}"

evidence_dir="${EVIDENCE_DIR:-docs/evidence/latest/recovery}"
mkdir -p "$evidence_dir"
checkpoint_timeout="${CHECKPOINT_WAIT_TIMEOUT_SECONDS:-180}"
deadline=$((SECONDS + checkpoint_timeout))
checkpoint_id=""

echo "Starting paced recovery replay"
REPLAY_RUN_ID="${RECOVERY_RUN_ID:-recovery-run}" \
  REPLAY_SPEED_FACTOR="${RECOVERY_REPLAY_SPEED:-20}" \
  bash scripts/replay.sh >"$evidence_dir/recovery-replay.log" 2>&1 &
replay_pid=$!

while [ "$SECONDS" -lt "$deadline" ]; do
  checkpoint_id="$(
    curl --fail --silent "${FLINK_REST_URL%/}/jobs/$FLINK_JOB_ID/checkpoints" |
      jq -r '.latest.completed.id // empty'
  )"
  [ -n "$checkpoint_id" ] && break
  sleep 2
done
if [ -z "$checkpoint_id" ]; then
  kill "$replay_pid" 2>/dev/null || true
  echo "ERROR: no completed checkpoint within ${checkpoint_timeout}s" >&2
  exit 1
fi
printf '%s\n' "$checkpoint_id" >"$evidence_dir/completed-checkpoint-id.txt"

echo "Checkpoint $checkpoint_id completed; executing controlled TaskManager failure"
bash -lc "$FLINK_FAILURE_COMMAND" >"$evidence_dir/failure-command.log" 2>&1

deadline=$((SECONDS + checkpoint_timeout))
while [ "$SECONDS" -lt "$deadline" ]; do
  state="$(
    curl --fail --silent "${FLINK_REST_URL%/}/jobs/$FLINK_JOB_ID" |
      jq -r '.state // empty'
  )"
  [ "$state" = RUNNING ] && break
  sleep 2
done
if [ "${state:-}" != RUNNING ]; then
  kill "$replay_pid" 2>/dev/null || true
  echo "ERROR: Flink job did not return to RUNNING" >&2
  exit 1
fi
wait "$replay_pid"

cart_args=()
if [ "${RUNTIME_PROFILE:-core}" = serving ]; then
  bash scripts/lookup_active_cart.sh 100 >"$evidence_dir/recovered-cart.txt"
  cart_args=(--active-cart-file "$evidence_dir/recovered-cart.txt")
fi
PYTHONPATH=producer/src python scripts/canonical_snapshot.py capture \
  --output "$evidence_dir/recovered.json" "${cart_args[@]}"
PYTHONPATH=producer/src python scripts/canonical_snapshot.py compare \
  --baseline "$BASELINE_SNAPSHOT" \
  --recovered "$evidence_dir/recovered.json" |
  tee "$evidence_dir/comparison.log"
