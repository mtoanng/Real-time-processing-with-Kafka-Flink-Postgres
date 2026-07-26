#!/usr/bin/env bash
# Experiment A: replay one fixture twice while one checkpointed job remains running.
set -euo pipefail

cd "$(dirname "$0")/.."

run_a="${RUN_A_ID:-fixture-run-a}"
run_b="${RUN_B_ID:-fixture-run-b}"
if [ "$run_a" = "$run_b" ]; then
  echo "ERROR: RUN_A_ID and RUN_B_ID must differ" >&2
  exit 2
fi

echo "Replaying deterministic fixture as $run_a"
REPLAY_RUN_ID="$run_a" bash scripts/replay.sh
echo "Replaying the same source sequence as $run_b"
REPLAY_RUN_ID="$run_b" bash scripts/replay.sh

bash scripts/verify_bounded_pipeline.sh "$run_a" "$run_b"
