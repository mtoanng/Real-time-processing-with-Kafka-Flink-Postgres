#!/usr/bin/env bash
# Verify two replay attempts against canonical ClickHouse results.
set -euo pipefail

cd "$(dirname "$0")/.."

if [ "$#" -ne 2 ] || [ -z "$1" ] || [ -z "$2" ]; then
  echo "usage: bash scripts/verify_bounded_pipeline.sh <run-a> <run-b>" >&2
  exit 2
fi

run_a="$1"
run_b="$2"
export REPLAY_RUN_IDS="$run_a,$run_b"
export EVIDENCE_DIR="${EVIDENCE_DIR:-artifacts/reconciliation/${run_a}-${run_b}}"

echo "Computing independent bounded expectations"
PYTHONPATH=producer/src python scripts/reconcile_final_e2e.py

echo "Verifying canonical ClickHouse results"
PYTHONPATH=producer/src python scripts/verify_clickhouse.py \
  --run-id "$run_a" \
  --run-id "$run_b"

if [ "${RUNTIME_PROFILE:-core}" = serving ]; then
  echo "Verifying optional Cassandra active cart for user 100"
  lookup_output="$(bash scripts/lookup_active_cart.sh 100)"
  echo "$lookup_output"
  if ! grep -q 'user_id=100 item_id=501 ' <<<"$lookup_output"; then
    echo "ERROR: expected active item 501 for user 100" >&2
    exit 1
  fi
  if grep -q 'user_id=100 item_id=500 ' <<<"$lookup_output"; then
    echo "ERROR: purchased item 500 is still active for user 100" >&2
    exit 1
  fi
fi

echo "Bounded pipeline reconciliation passed for runs $run_a and $run_b"
