#!/usr/bin/env bash
# Print the exact two-run recovery workflow without causing a failure.
set -euo pipefail

cd "$(dirname "$0")/.."

: "${FLINK_CHECKPOINT_DIR:?set FLINK_CHECKPOINT_DIR to persistent storage}"

cat <<'STEPS'
Experiment B requires a disposable Linux host and a fresh ClickHouse database.

1. Uninterrupted baseline:
   - start the same core profile and submit the single job;
   - replay the fixture once;
   - capture stable business columns:
     PYTHONPATH=producer/src python scripts/canonical_snapshot.py capture \
       --output docs/evidence/latest/recovery/uninterrupted.json

2. Recovery run against another fresh database:
   - submit the same JAR and record FLINK_JOB_ID;
   - set FLINK_FAILURE_COMMAND to restart one TaskManager;
   - set BASELINE_SNAPSHOT to uninterrupted.json;
   - run:
     RECOVERY_TEST_CONFIRM=YES bash scripts/run_flink_recovery_test.sh

The recovery script waits for a completed checkpoint before the failure and
compares canonical raw/metrics. Under serving it also compares active-cart rows.
STEPS
