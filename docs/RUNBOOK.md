# Core runbook

## Credential-independent checks

```bash
python -m pip install -e ".[kafka]"
python -m pip install ruff
make checks
```

No service is contacted by these checks.

## Start and verify

```bash
cp .env.example .env
make start
make replay
make verify
```

Inspect the running SQL job and completed checkpoints at
`http://localhost:8082`. Canonical results can be queried directly:

```bash
curl -sS -u default:local-clickhouse \
  'http://localhost:8123/?database=taobao_behavior' \
  --data-binary 'SELECT * FROM raw_behavior_events_canonical ORDER BY source_sequence'

curl -sS -u default:local-clickhouse \
  'http://localhost:8123/?database=taobao_behavior' \
  --data-binary 'SELECT * FROM item_metrics_1m_canonical ORDER BY window_start,item_id'
```

`make verify` independently compares stable business columns with
`tests/fixtures/golden_outputs.json`. It ignores ingestion timestamps and does
not treat `replay_run_id` as canonical identity.

## Recovery experiment

The SQL source is bounded for the fixture. To exercise restart behavior with a
larger bounded input:

1. Start `core` and submit the SQL pipeline.
2. Wait for at least one completed checkpoint in the Flink UI.
3. Terminate the TaskManager process and restart it without deleting the
   `flink_checkpoints` volume.
4. Let the job finish.
5. Save `make verify` output and canonical query snapshots.
6. Compare them with an uninterrupted run using only stable business columns.

This experiment is **NOT VERIFIED** until real output is captured. Do not claim
checkpoint mode as end-to-end transactional exactly once.

## Teardown

```bash
make stop
```

This keeps named volumes. Remove them only when intentionally discarding local
Kafka, ClickHouse, and checkpoint state.

## Legacy artifacts

The Compose `legacy` profile is not part of the active runbook. PostgreSQL,
Debezium, Redis, API, and Java DataStream artifacts are retained only for a
separately approved cleanup or migration.
