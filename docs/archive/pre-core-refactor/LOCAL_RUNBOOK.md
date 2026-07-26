# Local / Disposable-Host Runbook

> Archived pre-core-refactor runbook. See `docs/RUNBOOK.md`.

The laptop is intended for `checks`. Start the complete runtime only on a
machine with enough memory for Kafka, ClickHouse, Cassandra, and Flink.

For a first run, use a fresh ClickHouse volume/database. The schema command is
non-destructive and will not convert an existing legacy `MergeTree` table into
the new `ReplacingMergeTree` contract. Preserve old data and perform an explicit
migration if the volume already contains the earlier schema.

## 1. Credential-independent checks

```bash
pip install -e '.[kafka]'
pip install ruff
make test
make package
make infra-config
make terraform-validate
```

## 2. Configure local core

```bash
cp .env.example .env
set -a
source .env
set +a
```

Keep `RUNTIME_PROFILE=core` and `CASSANDRA_MODE=local`. Adjust local ports only
when they conflict with another process.

## 3. Start dependencies

```bash
bash scripts/run.sh
docker compose -f infra/docker-compose.yml --profile core ps
bash scripts/apply_cassandra_schema.sh
PYTHONPATH=producer/src python scripts/apply_clickhouse_schema.py
bash scripts/register_schemas.sh
```

Compose supplies one local Cassandra node. The schema script uses host `cqlsh`
when installed, otherwise it executes `cqlsh` inside that container.

## 4. Build, submit, and replay

Install/start Flink 1.20.2 separately, then:

```bash
mvn -B -pl flink-jobs/taobao-stream-job -am package
FLINK_DETACHED=true bash scripts/run_flink.sh
bash scripts/replay.sh
bash scripts/verify_bounded_pipeline.sh fixture-run
```

Expected independent fixture results are committed under
`docs/evidence/final-e2e/expected-reconciliation.json`: 1,000 produced, 999
valid raw, 1 invalid, 2 late, and item 501 as user 100's only active cart row.

## 5. Full profile

Set `RUNTIME_PROFILE=full`, provide the PostgreSQL/Debezium values, then:

```bash
bash scripts/run.sh
bash scripts/run_phase6_demo.sh
```

Verify a versioned rule update and a durable row in
`behavior_alerts_deduplicated`. This remains unverified until actual logs and
query evidence are captured.

## 6. Stop and clean local state

```bash
bash scripts/stop.sh
# Only when intentionally deleting local database volumes:
bash scripts/stop.sh --volumes
```

## Troubleshooting

- Cassandra configuration errors occur before job execution; verify mode-specific
  variables and the exact datacenter.
- If ClickHouse counts exceed expectations physically, query the committed
  `*_deduplicated` views or use `FINAL`.
- If a minute window is absent, confirm later event-time input advanced the
  watermark past that window.
- If a replay cannot serialize a row, inspect producer rejection output; only
  Avro-encodable semantic invalids can reach the Flink invalid table.
