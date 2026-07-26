# Taobao Recoverable Streaming Platform

An event-time portfolio project built from the raw Alibaba Tianchi
`UserBehavior.csv` dataset.

Status:

- **CODEBASE-READY:** verified by the credential-independent checks recorded in
  [`docs/evidence/latest`](docs/evidence/latest/).
- **DEPLOYMENT-VERIFIED:** **NOT VERIFIED**; no live service or cloud result is
  claimed.

## Architecture

```text
Taobao fixture / bounded raw source
  -> Python deterministic replay
  -> Kafka + Schema Registry
  -> one Java Flink DataStream job
       -> ClickHouse canonical history
       -> ClickHouse one-minute item/category metrics
       -> ClickHouse INVALID / DUPLICATE / LATE quality events
```

Optional, isolated extensions:

```text
serving:       core -> Cassandra user_active_cart
observability: Grafana -> ClickHouse canonical views
```

The checked-in `behavior_rules`/Debezium branch is deprecated legacy code,
isolated from `core`, and not the target CDC architecture. A separately
approved future phase will replace it with PostgreSQL `product_catalog` CDC and
current-state enrichment. Product CDC is **NOT IMPLEMENTED** and **NOT
VERIFIED**.

See [architecture](docs/ARCHITECTURE.md),
[stream semantics](docs/SEMANTICS.md), and the
[three-layer codebase learning guide](docs/learning/README.md).

## Responsibilities

| Component | Responsibility |
| --- | --- |
| Python | Raw-row validation, deterministic IDs, bounded replay |
| Kafka / Schema Registry | Transport and Avro contract |
| Java Flink | Validation, TTL deduplication, event time, metrics, recovery state |
| ClickHouse | Canonical history, metrics, quality evidence |
| Cassandra | Optional per-user active-cart lookup only |
| PostgreSQL / Debezium | Deprecated legacy behavior-rule extension; never clickstream source |

`event_id` depends on stable source fields and source sequence.
`replay_run_id` is lineage only.

## Checks profile

No external service connections:

```bash
pip install -e '.[kafka]'
pip install ruff
make checks
```

The equivalent individual commands are documented in
[the runbook](docs/RUNBOOK.md).

## Minimal core demo

Run the complete stack only on a disposable host with adequate memory:

```bash
cp .env.example .env
set -a
source .env
set +a

RUNTIME_PROFILE=core bash scripts/run.sh
PYTHONPATH=producer/src python scripts/apply_clickhouse_schema.py
bash scripts/register_schemas.sh
mvn -B -pl flink-jobs/taobao-stream-job -am package
FLINK_DETACHED=true bash scripts/run_flink.sh
bash scripts/run_replay_identity_experiment.sh
```

Core requires Kafka, Schema Registry, Flink, and ClickHouse. It does not read
Cassandra, Astra, PostgreSQL, Debezium, or Grafana settings.

## Optional profiles

```bash
# Core plus local/Astra active-cart projection
RUNTIME_PROFILE=serving bash scripts/run.sh
bash scripts/apply_cassandra_schema.sh

# Core plus Grafana backed by canonical ClickHouse views
RUNTIME_PROFILE=observability bash scripts/run.sh
```

Every profile submits the same Java JAR.

The legacy `cdc` profile remains renderable only to keep the deprecated
implementation isolated and compilable during this release. It is not a
supported release target.

## Canonical verification

Experiment A publishes the same source sequences under two run IDs. Expected
canonical raw and metric digests remain unchanged; the second valid replay is
classified as duplicate quality evidence:

```bash
bash scripts/run_replay_identity_experiment.sh
```

Canonical consumers query:

```text
raw_behavior_events_canonical
item_metrics_1m_canonical
stream_quality_events_canonical
```

These views explicitly use `FINAL`. Ordinary replacement-table queries are not
immediately duplicate-free.

## Recovery experiment

Capture an uninterrupted baseline, then execute a controlled TaskManager
restart after a completed checkpoint:

```bash
bash scripts/run_checkpoint_experiment.sh
RECOVERY_TEST_CONFIRM=YES bash scripts/run_flink_recovery_test.sh
```

The comparator uses stable raw and metric business columns, excluding ingestion
timestamps and replay lineage. See [operations](docs/OPERATIONS.md).

## Verification boundary

Credential-independent tests prove code, state-harness, DDL/CQL, profile, and
packaging contracts. They do not prove Kafka permissions, live checkpoints,
ClickHouse merges, Cassandra connectivity, or recovery on a real cluster.

The platform uses:

- checkpoint-consistent Flink state and Kafka offsets;
- at-least-once external writes;
- effectively-once ClickHouse canonical queries;
- no cross-system transactional exactly-once guarantee.

## Teardown

```bash
bash scripts/stop.sh
# Destructive local-volume removal only after checking the Compose project:
bash scripts/stop.sh --volumes
```

Cloud provisioning and teardown require separate explicit approval. See
[the operator runbook](docs/RUNBOOK.md).
