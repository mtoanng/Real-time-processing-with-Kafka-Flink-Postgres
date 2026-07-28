# SQL-first Taobao streaming analytics

This portfolio project demonstrates deterministic replay, event identity,
event-time processing, bounded state, recovery, and canonical analytical reads.
Data Engineers author Python and SQL; Flink and connector implementations remain
JVM platform capabilities.

```text
Taobao UserBehavior.csv
  -> deterministic Python replay
  -> Kafka + Schema Registry (Confluent Avro wire format)
  -> Flink 1.20.2 SQL
       validation
       event_id deduplication with State TTL
       watermarks and one-minute event-time metrics
       durable INVALID / DUPLICATE / LATE classifications
  -> Kafka output contracts
  -> ClickHouse Kafka Engine materialization
  -> canonical ReplacingMergeTree views
```

## Responsibilities

| Layer | Responsibility |
| --- | --- |
| Python | replay, deterministic IDs, configuration, tests, reconciliation |
| Flink SQL | validation, deduplication, event time, windows, metrics |
| Flink JVM | runtime, checkpointed Kafka offsets/state, connector execution |
| ClickHouse | canonical raw history, metrics, and durable quality evidence |

The active authoring surface contains no project Java. The source-free Maven
module packages prebuilt Kafka/Avro connector classes for SQL Client. The former
Java DataStream job under `flink-jobs/` is retained as non-active migration
evidence and is excluded from the root build.

## Checks

Credential-independent checks open no service connections:

```bash
python -m pip install -e ".[kafka]"
python -m pip install ruff
make checks
```

They validate deterministic replay, Avro contracts, SQL semantics, rendering,
Python behavior, connector packaging, shell syntax, and Compose rendering.

## Minimal core demo

Prerequisites: Python 3.11+, Maven, Docker Compose, Bash, and enough disk for
Kafka, Flink, and ClickHouse images.

```bash
cp .env.example .env
make start
make replay
make verify
make stop
```

`make replay` publishes the 12-row fixture as `golden-a` and `golden-b`, then
submits one bounded SQL statement set. The second replay has the same
`event_id` values because `replay_run_id` is lineage only.

## Canonical outputs

- `raw_behavior_events_canonical`: one source-faithful row per `event_id`.
- `item_metrics_1m_canonical`: one row per
  `(window_start, item_id, source_category_id)`.
- `stream_quality_events_canonical`: durable invalid, duplicate, and late
  classifications.

Canonical views use `FINAL`. Ordinary base-table reads may expose physical
retry rows before ClickHouse merges them.

## Delivery and recovery boundary

- Kafka offsets and Flink-managed state recover consistently from completed
  checkpoints.
- Flink SQL output topics use at-least-once delivery.
- ClickHouse uses stable logical keys and `ReplacingMergeTree` to provide
  effectively-once canonical query results.
- There is no global transaction across Kafka, Flink, and ClickHouse.

Checkpoint storage is persisted in the `flink_checkpoints` volume. The live
failure/recovery experiment remains **NOT VERIFIED** until executed against the
real Compose runtime.

## Non-active legacy extensions

Redis cart serving, the HTTP API, PostgreSQL/Debezium Product CDC, and the old
Java DataStream job remain in the repository only as migration evidence. They
are not started by the `core` profile, do not participate in canonical history
or metrics, and are not part of the release target.

Read [semantics](docs/SEMANTICS.md), the [runbook](docs/RUNBOOK.md), and the
[active blueprint](docs/PROJECT1_BLUEPRINT_FINAL.md) before changing contracts.

Status: **CODEBASE-READY only after `make checks` passes; DEPLOYMENT-VERIFIED:
NOT VERIFIED.**
