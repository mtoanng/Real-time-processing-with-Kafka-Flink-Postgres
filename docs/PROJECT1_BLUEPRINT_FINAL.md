# Project 1 active blueprint

## Release target

```text
Python deterministic replay
  -> Kafka + Schema Registry
  -> one Flink 1.20.2 SQL statement set
  -> Kafka output contracts
  -> ClickHouse canonical raw, one-minute metrics, and quality evidence
```

The Data Engineer authoring interface is Python and SQL. Flink remains a JVM
engine and uses prebuilt JVM connectors. No project-authored Java business
pipeline is required by the active build or runtime.

## Invariants

1. `event_id` is replay-independent canonical identity.
2. `replay_run_id` is lineage only.
3. Raw history is source-faithful and unique by `event_id`.
4. Metric grain is `(window_start, item_id, source_category_id)`.
5. Invalid, duplicate, and late classifications are durable.
6. Deduplication state is bounded by a configurable TTL.
7. Persistent checkpoints protect Flink state and Kafka positions.
8. ClickHouse canonical reads use explicit replacement semantics.
9. The system does not claim global exactly-once delivery.

## Active versus legacy

Active: Python replay, Kafka, Schema Registry, Flink SQL, ClickHouse.

Legacy/non-active: Java DataStream job, Redis serving, HTTP API, PostgreSQL,
Debezium, and Product CDC. These artifacts must not block or redefine core
analytics.

Future Product CDC enrichment is **NOT IMPLEMENTED** and **NOT VERIFIED**. It
must be separately approved and must replace—not coexist with—legacy behavior
rule CDC semantics.

## Verification status

Credential-independent checks establish codebase readiness. Real Kafka/Flink/
ClickHouse execution and recovery evidence are required before marking
deployment verification complete.
