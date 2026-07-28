# Active blueprint: minimal event-time streaming core

This file is the source of truth for implementation scope. The current phase is
the semantic-preserving repository simplification explicitly authorized on
`refactor/redis_store_migration`.

## Architecture

```text
raw Taobao rows -> Python replay -> Kafka Avro + Schema Registry
-> one Java Flink DataStream job
-> ClickHouse canonical raw, one-minute metrics, and quality
-> Redis/Valkey active-cart projection
```

The runtime contains Kafka, Schema Registry, ClickHouse, Redis, and one Flink
JobManager/TaskManager pair. Checks start no services. There are no optional
application profiles.

## Required semantics

- Event identity is SHA-256 of the five source fields plus source sequence;
  replay run ID is lineage only.
- Semantic validation precedes bounded event-ID deduplication.
- Accepted unique events always reach canonical raw.
- Events at or behind the watermark create late evidence and skip metrics.
- Metric identity is `(window_start, item_id, source_category_id)`.
- ClickHouse canonical views are logically duplicate-free without waiting for
  merges.
- Redis stores only bounded-TTL, user-keyed active carts; cart upserts and buy
  deletes with stale-transition protection.
- Checkpoints coordinate Flink state and Kafka offsets. External sinks converge
  through deterministic IDs and logical keys; there is no global transaction.

The exact accounting equations and golden outputs are defined in
`docs/SEMANTICS.md` and `tests/fixtures/golden_outputs.json`.

## Delivery gate

Codebase verification requires Python tests, Java tests/package, schema
contracts, and Compose rendering. Live runtime and recovery claims remain
`NOT VERIFIED` until the commands in `docs/RUNBOOK.md` produce real evidence.
