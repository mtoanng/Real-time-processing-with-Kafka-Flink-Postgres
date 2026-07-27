# Project 1 Blueprint — Recoverable Event-Time Streaming Core

This file is the active source of truth. Historical phase reports and archived
F1 documents describe earlier contracts and do not override it.

## Goal

Build a bounded, explainable streaming platform for the raw Alibaba Tianchi
Taobao `UserBehavior.csv` dataset:

```text
raw Taobao rows
-> deterministic Python replay
-> Kafka + Schema Registry
-> one Java Flink DataStream job
-> ClickHouse canonical history, one-minute metrics, and quality events
```

The project is not a recommendation system and does not include AI/ML,
Kubernetes, S3 event archival, a frontend, or a second processing engine.

## Dataset contract

The input is the raw five-column `UserBehavior.csv`:

```text
user_id,item_id,category_id,behavior_type,timestamp
```

`behavior_type` is one of `pv`, `cart`, `fav`, or `buy`. The raw dataset is not
committed. A deterministic 1,000-row fixture is committed for tests and bounded
experiments.

Malformed CSV or non-Avro-encodable rows rejected by Python are producer
rejections. Semantically invalid decoded events are classified by Flink.

## Runtime profiles

| Profile | Boundary |
| --- | --- |
| `checks` | Tests, compilation, schema checks, Compose rendering, Terraform validation; no connections |
| `core` | Kafka, Schema Registry, one Flink job, ClickHouse |
| `serving` | Core plus optional Valkey/Redis active-cart projection |
| `cdc` | Deprecated legacy PostgreSQL/Debezium behavior-rule compatibility only |
| `observability` | Core plus Grafana backed by ClickHouse |

All runtime profiles reuse the same Java job. Redis, PostgreSQL,
Debezium, and Grafana settings are not read by `core`.

## Event identity and lineage

`event_id` is SHA-256 over stable source fields and `source_sequence`. The same
row at the same source sequence has the same ID across replay attempts.

`replay_run_id` is lineage only. It is not a deduplication key, aggregation key,
raw uniqueness key, metric uniqueness key, or canonical comparison field.

## Flink topology

```text
decoded
  -> semantic validation
       -> INVALID quality event
       -> valid
            -> keyed event_id deduplication with State TTL
                 -> DUPLICATE quality event
                 -> accepted unique
                      -> canonical raw sink
                      -> timestamps + watermarks
                           -> LATE quality event
                           -> on-time one-minute metrics
```

The accounting equations are:

```text
decoded = invalid + valid
valid = duplicate + accepted_unique
accepted_unique = on_time + late_for_aggregation
canonical_raw = accepted_unique
metrics_input = on_time
```

Deduplication is guaranteed within `FLINK_DEDUP_RETENTION_HOURS`; the bounded
demo default is 168 hours. State TTL prevents unbounded retention. A late event
remains in canonical raw history but cannot change a closed metric window.

Metric grain and key:

```text
(window_start, item_id, source_category_id)
```

The DataStream key is `(item_id, source_category_id)` and the window is a
one-minute tumbling event-time window. `source_category_id` is the original
Taobao event field; future catalog data cannot redefine core metric identity.

## ClickHouse contracts

- `raw_behavior_events`: accepted unique history, replacement key stable on
  event date and `event_id`.
- `item_metrics_1m`: one logical row per
  `(window_start, item_id, source_category_id)`.
- `stream_quality_events`: durable `INVALID`, `DUPLICATE`, and `LATE`
  classifications keyed by deterministic `quality_event_id`.
- `behavior_alerts`: deprecated legacy CDC/timer output, separate from core
  quality.

Canonical consumers use `raw_behavior_events_canonical`,
`item_metrics_1m_canonical`, and `stream_quality_events_canonical`. These views
use `FINAL`; ordinary table reads are not an immediate deduplication guarantee.

## Recovery and delivery

Runtime profiles enable checkpoints by default and require persistent
`FLINK_CHECKPOINT_DIR`. Flink uses `CheckpointingMode.EXACTLY_ONCE` for managed
state and Kafka offsets, stable operator UIDs, and configurable fixed-delay
restart settings.

Guarantees:

```text
Flink state and Kafka offsets: checkpoint-consistent recovery
ClickHouse: at-least-once writes plus effectively-once canonical reads
overall platform: not transactional global exactly once
```

## Optional Valkey/Redis boundary

The serving profile maintains one bounded Redis Hash per user:

```text
key:   taobao:active_cart:{user_id}
field: item_id
value: category_id|added_at_ms|last_updated_at_ms
```

`cart` upserts, `buy` deletes, and `pv`/`fav` do nothing. Event-time/source
ordering prevents a stale cart from recreating an item after a newer buy.
Repeated `HSET`/`HDEL` mutations are harmless. Every mutation refreshes a
required bounded cart TTL. Writes remain synchronous; asynchronous and
pipelined throughput work is deferred until a real benchmark justifies it.
Local and managed Redis-compatible endpoints share one code path using
host/port, optional ACL credentials, and optional TLS.

## Deprecated legacy CDC boundary

The repository still contains the previous implementation:

```text
PostgreSQL behavior_rules
-> Debezium
-> compacted Kafka rules topic
-> Flink Broadcast State
```

It is not the clickstream source, is not required by core, and is not the target
CDC architecture. Do not expand or harden it during this phase.

The approved future migration target is:

```text
PostgreSQL product_catalog
-> Debezium
-> Kafka product changelog
-> Flink current-state product enrichment
```

It will replace rather than coexist with the behavior-rule branch. Product CDC
Enrichment is **NOT IMPLEMENTED** and **NOT VERIFIED**. This phase creates no
product table, connector configuration, changelog schema, product state,
enriched ClickHouse table, or Redis product snapshot field.

## This bounded phase

The one active implementation phase is **Redis serving simplification
migration**:

1. replace optional Cassandra/Astra active-cart serving with Valkey/Redis;
2. preserve the existing user-keyed cart/buy ordering business logic;
3. use one bounded TTL Hash per user with idempotent `HSET`/`HDEL`;
4. keep Redis isolated from `core`;
5. remove Cassandra/Astra code, dependencies, configuration, infrastructure,
   scripts, and active documentation;
6. verify the replacement with credential-independent tests and profile
   contracts.

Async/pipelined sink tuning, Redis Cluster, Sentinel, performance benchmarking,
product CDC, savepoint upgrades, and cloud deployment are later phases and are
not part of this work.

## Release gates

`CODEBASE-READY` requires all credential-independent tests, package builds,
schema contracts, Compose profiles, and Terraform validation to pass.

`DEPLOYMENT-VERIFIED` additionally requires real bounded service evidence for
Kafka delivery, Flink execution/recovery, ClickHouse canonical results, and
optional profile behavior. Static validation is not deployment evidence.
