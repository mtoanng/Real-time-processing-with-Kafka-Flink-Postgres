# Architecture

## Core data plane

```text
UserBehavior.csv fixture/bounded subset
  -> Python replay (event_id, replay_run_id)
  -> Kafka Avro topic + Schema Registry
  -> one Java Flink job
       -> semantic validation
       -> event_id keyed State-TTL deduplication
       -> ClickHouse canonical raw
       -> timestamps/watermarks
       -> one-minute (item_id, source_category_id) metrics
       -> ClickHouse stream quality
```

There is one Flink deployment artifact and one source topic for behavior
events. Debezium is never the behavior-event source.

## Runtime boundaries

| Profile | Services | Java branches |
| --- | --- | --- |
| `checks` | None | Job not submitted |
| `core` | Kafka, Schema Registry, Flink, ClickHouse | Canonical raw, metrics, quality |
| `serving` | Core plus Valkey/Redis | Core plus active-cart projection |
| `cdc` | Deprecated legacy PostgreSQL/Debezium profile | Legacy Broadcast State/timers only |
| `observability` | Core plus Grafana | Core job unchanged |

## Storage ownership

ClickHouse owns immutable analytical history as a logical model, materialized
with replacement-capable physical tables. It also owns one-minute analytical
rollups and durable quality classifications.

Valkey/Redis is optional and owns only bounded current active-cart Hashes for
lookup by `user_id`. It is not analytical history. Each user key has a required
TTL.

The existing PostgreSQL/Debezium `behavior_rules` control plane, Broadcast
State processor, cart-abandonment timers, and `behavior_alerts` sink are
deprecated legacy artifacts. They are isolated from `core` and are not a
current release target.

## CDC migration decision

A separately approved phase will replace, not coexist with, the legacy rule
branch:

```text
PostgreSQL product_catalog
  -> Debezium
  -> Kafka product changelog
  -> Flink current-state product enrichment
```

That phase is **NOT IMPLEMENTED** and **NOT VERIFIED**. It must preserve
`raw_behavior_events` as source-faithful canonical history, keep core metrics
on `source_category_id`, and make all product data optional to the core path.
No product schema, connector, state, enriched table, or Redis product
snapshot field exists in this release.

## Recovery boundary

Flink checkpoints contain Kafka offsets, event-ID dedup state, window state,
and optional active-cart/control-plane state. Stable UIDs protect operator
identity across process restarts of the same artifact.

ClickHouse and Redis are external systems, not participants in a Flink
distributed transaction. Deterministic identifiers and idempotent logical keys
make bounded replay converge through canonical reads.
