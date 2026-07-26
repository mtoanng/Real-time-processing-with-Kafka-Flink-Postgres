# Layer 1: General Architecture

## 1. Purpose

The platform turns raw Alibaba Tianchi Taobao behavior rows into explainable
streaming outputs:

- source-faithful accepted event history;
- one-minute behavior metrics by item and source category;
- durable classifications of invalid, duplicate, and late events;
- optionally, current active-cart items for one user.

The design is deliberately bounded. A committed 1,000-row fixture supports
tests and demonstrations. The raw full dataset is not committed, and a
full-scale run is optional.

## 2. Input grain

The source dataset is the raw, headerless `UserBehavior.csv` with exactly five
columns:

```text
user_id,item_id,category_id,behavior_type,timestamp
```

One row means one user performed one behavior on one item at one event-time
second. `behavior_type` is `pv`, `cart`, `fav`, or `buy`.

This event grain is the first decision to understand. The system does not begin
with sessions, aggregates, recommendations, features, or customer profiles.
Those would discard or invent information before the streaming system sees it.

## 3. System topology

```text
                    control and contracts
                 +--------------------------+
                 | Schema Registry: Avro ID |
                 +------------+-------------+
                              |
                              v
raw CSV -> Python replay -> Kafka topic -> Java Flink job
                                        |
                 +----------------------+----------------------+
                 |                      |                      |
                 v                      v                      v
        accepted unique raw      one-minute metrics       quality events
                 |                      |                      |
                 +----------------------+----------------------+
                                        |
                                        v
                                   ClickHouse
                        physical replacement-capable tables
                                        |
                                        v
                                  canonical views

optional serving branch:
accepted unique -> keyed active-cart state -> Cassandra user_active_cart
```

There is one behavior-event Kafka topic and one Java Flink application
artifact. Optional profiles activate branches in that same job; they do not
create a second clickstream pipeline.

## 4. Why each technology exists

| Technology | Architectural responsibility | What it must not own |
| --- | --- | --- |
| Python | Read raw CSV in bounded batches, validate encoding-level fields, create deterministic identity, preserve order, pace replay, publish Avro | Streaming aggregation, Flink state, analytical storage |
| Kafka | Durable ordered transport within partitions and replay/consumer decoupling | Business aggregation or canonical analytical truth |
| Schema Registry | Shared Avro value contract and schema-ID resolution | Event validation beyond encodability |
| Java Flink DataStream | Semantic validation, keyed State-TTL deduplication, event-time/watermarks, windows, optional stateful projections, checkpoint recovery | Long-term analytical history |
| ClickHouse | Analytical history, rollups, durable quality evidence, canonical query boundary | Per-event Flink recovery state |
| Cassandra | Optional bounded current cart keyed by user | Raw history, metrics, arbitrary querying |
| PostgreSQL/Debezium | Deprecated behavior-rule compatibility artifacts only | Taobao clickstream source or current target CDC design |
| Grafana | Optional visualization of ClickHouse data | Source-of-truth computation |

## 5. Identity versus lineage

`event_id` is calculated in Python:

```text
SHA-256(
  user_id,
  item_id,
  category_id,
  behavior_type,
  timestamp,
  source_sequence
)
```

The fields are joined with a stable unit-separator character before hashing.
The replay attempt is absent. Therefore, the same source row at the same source
position has the same event ID in every attempt.

`replay_run_id` answers a different question: “During which attempt was this
record observed?” It is useful for diagnostics and quality reconciliation but
must not change raw uniqueness or metric grouping.

These identities prevent a retry from creating a new business event merely
because the retry has a new run name.

## 6. Kafka key

The Python producer uses the decimal `user_id` string as the Kafka key.

This preserves per-user partition ordering while allowing many users to spread
across partitions. It also matches the optional user-keyed cart and deprecated
user-keyed timer branches. Flink re-keys later where business logic requires a
different key:

- by `event_id` for deduplication;
- by `(item_id, source_category_id)` for metrics;
- by `user_id` for active cart and deprecated alerts.

Kafka partitioning and Flink keying solve related but different problems.
Kafka chooses transport partitions; `keyBy` redistributes records for a
particular stateful computation.

## 7. Core event lifecycle

```text
decoded
  |
  +-- semantically invalid ----------> INVALID quality
  |
  +-- valid
        |
        +-- event_id already seen ----> DUPLICATE quality
        |
        +-- accepted unique ----------> canonical raw sink
                 |
                 +-- time <= watermark -> LATE quality
                 |
                 +-- on time ----------> one-minute metric window
```

The accounting equations are:

```text
decoded = invalid + valid
valid = duplicate + accepted_unique
accepted_unique = on_time + late_for_aggregation
canonical_raw = accepted_unique
metrics_input = on_time
```

An accepted unique event is written to raw history before lateness determines
whether it may update metrics. Lateness is an aggregation decision, not a
reason to erase historical truth.

## 8. Validation boundary

Python rejects rows that cannot form the Avro event contract, for example:

- wrong number of CSV columns;
- non-integer IDs or timestamp;
- unsupported behavior string;
- blank replay run ID.

Those rows never reach Kafka and count as producer rejections.

Flink handles semantic invalidity after successful decoding, for example a
non-positive identifier or negative event timestamp. This distinction lets the
system reconcile “could not publish” separately from “published but invalid
for business processing.”

## 9. Bounded deduplication

After validation, Flink keys by `event_id`. `EventDeduplicator` stores first
seen processing time in checkpointed `ValueState<Long>`.

State TTL defaults to 168 hours:

```text
FLINK_DEDUP_RETENTION_HOURS=168
```

Within that horizon, the first valid occurrence is accepted and later
occurrences become `DUPLICATE` quality events. TTL bounds state growth. After
TTL expires, the same event may be accepted again, so the guarantee is not
eternal global uniqueness.

ClickHouse logical keys provide another convergence layer. If the same
accepted event is written more than once because an external sink retry occurs,
canonical reads still collapse replacement rows by stable identity.

## 10. Event time and watermarks

The Avro event carries `event_time_ms`, derived from the source timestamp.
After deduplication, Flink assigns timestamps and emits a watermark on every
event:

```text
watermark = maximum event timestamp seen - 5 seconds - 1 millisecond
```

The custom immediate generator makes fixture behavior deterministic.
Thirty-second idleness prevents an idle source partition from indefinitely
holding back the combined watermark.

`LateEventRouter` treats an event as late when:

```text
event_time_ms <= current_watermark
```

Late events remain in raw canonical history, emit `LATE` quality, and do not
enter metric windows.

## 11. One-minute metrics

On-time events are keyed by:

```text
(item_id, source_category_id)
```

They enter one-minute tumbling event-time windows with zero allowed lateness.
The accumulator counts `pv`, `cart`, `fav`, and `buy`, and maintains a set of
user IDs for exact distinct users in the bounded demo.

The logical row grain is:

```text
(window_start, item_id, source_category_id)
```

`source_category_id` is the category copied from the original event. A future
product catalog cannot redefine historical metric identity.

## 12. Storage ownership and physical design

### ClickHouse

`raw_behavior_events` stores accepted unique events. Its replacement ordering
key is `(toDate(event_time), event_id)`.

`item_metrics_1m` stores one logical aggregate per minute, item, and source
category.

`stream_quality_events` stores deterministic `INVALID`, `DUPLICATE`, and
`LATE` classifications.

All three use `ReplacingMergeTree`. Merges are asynchronous, so ordinary table
queries can temporarily show multiple physical versions. Consumers use:

```text
raw_behavior_events_canonical
item_metrics_1m_canonical
stream_quality_events_canonical
```

These views explicitly select from their tables with `FINAL`.

### Cassandra

The optional table is:

```sql
PRIMARY KEY ((user_id), item_id)
```

The partition key supports exactly one query shape: retrieve the current cart
items for one user. `cart` upserts an item, `buy` deletes it, and `pv`/`fav`
produce no mutation. Event time and source sequence reject stale transitions.

### Deprecated behavior alerts

`behavior_alerts` is separate from stream quality. It belongs to the old
PostgreSQL/Debezium behavior-rule and timer branch. It is retained for
compatibility but is not part of the core release target.

## 13. Runtime profiles

| Profile | Services and branches | Status |
| --- | --- | --- |
| `checks` | No services and no submitted job | Active local/static verification boundary |
| `core` | Kafka, Schema Registry, Flink, ClickHouse; raw, metric, quality branches | Active release core |
| `serving` | Core plus Cassandra and active-cart branch | Optional |
| `observability` | Core plus Grafana; Java topology unchanged | Optional |
| `cdc` | Core services plus PostgreSQL/Debezium and legacy rule/timer branch | Deprecated compatibility only |

Core must not read Cassandra, Astra, PostgreSQL, Debezium, or Grafana
configuration.

## 14. Recovery model

When enabled, Flink checkpoints:

- Kafka source offsets;
- event-ID deduplication state;
- open metric window state;
- optional active-cart state;
- deprecated broadcast and timer state when the CDC profile is used.

The job uses `CheckpointingMode.EXACTLY_ONCE`, persistent checkpoint storage,
retained externalized checkpoints, a fixed-delay restart strategy, and stable
operator UIDs.

This means Flink can restore its managed computation and Kafka position
consistently. ClickHouse and Cassandra do not participate in a Flink
transaction. Sink writes can repeat around a failure.

The design converges through:

- stable event and quality identifiers;
- replay-independent metric keys;
- replacement-capable ClickHouse tables plus canonical views;
- deterministic Cassandra primary-key mutations.

This is recoverable and effectively once at canonical query boundaries, not
globally exactly once.

## 15. Deployment boundary

The complete stack must not run on the constrained laptop. Local tests use the
fixture without service connections. A disposable remote host is the intended
integration environment.

Terraform describes an optional temporary AWS host, optional Confluent Cloud
resources, and optional Astra resources. It has been statically validated, but
no cloud deployment is claimed. The Confluent definitions still include
deprecated rule and Kafka Connect resources; their presence does not make CDC a
current release target.

## 16. Current versus future CDC

Present but deprecated:

```text
PostgreSQL behavior_rules
  -> Debezium
  -> compacted behavior-rules Kafka topic
  -> Flink Broadcast State and event-time timers
  -> ClickHouse behavior_alerts
```

Approved future direction, not implemented:

```text
PostgreSQL product_catalog
  -> Debezium
  -> product changelog topic
  -> Flink current-state enrichment
```

The future branch must replace the old rule branch. No product table, product
schema, connector, Flink product state, enriched ClickHouse table, or Cassandra
product snapshot exists now.

