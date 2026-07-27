# Layer 2: Component Guide

This layer treats each runtime area as a black box first: what enters, what
leaves, what state it owns, how it fails, and how it is verified.

## 1. Component map

| Component | Input | Output | State/ownership | Active status |
| --- | --- | --- | --- | --- |
| Source audit | Raw `UserBehavior.csv` plus acquisition manifest | Satisfied audit or explicit rejection | File hash/profile only | Active preparation |
| Replay reader | Raw five-column rows | Parsed events and parse issues | One bounded batch | Active core input |
| Replay publisher | Parsed event | Kafka keyed Avro record | Producer delivery callbacks | Active core input |
| Kafka | User-keyed event records | Partitioned event stream | Topic log and consumer offsets | Active core |
| Schema Registry | Avro schema | Schema ID resolution | Registered subject versions | Active core |
| Flink validation | Decoded event | Valid main output or invalid side output | Stateless | Active core |
| Flink deduplication | Valid event keyed by event ID | Accepted unique or duplicate quality | TTL `ValueState` | Active core |
| Watermark/late routing | Accepted unique event | On-time or late side output | Maximum observed timestamp per source operator | Active core |
| Metric aggregation | On-time event keyed by item/category | One-minute metric | Window accumulator and user-ID set | Active core |
| ClickHouse sinks | Raw, metric, quality records | Physical table writes | External at-least-once storage | Active core |
| ClickHouse canonical views | Replacement tables | Logical canonical results | Query-time `FINAL` collapse | Active core |
| Active-cart projector | Accepted unique user-keyed event | Upsert/delete mutation | Per-user item `MapState` | Optional serving |
| Redis sink | Cart mutation | Per-user Hash field change | External bounded hot state | Optional serving |
| Grafana | ClickHouse query results | Dashboards | Dashboard configuration | Optional observability |
| Legacy rule control plane | PostgreSQL changes | Rule Avro stream | PostgreSQL and compacted topic | Deprecated |
| Legacy alert processor | On-time event plus broadcast rule | Behavior alert | Broadcast state, pending carts, timers | Deprecated |

## 2. Raw source and source audit

Files:

- `config/dataset-source.example.json`
- `producer/src/taobao_replay/source_audit.py`
- `producer/src/taobao_replay/profile.py`

The audit requires the official Tianchi host and dataset ID 649, the exact
filename `UserBehavior.csv`, the `raw_event_rows` dataset kind, a timestamped
acquisition manifest, a 64-character SHA-256 value, and a positive expected row
count.

The full raw Tianchi file must be headerless. A matching hash and row count
prove that the candidate file is the acquired artifact described by the
manifest. The audit also profiles accepted/invalid rows, behavior counts,
minimum/maximum event time, and backward time jumps using bounded memory.

The fixture reader is slightly more permissive than the raw-source audit: it
accepts either headerless CSV or one canonical header. This makes committed
test fixtures convenient without weakening the production source contract.

Failure boundary: audit or parsing errors are local and occur before Kafka.
They cannot become Flink quality events because they were never published.

## 3. Deterministic replay

Files:

- `producer/src/taobao_replay/contracts.py`
- `reader.py`
- `replay.py`
- `cli.py`

`iter_source_rows` preserves row order and assigns zero-based source sequence.
`iter_event_batches` parses at most `batch_size` rows at a time and separates
accepted events from `ParseIssue` values.

`replay_file` calls an injected `emit` function for each accepted event and an
optional rejection callback for each parse issue. With `speed=0`, it emits as
fast as possible. With `speed>0`, it maps source event-time differences to
wall-clock delay divided by the speed factor.

The CLI exposes:

- `profile`: bounded source statistics;
- `source-audit`: official raw-source verification;
- `replay`: accepted JSONL plus optional rejected JSONL;
- `publish`: Confluent Avro publication plus optional rejected JSONL.

Output files default to exclusive creation. `--force` is required to replace
them, and input/output path overlap is rejected.

## 4. Kafka producer and topic

Files:

- `producer/src/taobao_replay/kafka.py`
- `schemas/user-behavior-event.avsc`
- `scripts/register_schemas.sh`
- `scripts/replay.sh`

The value serializer uses the committed Avro schema with automatic
registration disabled. Schema provisioning is a separate operator action.

Producer settings include:

```text
enable.idempotence=true
acks=all
```

These settings improve producer retry safety but do not replace application
event identity or prove end-to-end exactly once.

Security supports local `PLAINTEXT` and managed `SASL_SSL`. Partial SASL
configuration fails fast. Schema Registry basic authentication is optional.

The Kafka key is the user ID string. The topic defaults to
`user-behavior-events`.

`KafkaEventPublisher.close` flushes pending messages and raises an error for a
timeout or delivery callback failure. A successful `produce` call alone is not
treated as delivery evidence.

## 5. Avro contract and generated Java types

The event schema contains:

| Field | Purpose |
| --- | --- |
| `event_id` | Replay-independent logical identity |
| `user_id` | User and Kafka partitioning identity |
| `item_id` | Item dimension |
| `category_id` | Original source category |
| `behavior_type` | Avro enum: `pv`, `cart`, `fav`, `buy` |
| `event_time_ms` | Source event time in milliseconds |
| `source_sequence` | Source row position |
| `replay_run_id` | Attempt lineage |

The Maven Avro plugin generates `UserBehaviorEvent`, `BehaviorType`, and legacy
`BehaviorRule` Java classes under `target/generated-sources/avro`. Generated
files are build artifacts and are not hand-edited.

## 6. Kafka source and configuration

Files:

- `TaobaoStreamJob.java`
- `RuntimeProfileConfig.java`

The Flink `KafkaSource` starts from earliest offsets when no committed group
offset exists. It uses a value-only Confluent Registry deserializer because the
Kafka key is not required in the event object after partitioning.

Kafka offsets become part of Flink checkpoints. The group ID defaults to
`taobao-stream-job`.

`RuntimeProfileConfig` validates the selected profile before opening services.
It also maps local or managed Kafka security and Schema Registry credentials.
The checks profile cannot submit a job.

## 7. Semantic validation

File: `processing/EventValidator.java`.

This operator is a `ProcessFunction`. A valid event is collected to the main
stream. Invalid events become `StreamQualityEvent` values on the
`invalid-events` side output.

It checks:

- nonblank event ID;
- positive user, item, and category IDs;
- non-null behavior type;
- non-negative event time;
- non-negative source sequence;
- nonblank replay run ID.

Reason codes are stable categories such as `INVALID_IDENTIFIER` and
`INVALID_EVENT_TIME`. The human-readable message remains available separately.

## 8. Event-ID deduplication

Files:

- `processing/DeduplicationConfig.java`
- `processing/EventDeduplicator.java`

The valid stream is re-keyed by event ID. A `ValueState<Long>` named
`seen-event-id` records processing time.

TTL behavior:

- update only when the value is created or written;
- never return an expired value;
- clean expired state during full snapshots;
- accept retention between one hour and one year;
- default to seven days.

A duplicate emits a quality event with
`DUPLICATE_WITHIN_RETENTION`. It never reaches raw, metrics, Redis, or the
deprecated alert branch.

## 9. Watermarks and lateness

Files:

- `ImmediateBoundedOutOfOrdernessGenerator.java`
- `LateEventRouter.java`

The accepted stream has no watermarks before deduplication. This prevents
invalid or duplicate observations from advancing event-time progress.

The generator emits immediately on every accepted event, using five seconds of
bounded disorder. `LateEventRouter` compares each event time with the current
watermark.

The `late-events` side output is mapped to `LATE_FOR_AGGREGATION` quality.
The main output alone feeds metrics.

## 10. Metric aggregation

Files:

- `model/ItemCategoryKey.java`
- `aggregation/ItemMetricsAccumulator.java`
- `ItemMetricsAggregator.java`
- `ItemMetricsWindowFunction.java`
- `model/ItemMetrics1m.java`

`ItemCategoryKey` implements equality and hashing for item plus source
category. The accumulator holds four behavior counters, a `HashSet<Long>` of
users, and lineage metadata.

The aggregate function updates counters and exact distinct users incrementally.
The window function adds window start and business key to create
`ItemMetrics1m`.

The accumulator selects lineage from the greatest source sequence, breaking a
tie with replay-run string order. This makes the stored lineage deterministic;
it does not enter the metric key or business comparison.

The exact user set is acceptable for a bounded teaching fixture. It is not a
demonstrated full-scale cardinality strategy.

## 11. Stream quality model

File: `model/StreamQualityEvent.java`.

Core quality types are `INVALID`, `DUPLICATE`, and `LATE`.

`quality_event_id` hashes:

```text
quality type
event ID
replay run ID
source sequence
reason code
```

Therefore, retrying the same classification within one replay attempt converges
to one logical quality row, while a later replay attempt stays separately
visible.

`observed_at` is operational time and becomes the replacement version. It is
not used in stable raw or metric comparisons.

## 12. ClickHouse sink and tables

Files:

- `sink/ClickHouseRowMapper.java`
- `sink/ClickHouseSinkFactory.java`
- `infra/clickhouse/schema.sql`
- `infra/clickhouse/verify.sql`

The row mapper converts Java values to column maps and UTC timestamps. The sink
factory constructs asynchronous ClickHouse sinks using `JSONEachRow`.

Current batching differs by record type: raw and quality favor smaller batches;
metrics allow larger batches. These values are implementation settings, not
benchmarked tuning.

The physical tables use deterministic ordering keys and `record_version`.
Canonical views use `FINAL`. Stable verification excludes:

- `replay_run_id`;
- `ingested_at`;
- quality observation time when comparing raw/metrics.

This separation is crucial: lineage and transport timing may differ even when
the business result is identical.

## 13. Optional Valkey/Redis serving

Files:

- `processing/ActiveCartProjector.java`
- cart model classes under `model/`
- `sink/RedisConfig.java`
- `RedisClientFactory.java`
- `RedisCartCodec.java`
- `RedisActiveCartSink.java`
- `ActiveCartLookupCli.java`

The stream is keyed by user ID. `MapState<item_id, CartItemState>` remembers
the latest cart/buy ordering for that user and item.

Ordering is `(event_time_ms, source_sequence)`. An older or equal transition is
ignored. A new `cart` creates or updates the active row; a `buy` records
inactive state and emits a delete.

The sink opens one pooled Redis-compatible client per sink instance, verifies
it with `PING`, executes synchronous `HSET` or `HDEL`, refreshes the required
cart TTL, and closes the client. Repeated writes converge for the same
user/item field.

Local and managed endpoints share host, port, optional ACL credentials, and
optional TLS configuration. Cluster/Sentinel discovery, pipelining, and async
I/O are intentionally deferred until a measured requirement exists.

The lookup CLI supports only:

```text
HGETALL taobao:active_cart:{user_id}
```

There is intentionally no arbitrary Redis command interface.

## 14. Optional observability

Files:

- `infra/grafana/provisioning/`
- `infra/docker-compose.yml`

The observability profile adds Grafana and its ClickHouse datasource/dashboard
configuration. It does not change the Java topology. Dashboard results are
downstream views, not independent business computations.

## 15. Deprecated legacy CDC

Files:

- `infra/postgres/schema.sql`
- `infra/debezium/postgres-behavior-rules.json`
- `schemas/behavior-rule*.avsc`
- `processing/CartAbandonmentRuleProcessor.java`
- `processing/RuleVersionPolicy.java`
- `model/BehaviorAlert.java`

The old branch captures changes to one `behavior_rules` row, routes them to a
compacted Kafka topic, and broadcasts newer rule versions to all Flink tasks.
User-keyed cart events create pending state and event-time timers. A buy removes
pending state; a due timer can emit `BehaviorAlert`.

This code compiles and has unit/contract coverage only for compatibility. It is
not core, not a release target, and not the approved future CDC design.

## 16. Runtime and infrastructure

`infra/docker-compose.yml` defines local services by profile. The shared Flink
environment contains core settings and optional settings, but
`RuntimeProfileConfig` controls which optional settings are actually read.
ClickHouse and checkpoint data use named volumes.

Terraform defines:

- a temporary AWS VPC, subnet, route, SSH-restricted security group, Elastic
  IP, and EC2 demo host;
- optional Confluent environment, Kafka cluster, topics, identity, ACLs, and
  schemas;
- operator-provided managed Redis endpoint; Terraform does not provision it.

Defaults keep Confluent resource creation disabled. Terraform validation is not
deployment evidence. Cloud creation or teardown requires explicit approval.

The Terraform Confluent file still declares legacy rule and Connect resources.
Treat those declarations as deprecated artifacts, not an instruction to deploy
CDC.

## 17. Independent verification components

`scripts/reconcile_final_e2e.py` computes expected results from the fixture
without using Flink or ClickHouse code paths for aggregation. This independence
helps detect shared implementation errors.

`scripts/verify_clickhouse.py` queries canonical views, normalizes numeric
fields, computes stable digests, and compares counts and quality classes.

`scripts/canonical_snapshot.py` captures stable raw, metric, and optional cart
results for uninterrupted-versus-recovered comparison.

Verification scripts do not make unavailable services “verified.” They become
live evidence only when run successfully against a real bounded environment
and the evidence is retained.
