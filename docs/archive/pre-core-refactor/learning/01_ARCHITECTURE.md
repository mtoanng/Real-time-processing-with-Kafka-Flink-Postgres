# Layer 1: Architecture

> Archived learning material; see `docs/ARCHITECTURE.md`.

## Source and Event Grain

The source is Alibaba Tianchi’s raw `UserBehavior.csv`. One input row is one
user behavior event with `user_id`, `item_id`, `category_id`, `behavior_type`,
and Unix `timestamp`. The allowed behavior values are `pv`, `cart`, `fav`, and
`buy`. The raw dataset is not committed; the committed fixture is bounded and
deterministic.

The platform keeps event grain through Kafka and the ClickHouse raw table.
Aggregates are a separate one-minute item view and do not replace the raw
event history.

## Replay, Kafka, and Schema Registry

`producer/src/taobao_replay/reader.py` reads headerless CSV or the canonical
header in bounded batches without reordering rows. `contracts.py` validates
the five-column contract, converts seconds to milliseconds, and creates a
deterministic SHA-256 `event_id` from the row values and source sequence.
`replay.py` emits accepted events in source order and can pace them with a
speed factor. Invalid rows can be written separately.

`producer/src/taobao_replay/kafka.py` publishes Avro values to the default
`user-behavior-events` topic. The Kafka key is the string form of `user_id`.
Kafka chooses the partition from that key; therefore events for the same user
are routed consistently to one partition while the partition assignment and
count remain stable. The producer requests `enable.idempotence=true` and
`acks=all`.

`schemas/user-behavior-event.avsc` is the value contract. The Python producer
uses Confluent’s Avro serializer and disables automatic schema registration.
`scripts/register_schemas.sh` registers the value subject and checks backward
compatibility. The registry can use local plaintext or managed authenticated
configuration.

## Flink DataStream Topology

The entrypoint is `com.taobao.behavior.TaobaoStreamJob`. There is one Java
Flink job:

```text
KafkaSource<UserBehaviorEvent>
  -> EventValidator
  -> event-time timestamps and watermarks
  -> LateEventRouter
  -> ClickHouse raw sink
  -> keyed one-minute item metrics -> ClickHouse metrics sink
  -> keyed ActiveCartProjector -> mandatory Cassandra sink
  -> full-profile keyed broadcast-rule processor -> ClickHouse alert sink
```

The source starts at `earliest`. `ConfluentRegistryAvroDeserializationSchema`
creates generated specific Avro `UserBehaviorEvent` objects. Invalid events go
to an `OutputTag`; valid events continue. Late events are routed to another
side output and are not sent to the on-time analytical windows or active-cart
projection.

## Event Time and Watermarks

`ImmediateBoundedOutOfOrdernessGenerator` tracks the maximum event timestamp
seen and emits `maximumTimestamp - 5 seconds - 1 millisecond` on every event.
The timestamp assigner uses `event_time_ms`, and idleness is 30 seconds. The
five-second bound is a processing assumption, not a guarantee about the raw
dataset. The one-minute window uses event time, zero allowed lateness, and
side-output late data.

## Keyed State and Timers

The active-cart branch is keyed by `user_id`. `ActiveCartProjector` uses a
Flink `MapState<Long, CartItemState>` where the map key is `item_id` inside
the user key. `cart` emits an upsert, `buy` emits a delete, and `pv`/`fav`
emit no Cassandra mutation. Event time plus source sequence rejects stale
events. A buy retains an inactive state marker so a later stale cart cannot
recreate the row. This projector does not use timers.

Timers exist only in the `full` rule branch. `CartAbandonmentRuleProcessor`
registers an event-time timer for a cart-abandonment threshold and emits an
alert if the cart remains due. The current rule processor’s pending-cart map
is keyed by the Flink user key and stores item-level events.

## Analytical Serving: ClickHouse

`infra/clickhouse/schema.sql` creates:

- `raw_behavior_events`: one row per accepted event;
- `item_metrics_1m`: item and behavior counts per event-time minute;
- `invalid_behavior_events`, `late_behavior_events`, and `behavior_alerts`:
  durable diagnostic records.

The physical tables use stable keys and `ReplacingMergeTree(record_version)`.
Committed `*_deduplicated` views are the business-query contract under
at-least-once delivery.

The Java raw sink maps Avro fields through `ClickHouseRowMapper`. The metrics
branch uses `ItemMetricsAggregator` and `ItemMetricsWindowFunction`. ClickHouse
owns raw behavioral history, item-level metrics, behavioral aggregates, and
analytical SQL. Cassandra is not an analytical store.

## Operational Serving: Apache Cassandra

`infra/cassandra/schema.cql` creates only `user_active_cart` after the keyspace
has been provisioned:

```sql
PRIMARY KEY ((user_id), item_id)
```

The only supported query is all active items for one `user_id`. The Java sink
uses CQL through the Apache Cassandra Java Driver. Local mode uses contact
points and datacenter; Astra mode loads the Secure Connect Bundle and token.
Both modes prepare the same INSERT and DELETE statements, reuse one session per
sink instance, enforce timeouts, and close deterministically.

The current code does not use the Astra Data API and has no active ScyllaDB
serving path. Cassandra is mandatory for `core` and `full`.

## Optional PostgreSQL/Debezium Control Plane

`infra/postgres/schema.sql` defines `behavior_rules`, not event history.
Debezium reads that table, unwraps changes, and routes them to the compacted
`behavior-rules` topic. The record key is `rule_id`, so compaction retains the
latest rule per ID. Flink connects the broadcast rule stream to keyed behavior
events using `KeyedBroadcastProcessFunction`.

The broadcast side stores versioned rules in `BroadcastState`. Every task
receives the rule update, while the keyed event side reads a read-only view of
that state. `RuleVersionPolicy` prevents older rule versions replacing newer
ones. This control plane is optional and does not own analytical history or
active-cart rows.

## Grafana

Grafana is an optional visualization profile. Its provisioned ClickHouse
datasource and Taobao dashboard query `raw_behavior_events` and
`item_metrics_1m`. Grafana is not required to process events, write ClickHouse,
write Cassandra, or run the CDC branch.

## Runtime Profiles

| Profile | What it enables | Dependencies | Status |
| --- | --- | --- | --- |
| `checks` | Tests, lint, package, Compose and Terraform validation | None | Credential-independent checks |
| `core` | Replay, Kafka/registry, one Flink job, ClickHouse, Cassandra | Kafka, Schema Registry, ClickHouse, local Cassandra or Astra | Code/static verified; live execution not claimed |
| `full` | Core plus PostgreSQL, Debezium, Broadcast State, timers, alerts, Grafana | Core plus PostgreSQL/Kafka Connect/Grafana | Code/static present; live full runtime not verified |

The local Compose file supplies Kafka, Schema Registry, ClickHouse, and one
Cassandra node for core; full adds PostgreSQL, Debezium, and Grafana. Flink is
installed and submitted separately. Managed mode starts no local dependencies.

## Deployment Architecture

The Terraform root defines a disposable AWS network and EC2 host, optional
Confluent resources, and an isolated optional Astra module. Bootstrap installs
Java 11, Docker, Python, and a pinned Flink distribution, then can install a
runtime bundle and shaded application JAR. Runtime profiles and credentials
are supplied later through an operator-owned environment file.

The Astra module is disabled by default. Terraform does not download the SCB,
does not store tokens, and is not proof of deployment. The repository’s current
status records Terraform formatting as verified but provider initialization/
validation and all cloud operations as not verified.

## Failure Boundaries

- CSV parse failure: row is rejected and can be written to invalid output;
  valid rows continue.
- Kafka publish failure: delivery errors surface on producer close; replay is
  not silently considered successful.
- Schema mismatch: registry compatibility or deserialization fails the
  affected contract/path rather than producing an untyped event.
- Flink validation/late data: side outputs preserve rejected or late records;
  they are not included in on-time projections.
- ClickHouse sink failure: analytical output can lag or retry according to the
  connector; live ClickHouse recovery is not verified here.
- Cassandra failure: core/full fail because active-cart serving is mandatory;
  connection/retry/restart evidence is not verified.
- Checkpoint failure/restart: checkpoints default on in core/full and use
  at-least-once mode; exactly-once external delivery is not claimed.
- CDC failure: rules and alerts are optional; event history and active-cart
  code paths do not become a general configuration store.
- Terraform/teardown failure: guarded scripts require explicit operator
  confirmation; no apply or destroy was executed for this repository.
