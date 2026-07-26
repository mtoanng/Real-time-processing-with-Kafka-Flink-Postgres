# SQL / PyFlink Migration Feasibility

> Archived pre-core-refactor audit; not an active implementation contract.

Status: historical feasibility audit only. The active blueprint retains one
Java DataStream job; the checks/core/full and mandatory-Cassandra contracts in
the current blueprint supersede profile/count examples in this document.

Audit date: 2026-07-23

## Scope and decision boundary

This is a non-destructive feasibility audit. No source, dependency, Flink
version, Terraform, deployment architecture, or active runtime path was
changed. The current Java job remains the rollback implementation.

The requested long-term strategy conflicts with the current blueprint clause
that locks the core to Java DataStream and forbids a PyFlink replacement. This
document is therefore a proposed migration direction, not an authorization to
start a migration phase. A blueprint decision is required before implementation.

## Verdict

A single hybrid Flink job using Flink SQL/Table API for relational work and
PyFlink DataStream for business state is feasible on the current Flink 1.20.2
code contract, but a zero-Java result is neither safe nor promised.

- Kafka/Confluent Avro declarations, semantic validation, projections, and the
  one-minute item aggregation are good SQL/Table candidates.
- Active-cart `MapState`, stale-event ordering, Broadcast State, rule versions,
  and event-time timers are available in PyFlink DataStream 1.20.
- The current broadcast processor does **not** use `applyToKeyedState`. PyFlink
  1.20 lacks `apply_to_keyed_state`, but that gap does not block parity for the
  code that exists today.
- The exact custom watermark generator is not directly expressible through the
  public PyFlink 1.20 watermark API. SQL and built-in PyFlink bounded
  out-of-orderness watermarks emit periodically, while the Java implementation
  emits on every event. The Java behavior must stay until the late-event test is
  deliberately rebaselined or a minimal JVM watermark adapter proves parity.
- The bundled ClickHouse connector is a Java DataStream `Sink` and has no
  `DynamicTableSinkFactory`. There is no safe direct SQL/Table sink in the
  repository today.
- Astra can be contacted with a Python Cassandra driver, but the current
  Flink/Astra sink cannot be moved cleanly to PyFlink without changing driver,
  lifecycle, packaging, and failure behavior. The current Java driver adapter
  is justified until a real Astra parity run succeeds.

Overall status: **FEASIBLE AS A STAGED HYBRID MIGRATION; FEATURE PARITY NOT
PROVEN**.

## Evidence inspected

The audit covered the authoritative blueprint; active and archived
implementation/completion reports; all 26 active Java production classes; all
Java tests and Avro test resources; both Maven POMs and the resolved runtime
dependency tree; the 43.7 MB shaded JAR and its service registrations;
ClickHouse/Cassandra/PostgreSQL schemas; Debezium configuration; Docker Compose;
Terraform/Astra modules; EC2 bootstrap; operational scripts; and current
runbooks.

Local JAR inspection established that:

- `META-INF/services/org.apache.flink.table.factories.Factory` contains Kafka,
  Upsert Kafka, Avro, and Confluent Avro factories.
- `flink-connector-clickhouse-1.17:0.2.0:all` contains
  `ClickHouseAsyncSink`, converters, and client classes, but no ClickHouse Table
  factory or Table connector service registration.
- Cassandra is not a Flink connector in the current build. It is the Apache
  Cassandra Java Driver 4.19.2 called by a custom `RichSinkFunction`.

## Component feasibility

### Kafka and Confluent Avro source

Classification: **MIGRATE TO FLINK SQL**.

The Kafka Table connector plus `avro-confluent` can infer the Avro reader schema
from a table declaration and fetch writer schemas from Schema Registry. The
current UTF-8 `user_id` Kafka key should be declared as a distinct prefixed key
column, while the Avro value retains its `BIGINT user_id`. This avoids silently
changing the producer contract.

Important parity boundaries:

- The Java source is value-only and never verifies the Kafka key against the
  value. Key/value equality would be a new validation rule, not parity.
- The Java source always requests earliest offsets as its reset position.
- The current Java deserializer receives only the registry URL. Managed
  registry authentication is present in scripts/environment templates but is
  not passed into `ConfluentRegistryAvroDeserializationSchema`; the SQL design
  must explicitly configure `avro-confluent.basic-auth.*` without claiming the
  existing managed path works.
- Wire-format/schema failures occur before semantic SQL validation and cannot be
  routed as ordinary invalid rows by a `WHERE` clause.

### Schema and semantic validation

Classification: **MIGRATE TO FLINK SQL** for semantic checks; retain schema
registration and compatibility tests outside the job.

Positive IDs, non-negative event/source times, nonblank identifiers, and the
accepted behavior domain are relational predicates. Build two append-only
views from the decoded source: `valid_events` and `invalid_events`, with a
deterministic `CASE` expression for the invalid reason.

SQL validation does not replace:

- producer-side raw-row validation;
- Schema Registry compatibility checks;
- the Debezium writer/reader Avro resolution test; or
- handling of records that fail Avro deserialization before becoming a row.

### Timestamps, watermarks, and late events

Classification: **BLOCKED BY PYFLINK FEATURE GAP** for exact watermark parity;
**KEEP TEMPORARILY UNTIL PARITY IS PROVEN** for the late router.

The Java generator emits `maximumTimestamp - 5_000 - 1` immediately on each
event, adds 30-second idleness, and treats `event_time <= currentWatermark` as
late. Flink SQL can declare a rowtime column and use `CURRENT_WATERMARK` to
split rows:

```sql
-- logical shape only
WHERE CURRENT_WATERMARK(event_time) IS NULL
   OR event_time > CURRENT_WATERMARK(event_time)
```

However, Table and built-in PyFlink watermark strategies periodically emit the
largest generated watermark. That can change whether two rapidly consumed
fixture events observe an intervening watermark. A SQL expression of exactly
five seconds also differs by one millisecond from the Java `- 1` convention.
The existing `WatermarkAndLateDataTest` is therefore a mandatory parity gate.

Raw ClickHouse history must continue to branch from `valid_events` before late
routing. Late events are valid raw history today; they are excluded only from
the rollup, Cassandra, and CDC/timer branches.

### Invalid- and late-event outputs

Classification: **MIGRATE TO FLINK SQL** for invalid classification;
**KEEP TEMPORARILY UNTIL PARITY IS PROVEN** for late classification.

Both outputs are append-only. They are currently print sinks, not durable dead
letter topics or database tables. A migration must not silently invent durable
storage or claim reconciliation evidence that the current job does not have.

### One-minute item aggregation

Classification: **MIGRATE TO FLINK SQL**.

Use an event-time `TUMBLE` table-valued function over the on-time view, grouped
by `window_start`, `window_end`, `replay_run_id`, and `item_id`. Compute the four
conditional counts and exact `COUNT(DISTINCT user_id)`.

Do not group by `category_id` without an explicit policy. The Java accumulator
fails the job if one item changes category inside a window. Grouping by category
would silently split one Java group into several SQL rows. A parity query must
carry `MIN(category_id)`, `MAX(category_id)`, or a distinct-category count and
prove the intended failure/routing behavior.

A TVF window aggregate emits one final row when the watermark closes the
window, so the intended result is insert-only. Replacing it with an unwindowed
continuous `GROUP BY` would create an updating changelog and is not equivalent.

### ClickHouse raw and aggregation sinks

Classification: **KEEP AS MINIMAL JAVA ADAPTER**.

Both current ClickHouse inputs are append-only, so a hybrid pipeline should use
`to_data_stream`, not `to_changelog_stream`, before calling a minimal JVM sink
adapter:

- `raw_behavior_events`: valid rows, including late rows;
- `item_metrics_1m`: final on-time window rows.

The current connector JAR has no SQL/Table factory. The generic Flink JDBC
connector does not list a ClickHouse dialect/driver among its supported
dialects, and replacing the current JSONEachRow async sink with JDBC would
change batching, retry, type conversion, metrics, and cloud behavior. It is not
a safe audit-only recommendation.

The current factory also accepts a `maxTimeInBufferMs` argument but hard-codes
`setMaxTimeInBufferMS(1_000L)`. Thus both sinks currently use one second even
though the metrics call passes ten seconds. Parity tests must use actual runtime
behavior, not the unused parameter.

### Active-cart state and stale-event protection

Classification: **MIGRATE TO PYFLINK DATASTREAM**.

PyFlink keyed `MapState` can preserve the `(user_id -> item_id -> lifecycle)`
transition logic, including event-time ordering and `source_sequence` tie
breaking. Preserve these details:

- only `cart` and `buy` mutate state;
- repeated identical cart/buy rows emit no duplicate mutation;
- a newer buy leaves an inactive keyed tombstone so an older cart cannot
  recreate the item;
- `added_at` is retained across an active-cart refresh;
- state is keyed by user and indexed by item.

The Java state can grow with inactive user/item tombstones. Introducing TTL
would be a behavior change and is outside this audit.

### Cassandra/Astra sink and lookup

Classification: **KEEP AS MINIMAL JAVA ADAPTER** for the Flink sink;
**REMOVE AFTER PARITY** is possible for the standalone lookup CLI.

PyFlink 1.20 exposes a wrapper for the historical Flink Cassandra connector,
but Maven Central has no Cassandra connector release targeted at Flink 1.20,
and that connector is not the current Java Driver 4.19.2/Astra implementation.
The current code requires a Secure Connect Bundle, token authentication, a
fixed keyspace/table contract, prepared INSERT/DELETE statements, and one
reused session per subtask.

The Apache Cassandra Python driver can connect to Astra with a Secure Connect
Bundle and token, so a standalone Python lookup is feasible. Using that driver
inside PyFlink workers is not a clean equivalent Flink sink: it changes driver
implementation, dependency distribution, connection lifecycle, blocking
behavior, metrics, and checkpoint/failure semantics. Keep the JVM sink until a
credentialed Astra test proves write, delete, retry, restart, and lookup parity.

### PostgreSQL/Debezium rule stream

Classification: **MIGRATE TO FLINK SQL** for Kafka/Avro decoding;
**MIGRATE TO PYFLINK DATASTREAM** for rule application.

The current Flink job does not run a Flink CDC PostgreSQL connector. Debezium
runs externally and publishes unwrapped Avro values to Kafka. Therefore the
Flink-side migration is another Kafka `avro-confluent` source, not a new
PostgreSQL source connector.

Using ordinary `connector='kafka'` preserves the current append-only sequence of
versioned rule events and allows `to_data_stream`. Modeling the topic as
`upsert-kafka` would create a current-rules changelog and require
`to_changelog_stream`; it would also change the current explicit version policy
and must not be done casually.

### Broadcast State and event-time timers

Classification: **MIGRATE TO PYFLINK DATASTREAM**.

The exact Java implementation uses:

- `KeyedBroadcastProcessFunction`;
- broadcast `MapState<String, BehaviorRule>`;
- keyed `MapState<Long, UserBehaviorEvent>` for pending carts;
- event-time timer registration in `processElement`; and
- `onTimer` reads of broadcast state.

It does **not** invoke `Context.applyToKeyedState` or any equivalent traversal of
all keyed state. PyFlink 1.20 supports every API used here, including
`on_timer`. It does not support `apply_to_keyed_state`, so a future rule-update
design that needs eager mutation of every keyed cart would reintroduce a JVM
requirement or require a different topology.

The current timer behavior has an ambiguity that must become a parity test: if
a threshold increases after a cart timer was registered, the old timer can fire
before the new threshold, retain the cart, and register no replacement timer.
Migration must not silently repair or preserve this without an explicit product
decision.

### Checkpoints, savepoints, and UIDs

Classification: **REMOVE AFTER PARITY** for `CheckpointPolicy`; configure the
same policy in the PyFlink entrypoint and deployment environment.

The current job enables AT_LEAST_ONCE checkpoints only when configured and sets
external checkpoint storage. This is portable. State/savepoint compatibility is
not automatic across Java serializers, SQL planner operators, and Python
pickled/typed state. Existing operator UIDs are useful rollback identifiers but
cannot make different state schemas compatible.

The first migration cutover must assume either a fresh state start or a proven,
documented state transformation. It must not claim savepoint compatibility or
exactly-once delivery without a real recovery exercise.

### Metrics

Classification: **MIGRATE TO PYFLINK DATASTREAM** for custom counters and keep
JVM connector metrics with the retained adapters.

The Java job defines no explicit business metric group. Invalid, late, and
alert records go to print sinks; connector/base runtime metrics are the main
available telemetry. A migration should first reproduce observable counts and
operator names, then add new metrics only as separately reviewed behavior.

### Test utilities and deployment packaging

Classification: **REMOVE AFTER PARITY** for Java-only test helpers and the Java
entrypoint; **KEEP AS MINIMAL JAVA ADAPTER** for a slim connector adapter JAR.

The target package needs all of the following without changing Flink 1.20.2:

- `apache-flink==1.20.2` in the remote Python environment;
- the PyFlink job package and pinned Python dependencies;
- Kafka and Confluent Avro JVM connector/format JARs;
- a slim Java adapter JAR for ClickHouse and Astra, including their selected
  dependency closures;
- matching Python worker configuration on every TaskManager; and
- one submission command for one hybrid job.

The Terraform bootstrap currently defaults to Flink 1.20.1 while Maven pins
1.20.2. This is a pre-existing deployment mismatch. It was not changed by this
audit and must be resolved before any migration runtime test.

## Append-only and changelog decisions

| Logical table/stream | Current/proposed mode | Required Table-to-DataStream conversion | Reason |
| --- | --- | --- | --- |
| Decoded behavior source | Append-only | `to_data_stream` | Kafka records arrive as inserts. |
| Valid behavior events | Append-only | `to_data_stream` | Filter/projection only. |
| Invalid behavior events | Append-only | `to_data_stream` | Each rejected decoded row is one insert. |
| Raw ClickHouse rows | Append-only | `to_data_stream` | One row per valid event; late rows included. |
| On-time events | Append-only | `to_data_stream` | Watermark filter/side-output classification only. |
| Late events | Append-only | `to_data_stream` | One diagnostic row per late event. |
| One-minute TVF item metrics | Append-only | `to_data_stream` | Final result only when the window closes. |
| Rule event source using ordinary Kafka | Append-only | `to_data_stream` | Preserves every versioned Debezium value event. |
| Current rules using `upsert-kafka` | Changelog | `to_changelog_stream` | Updates/deletes represent current rule state. Not recommended for first parity pass. |
| Active-cart mutation commands | Append-only DataStream | No Table conversion required | Explicit UPSERT/DELETE command records. |
| Logical `user_active_cart` table | Changelog/upsert | `to_changelog_stream` if modeled as a Table | Cart is insert/update; buy is delete. Current Cassandra adapter consumes commands instead. |
| Behavior alerts | Append-only | `to_data_stream` if produced as a Table | Each expired cart emits one alert. |
| Unwindowed item aggregation | Changelog | `to_changelog_stream` | Counts update continuously; this is not the current contract. |

`to_data_stream` must be used only when the planner confirms insert-only mode.
If an optimizer or query change introduces updates, conversion must fail rather
than discard `RowKind`; use `to_changelog_stream` and a sink that explicitly
handles update-before, update-after, and delete records.

## Connector and dependency inventory for Flink 1.20.2

### Existing Java artifact

| Purpose | Exact coordinate/version | Packaging finding |
| --- | --- | --- |
| Flink runtime APIs | `flink-core`, `flink-streaming-java` 1.20.2 | `provided`; cluster must supply compatible runtime. |
| Kafka | `flink-connector-kafka` 3.3.0-1.20 | Compiled into the shaded application with Kafka client dependencies. |
| Confluent Avro | `flink-avro-confluent-registry` 1.20.2 | Includes Table format factory; depends on `flink-avro` 1.20.2 and Confluent client 7.5.3. |
| ClickHouse | `flink-connector-clickhouse-1.17` 0.2.0, classifier `all` | DataStream async sink only; no Table factory. Built against a 1.17 connector line and runtime smoke remains `NOT VERIFIED`. |
| Cassandra/Astra | `java-driver-core` 4.19.2 | Plain Java driver, not a Flink connector or Table factory. |
| Avro generation/runtime | Apache Avro 1.11.3 | Generates `UserBehaviorEvent` and `BehaviorRule` Java records. |

The current shaded JAR is self-contained for these non-provided dependencies,
so separate runtime connector JARs are not presently copied into the repository.

### Proposed hybrid package

Use only artifacts compatible with the pinned Flink line:

- Kafka connector factory: `flink-connector-kafka:3.3.0-1.20` plus its resolved
  runtime dependencies, or an equivalent verified SQL-client bundle for that
  same connector release. Do not load both forms.
- Confluent Avro format: `flink-avro-confluent-registry:1.20.2` and
  `flink-avro:1.20.2`, with their resolved Confluent dependencies.
- ClickHouse: keep `flink-connector-clickhouse-1.17:0.2.0:all` only behind the
  retained adapter until a supported Table factory is selected and proven.
- Astra: keep `java-driver-core:4.19.2` and its dependency closure behind the
  retained adapter.

Do not place duplicate copies both in Flink `lib/` and the job/adapter bundle.
The existing shaded-JAR report already records vendor-bundle overlap and the
ClickHouse connector's selected Guava boundary.

## Required parity gates before any Java removal

1. Same 999 accepted / 1 invalid fixture reconciliation.
2. Same raw branch inclusion of late-but-valid events.
3. Same deterministic `+30s` then `+15s` late-event result and boundary tests.
4. Same item-500 one-minute metrics and category-change behavior.
5. Same active-cart cart/buy/idempotency/stale-order transitions.
6. Same rule version ordering, disable behavior, threshold-update behavior, and
   event-time alert timing.
7. Same ClickHouse column values and Astra CQL mutations.
8. Checkpoint/restart evidence with explicit duplicate windows.
9. One hybrid job submission on an actual Flink 1.20.2 distribution.
10. Rollback to the current shaded Java JAR from its own compatible checkpoint
    or from a documented fresh replay.

Until these gates pass, every migration claim remains `NOT VERIFIED`.

## Official version-specific references

- Apache Flink 1.20 DataStream/Table integration:
  https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/data_stream_api/
- Apache Flink 1.20 Broadcast State pattern:
  https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/fault-tolerance/broadcast_state/
- Apache Flink 1.20 Confluent Avro format:
  https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/table/formats/avro-confluent/
- Apache Flink 1.20 JDBC Table connector:
  https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/table/jdbc/
- DataStax Astra Python driver connection contract:
  https://docs.datastax.com/en/astra-db-serverless/databases/python-driver.html
