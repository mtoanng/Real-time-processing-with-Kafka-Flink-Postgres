# Proposed SQL / PyFlink Target Topology

> Archived pre-core-refactor audit; not an active implementation contract.

Status: historical, unapproved proposal. It is not the deployment topology;
the active repository uses the single Java job defined by the blueprint.

Audit date: 2026-07-23

## Status and constraints

This is a design proposal only. It does not change the current source code,
Flink version, Terraform, deployment architecture, or runnable Java job. The
authoritative blueprint currently requires a Java DataStream core, so the
topology below cannot become an implementation phase until that conflict is
explicitly resolved in the blueprint.

The target remains **one Flink job**. It does not add a second permanent
pipeline, a new database, or a new infrastructure technology. It deliberately
retains a small JVM adapter surface where the Flink 1.20.2 connector/runtime
boundary is not safely available from SQL or PyFlink. Zero Java is not a goal
or a promise.

## Logical topology

```text
Alibaba Taobao UserBehavior.csv
        |
        v
Python raw-row preparation and bounded replay (unchanged owner)
        |
        v
Kafka + Confluent Schema Registry
        |
        v
one hybrid Flink 1.20.2 job
  |
  +-- SQL/Table: Kafka + avro-confluent source
  |       |
  |       +-- SQL validation --------------------> invalid append stream
  |       |
  |       +-- valid append table
  |              |
  |              +-- to_data_stream ------------> minimal JVM ClickHouse
  |              |                                 raw-history adapter
  |              |
  |              +-- exact watermark/late gate
  |                     |                           retain JVM behavior until
  |                     +-- late append stream      periodic SQL parity is proven
  |                     |
  |                     +-- on-time append stream
  |                            |
  |                            +-- SQL TUMBLE TVF aggregation
  |                            |       |
  |                            |       +-- to_data_stream
  |                            |              |
  |                            |              v
  |                            |       minimal JVM ClickHouse metrics adapter
  |                            |
  |                            +-- PyFlink keyed active-cart state
  |                            |       |
  |                            |       +-- append mutation commands
  |                            |              |
  |                            |              v
  |                            |       minimal JVM Astra/Cassandra adapter
  |                            |
  |                            +-- keyBy(user_id) ---------------------+
  |                                                                      |
  +-- SQL/Table: Debezium rule Kafka + avro-confluent source              |
          |                                                               |
          +-- to_data_stream --> PyFlink rule-version policy --> broadcast+
                                                                          |
                                                                          v
                              PyFlink KeyedBroadcastProcessFunction
                              + keyed pending-cart MapState
                              + event-time timers
                              + append alert stream

Standalone active-cart lookup:
  existing Java CLI remains until a Python Cassandra-driver CLI proves
  Astra SCB/token, query, rendering, and error-handling parity.
```

The raw ClickHouse branch deliberately occurs before late classification.
That preserves the current contract: valid late events are analytical history,
but they do not affect rollups, active-cart serving, or abandonment alerts.

## API ownership and boundaries

| Boundary | Producer API and mode | Conversion | Consumer API | Parity rule |
| --- | --- | --- | --- | --- |
| Kafka behavior source to validation | SQL/Table, insert-only | None | SQL views | Keep value schema and earliest-start behavior; Kafka key/value equality is not a current validation rule. |
| Valid rows to raw sink | SQL/Table, insert-only | `to_data_stream` | Minimal Java ClickHouse adapter | Branch before the late filter and retain all valid events. |
| Valid rows to late gate | SQL/Table, insert-only | `to_data_stream` if the exact JVM gate is retained | Minimal Java watermark/late adapter | Preserve immediate `maxTimestamp - 5000 - 1`, 30-second idleness, and `eventTime <= watermark`. |
| On-time rows to item aggregation | Table with rowtime | `from_data_stream` when the exact JVM late gate remains; otherwise no conversion | SQL `TUMBLE` TVF | Preserve event-time metadata and prove category consistency semantics. |
| Window results to metrics sink | SQL/Table, insert-only final windows | `to_data_stream` | Minimal Java ClickHouse adapter | Reject the boundary if planner output is updating. |
| On-time rows to active cart | SQL/Table or post-gate DataStream, insert-only | `to_data_stream` only when starting in Table | PyFlink keyed DataStream | Preserve event-time/source-sequence stale protection and inactive tombstones. |
| Active-cart mutations to Astra | PyFlink DataStream, append command records | No Table conversion | Minimal Java Cassandra/Astra adapter | Use explicit UPSERT/DELETE operation codes and stable Row types. |
| Rule Kafka rows to broadcast logic | SQL/Table, ordinary Kafka insert-only | `to_data_stream` | PyFlink DataStream and Broadcast State | Preserve every versioned Debezium event and current version policy. |
| Upsert rule table, if later chosen | SQL/Table changelog | `to_changelog_stream` | RowKind-aware DataStream | Not part of the first parity migration; changes source semantics. |
| Logical active-cart Table, if later chosen | SQL/Table changelog | `to_changelog_stream` | RowKind-aware sink | Not recommended while the explicit command stream is the clearer Cassandra contract. |
| Alert output | PyFlink append DataStream | No conversion | Existing diagnostic output initially | Do not silently invent a durable alert sink. |

`to_data_stream` is safe only for insert-only tables. Any query that produces
updates or deletes must use `to_changelog_stream`, preserve `RowKind`, and feed
a consumer that explicitly supports the entire changelog. A one-minute event-
time TVF aggregate is intended to emit final insert rows; an unwindowed running
aggregate is not an equivalent substitute.

## SQL/Table layer

### Behavior source and validation

Declare the Kafka value with `avro-confluent`, retain the canonical event names
and types, and expose the record timestamp as rowtime only after decoding. Keep
the current semantic checks as explicit SQL predicates and derive a stable
invalid reason with `CASE`. Records that cannot be decoded by Avro never reach
this relational validation boundary and require a separately tested failure
policy.

The source declaration must pin the existing Kafka connector release and
Schema Registry settings. Managed Confluent authentication is not proven by
the current Java source and must be tested rather than inferred from environment
templates.

### Late-event boundary

The preferred long-term relational shape is a rowtime declaration followed by
an on-time/late split using `CURRENT_WATERMARK`. It is not currently an exact
behavioral substitute. Flink SQL and built-in PyFlink watermark strategies emit
periodically; the Java generator emits immediately for each event. Until the
fixture, equality boundary, partition-idleness, and remote-runtime tests agree,
keep the Java generator/router as a narrow adapter and convert the resulting
on-time stream back to a Table with explicit rowtime metadata.

### One-minute metrics

Use a `TUMBLE` table-valued function and group by window, replay run, and item.
Compute the behavior counts and exact distinct users in SQL. The Java job treats
multiple categories for one item in a window as a fatal inconsistency. The SQL
query must verify category cardinality; grouping by category would silently
split the current group and is not parity.

## PyFlink DataStream layer

### Active-cart state

Key by `user_id` and use typed `MapState` indexed by `item_id`. Port the current
cart/buy transition function case-for-case, including source-sequence tie
breaking, duplicate suppression, retained `added_at`, and inactive tombstones.
Do not introduce state TTL during parity work.

### Rule broadcast and timers

Consume the append-only rule event stream, apply the existing per-rule version
policy, broadcast the accepted rule, and connect it to the keyed on-time
behavior stream with PyFlink `KeyedBroadcastProcessFunction`. Maintain keyed
pending-cart state, register event-time timers, and emit an alert only from the
timer path when the current broadcast rule says the cart is due.

The current Java class does **not** call `applyToKeyedState`; it uses keyed
state only for the current key plus timer callbacks. PyFlink 1.20 supports that
pattern. A future requirement to eagerly traverse every key on a broadcast
update would be blocked because PyFlink 1.20 does not expose
`apply_to_keyed_state`.

The existing threshold-increase behavior needs a product decision and an
operator-level test: an old timer can fire before the new due time, leave the
cart pending, and register no replacement timer. The migration must not
silently fix or canonize that ambiguity.

## Minimal JVM adapter layer

The adapter JAR should contain only boundaries that have no proven clean
SQL/PyFlink equivalent on the pinned runtime:

- the exact watermark/late gate, temporarily, if periodic watermark behavior
  fails parity;
- ClickHouse raw and one-minute metric sinks using the existing async connector
  and explicit Row-to-column conversion; and
- Cassandra/Astra mutation sink using Java Driver 4.19.2, SCB/token
  authentication, prepared INSERT/DELETE statements, and one managed session
  per subtask.

The ClickHouse artifact in the repository has no Table sink factory. The Astra
integration is a custom Java driver sink, not a Flink Cassandra connector.
These findings justify adapters; they do not prove that the current connectors
work remotely. Both remain `NOT VERIFIED` until credentialed bounded runs and
restart tests succeed.

## Packaging on the pinned Flink line

The candidate is one submitted hybrid job containing:

- a PyFlink job package and pinned Python dependencies, with
  `apache-flink==1.20.2` available consistently to client and workers;
- `flink-connector-kafka:3.3.0-1.20` and its selected dependency closure;
- `flink-avro-confluent-registry:1.20.2`, `flink-avro:1.20.2`, and the selected
  Confluent client dependencies;
- a slim JVM adapter JAR containing the ClickHouse connector line currently in
  use and the Apache Cassandra Java Driver 4.19.2 dependency closure; and
- one unambiguous classloading location per connector dependency.

Do not place duplicate connector bundles in both Flink `lib/` and the submitted
artifact. The existing deployment expects a shaded Java JAR and does not
install PyFlink or distribute a Python environment. The Terraform bootstrap
also defaults to Flink 1.20.1 while Maven pins 1.20.2. Both are migration
blockers to resolve in an authorized future phase; this audit changes neither.

## Runtime profiles

Preserve the existing profile boundaries while changing implementation APIs:

| Profile | Candidate branches | Required external services |
| --- | --- | --- |
| Core | behavior source, validation, late gate, raw history, one-minute metrics | Kafka, Schema Registry, ClickHouse |
| Serving | Core plus active-cart state and mutation adapter | Core plus Astra/Cassandra |
| CDC | Serving plus rule source, Broadcast State, timers, alerts | Serving plus externally managed PostgreSQL/Debezium and rules topic |

PostgreSQL and Debezium remain external control-plane producers. The candidate
must not introduce a direct Flink PostgreSQL CDC source without a separate
architecture decision.

## Checkpoint, savepoint, and rollback topology

Carry over the configured checkpoint interval, mode, timeout, minimum pause,
concurrency, and external checkpoint storage into the PyFlink entrypoint. That
configuration is portable; the state bytes are not. Java Avro/POJO state, SQL
planner state, and Python state serializers are not automatically savepoint-
compatible.

The safe first cutover model is therefore:

1. keep the current Java artifact and its checkpoints immutable;
2. run a bounded candidate with isolated replay-run IDs and isolated test
   outputs, never dual-write ambiguously into the same result set;
3. reconcile input, invalid, late, raw, metric, cart-mutation, and alert counts;
4. exercise checkpoint/restart and prove connector failure windows;
5. choose either a documented fresh-state cutover or a separately proven state
   transformation; and
6. remove a Java component only after its own parity gate passes.

Rollback stops the candidate and resumes the current Java JAR from its own
compatible checkpoint, or performs a documented fresh bounded replay. A
candidate SQL/PyFlink savepoint must not be assumed readable by the Java job.

## Proposed migration gates, not implementation phases

1. Approve the blueprint/API-strategy change while retaining the one-job
   architecture.
2. Freeze deterministic baseline outputs and add missing composed topology,
   timer, changelog, and connector contract tests.
3. Prove the SQL source, validation, raw branch, and TVF aggregation without
   removing Java.
4. Prove PyFlink active-cart state and keyed-broadcast timer behavior against
   the Java oracle.
5. Define and test stable cross-language Row/command schemas for the minimal
   adapters.
6. Package one exact Flink/PyFlink 1.20.2 job and run bounded remote parity,
   recovery, lookup, and teardown checks.
7. Remove Java components individually only after evidence supports the
   `REMOVE AFTER PARITY` classification.

No gate above was started by this audit. Cloud connector behavior, savepoint
compatibility, recovery, performance, and deployment all remain
`NOT VERIFIED`.
