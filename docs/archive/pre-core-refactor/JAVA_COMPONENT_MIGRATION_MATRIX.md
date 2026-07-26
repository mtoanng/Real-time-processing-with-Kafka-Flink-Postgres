# Java Component Migration Matrix

> Archived pre-core-refactor audit; not an active implementation contract.

Status: historical feasibility audit only. No Java-to-PyFlink migration is
active; the current blueprint and Java runtime remain authoritative.

Audit date: 2026-07-23

All paths are exact repository-relative paths. `NOT PROVEN` means that no SQL
or PyFlink replacement exists in this repository and no runtime parity evidence
was produced. The classifications are recommendations, not deletion approval.

## Orchestration and configuration

### 1. Job topology and entrypoint

- Current exact path: `flink-jobs/taobao-stream-job/src/main/java/com/taobao/behavior/TaobaoStreamJob.java`
- Current Java class: `com.taobao.behavior.TaobaoStreamJob`
- Classification: **KEEP TEMPORARILY UNTIL PARITY IS PROVEN**
- Proposed API: one PyFlink entrypoint containing a `StreamTableEnvironment`,
  SQL/Table branches, and PyFlink DataStream branches.
- Reason: the class composes every source, side output, stateful operator, sink,
  UID, profile, and checkpoint setting; it is the safest whole-job rollback.
- Feature-parity status: **NOT PROVEN**; current topology is locally compiled
  and unit-tested, but live execution is already `NOT VERIFIED`.
- Connector/dependency impact: replace the 43.7 MB all-in-one job with a Python
  package plus a slim JVM connector adapter; retain Flink 1.20.2.
- Tests protecting behavior: all Java tests, especially
  `RuntimeProfileConfigTest`, `WatermarkAndLateDataTest`,
  `ItemMetricsAggregationTest`, and `ActiveCartProjectorTest`.
- Migration risk: very high; operator graph, UIDs, serializers, side outputs,
  and state ownership all change.
- Rollback path: submit the current shaded JAR with `scripts/run_flink.sh` from
  a compatible Java checkpoint or perform the bounded replay from a fresh job.

### 2. Runtime profile configuration

- Current exact path: `flink-jobs/taobao-stream-job/src/main/java/com/taobao/behavior/RuntimeProfileConfig.java`
- Current Java class: `com.taobao.behavior.RuntimeProfileConfig`
- Classification: **REMOVE AFTER PARITY**
- Proposed API: Python immutable configuration object plus Table connector
  options and PyFlink environment configuration.
- Reason: boolean/profile validation and Kafka property construction do not
  require JVM runtime code.
- Feature-parity status: **NOT PROVEN**.
- Connector/dependency impact: SQL DDL must forward Kafka `properties.*` and
  Confluent Avro registry authentication; Astra configuration remains owned by
  the JVM adapter while it exists.
- Tests protecting behavior: `RuntimeProfileConfigTest`.
- Migration risk: high; optional profiles must remain startup-optional, and
  secrets must not be interpolated into logs or committed SQL.
- Rollback path: keep the Java class and existing environment contract intact.

### 3. Checkpoint policy

- Current exact path: `flink-jobs/taobao-stream-job/src/main/java/com/taobao/behavior/processing/CheckpointPolicy.java`
- Current Java class: `com.taobao.behavior.processing.CheckpointPolicy`
- Classification: **REMOVE AFTER PARITY**
- Proposed API: PyFlink `StreamExecutionEnvironment` checkpoint configuration.
- Reason: AT_LEAST_ONCE mode, interval validation, and checkpoint storage are
  available to PyFlink.
- Feature-parity status: **NOT PROVEN**; policy shape is portable, state format is not.
- Connector/dependency impact: Python worker state and SQL planner state must be
  included in the same job checkpoint; sink guarantees remain unchanged.
- Tests protecting behavior: `CheckpointPolicyTest` and the unexecuted
  `docs/evidence/phase-5/CHECKPOINT_EXPERIMENT.md` procedure.
- Migration risk: very high for savepoint compatibility and medium for fresh starts.
- Rollback path: retain Java checkpoint configuration and use a Java-compatible
  checkpoint; otherwise restart either implementation from Kafka offsets under
  a documented fresh-state procedure.

## Source, validation, event time, and late data

### 4. Event validation

- Current exact path: `flink-jobs/taobao-stream-job/src/main/java/com/taobao/behavior/processing/EventValidator.java`
- Current Java class: `com.taobao.behavior.processing.EventValidator`
- Classification: **MIGRATE TO FLINK SQL**
- Proposed API: SQL `valid_events` and `invalid_events` views using predicates
  and a deterministic `CASE` reason.
- Reason: all current checks are row-local relational predicates.
- Feature-parity status: **DESIGN FEASIBLE, NOT PROVEN**; Avro decode failures
  remain outside row-level SQL validation.
- Connector/dependency impact: requires Kafka plus `avro-confluent` Table factories.
- Tests protecting behavior: `EventValidationTest`, `AvroContractTest`, and
  Python producer contract tests.
- Migration risk: medium; null/blank semantics and reason precedence must match.
- Rollback path: retain `EventValidator` and its `invalid-events` side output.

### 5. Per-event watermark generator

- Current exact path: `flink-jobs/taobao-stream-job/src/main/java/com/taobao/behavior/processing/ImmediateBoundedOutOfOrdernessGenerator.java`
- Current Java class: `com.taobao.behavior.processing.ImmediateBoundedOutOfOrdernessGenerator`
- Classification: **BLOCKED BY PYFLINK FEATURE GAP**
- Proposed API: preferably a minimal JVM `WatermarkStrategy` adapter invoked by
  the hybrid job; use SQL/built-in periodic watermarks only after rebaselining.
- Reason: public PyFlink 1.20 exposes built-in watermark strategies but not an
  equivalent Python custom generator that emits on every event.
- Feature-parity status: **BLOCKED FOR EXACT PARITY**.
- Connector/dependency impact: no connector impact; a tiny Flink-1.20.2 JVM
  adapter may be required.
- Tests protecting behavior: `WatermarkAndLateDataTest`.
- Migration risk: critical; periodic emission can change the fixture's late row.
- Rollback path: keep this Java generator and its current `- 5_000 - 1` rule.

### 6. Late-event router

- Current exact path: `flink-jobs/taobao-stream-job/src/main/java/com/taobao/behavior/processing/LateEventRouter.java`
- Current Java class: `com.taobao.behavior.processing.LateEventRouter`
- Classification: **KEEP TEMPORARILY UNTIL PARITY IS PROVEN**
- Proposed API: SQL `CURRENT_WATERMARK(event_time)` on-time/late views, or a
  PyFlink process function if side-output behavior is retained.
- Reason: the predicate is relational, but its result depends on the exact
  watermark emission behavior.
- Feature-parity status: **NOT PROVEN**.
- Connector/dependency impact: none beyond the selected watermark implementation.
- Tests protecting behavior: `WatermarkAndLateDataTest`.
- Migration risk: high; the current boundary is `event_time <= watermark` and
  raw history includes late events.
- Rollback path: keep the Java process function and `late-events` `OutputTag`.

### 7. Invalid-event carrier

- Current exact path: `flink-jobs/taobao-stream-job/src/main/java/com/taobao/behavior/model/InvalidBehaviorEvent.java`
- Current Java class: `com.taobao.behavior.model.InvalidBehaviorEvent`
- Classification: **REMOVE AFTER PARITY**
- Proposed API: append-only SQL row with `event_id`, `source_sequence`, and `reason`.
- Reason: the class is only a serializable carrier for a print side output.
- Feature-parity status: **NOT PROVEN**.
- Connector/dependency impact: none.
- Tests protecting behavior: `EventValidationTest` indirectly; no print-sink assertion exists.
- Migration risk: low for shape, medium for reason precedence.
- Rollback path: retain this POJO with `EventValidator`.

## One-minute aggregation

### 8. Aggregation accumulator

- Current exact path: `flink-jobs/taobao-stream-job/src/main/java/com/taobao/behavior/aggregation/ItemMetricsAccumulator.java`
- Current Java class: `com.taobao.behavior.aggregation.ItemMetricsAccumulator`
- Classification: **MIGRATE TO FLINK SQL**
- Proposed API: SQL conditional counts, exact `COUNT(DISTINCT user_id)`, and a
  category-consistency aggregate.
- Reason: the state is a conventional grouped window aggregate.
- Feature-parity status: **NOT PROVEN**.
- Connector/dependency impact: Table runtime state replaces Java `HashSet` state.
- Tests protecting behavior: `ItemMetricsAggregationTest`.
- Migration risk: high for state size/serialization and category inconsistency.
- Rollback path: keep the Java accumulator and window operator.

### 9. Incremental aggregator

- Current exact path: `flink-jobs/taobao-stream-job/src/main/java/com/taobao/behavior/aggregation/ItemMetricsAggregator.java`
- Current Java class: `com.taobao.behavior.aggregation.ItemMetricsAggregator`
- Classification: **MIGRATE TO FLINK SQL**
- Proposed API: `TUMBLE` TVF aggregation with `FILTER` counts and distinct users.
- Reason: standard relational window aggregation is clearer in SQL.
- Feature-parity status: **NOT PROVEN**; SQL must not group by category and hide
  the Java failure when an item changes category.
- Connector/dependency impact: requires Flink Table planner/runtime already
  present with PyFlink 1.20.2.
- Tests protecting behavior: `ItemMetricsAggregationTest`.
- Migration risk: high for category policy; medium for distinct-state footprint.
- Rollback path: retain `ItemMetricsAggregator`.

### 10. Window result function

- Current exact path: `flink-jobs/taobao-stream-job/src/main/java/com/taobao/behavior/aggregation/ItemMetricsWindowFunction.java`
- Current Java class: `com.taobao.behavior.aggregation.ItemMetricsWindowFunction`
- Classification: **MIGRATE TO FLINK SQL**
- Proposed API: select `window_start` and grouped keys from the `TUMBLE` TVF.
- Reason: its only custom behavior is adding window metadata and key fields.
- Feature-parity status: **NOT PROVEN**.
- Connector/dependency impact: none beyond Table runtime.
- Tests protecting behavior: `ItemMetricsAggregationTest`.
- Migration risk: medium; time-zone/type mapping must preserve UTC millisecond values.
- Rollback path: retain the process-window function.

### 11. Composite item/run key

- Current exact path: `flink-jobs/taobao-stream-job/src/main/java/com/taobao/behavior/model/ItemRunKey.java`
- Current Java class: `com.taobao.behavior.model.ItemRunKey`
- Classification: **REMOVE AFTER PARITY**
- Proposed API: SQL grouping columns `replay_run_id` and `item_id`.
- Reason: SQL grouping removes the need for a Java key carrier.
- Feature-parity status: **NOT PROVEN**.
- Connector/dependency impact: none.
- Tests protecting behavior: `ItemMetricsAggregationTest` indirectly; no direct equality test.
- Migration risk: low.
- Rollback path: retain this serializable key.

### 12. Item metrics carrier

- Current exact path: `flink-jobs/taobao-stream-job/src/main/java/com/taobao/behavior/model/ItemMetrics1m.java`
- Current Java class: `com.taobao.behavior.model.ItemMetrics1m`
- Classification: **REMOVE AFTER PARITY**
- Proposed API: insert-only Table row converted with `to_data_stream` for the
  retained ClickHouse adapter.
- Reason: the Table schema fully describes the carrier.
- Feature-parity status: **NOT PROVEN**.
- Connector/dependency impact: the Java sink adapter may temporarily require a
  Row-to-connector converter rather than this POJO.
- Tests protecting behavior: `ItemMetricsAggregationTest`,
  `ClickHouseMappingTest`, and `ClickHouseDdlContractTest`.
- Migration risk: medium for `TIMESTAMP_LTZ(3)` and unsigned ClickHouse mappings.
- Rollback path: retain `ItemMetrics1m` and current mapper.

## Active-cart business state

### 13. Active-cart projector

- Current exact path: `flink-jobs/taobao-stream-job/src/main/java/com/taobao/behavior/processing/ActiveCartProjector.java`
- Current Java class: `com.taobao.behavior.processing.ActiveCartProjector`
- Classification: **MIGRATE TO PYFLINK DATASTREAM**
- Proposed API: keyed PyFlink process function with `MapState[item_id, CartItemState]`.
- Reason: custom lifecycle, ordering, and tombstone semantics fit DataStream state.
- Feature-parity status: **API FEASIBLE, NOT PROVEN**.
- Connector/dependency impact: emits connector-neutral mutation rows to the
  retained Cassandra JVM adapter.
- Tests protecting behavior: `ActiveCartProjectorTest`.
- Migration risk: high for state serializer/savepoint compatibility and exact
  duplicate/stale tie-breaking.
- Rollback path: retain Java keyed state and restore only from Java-compatible state.

### 14. Active-cart item carrier

- Current exact path: `flink-jobs/taobao-stream-job/src/main/java/com/taobao/behavior/model/ActiveCartItem.java`
- Current Java class: `com.taobao.behavior.model.ActiveCartItem`
- Classification: **REMOVE AFTER PARITY**
- Proposed API: typed PyFlink `Row` or dataclass in the mutation stream.
- Reason: no behavior beyond five fields.
- Feature-parity status: **NOT PROVEN**.
- Connector/dependency impact: JVM adapter boundary needs an explicit Row schema.
- Tests protecting behavior: `ActiveCartProjectorTest`, `CassandraActiveCartSinkTest`.
- Migration risk: medium for cross-language timestamp representation.
- Rollback path: keep the Java POJO.

### 15. Per-item lifecycle state

- Current exact path: `flink-jobs/taobao-stream-job/src/main/java/com/taobao/behavior/model/CartItemState.java`
- Current Java class: `com.taobao.behavior.model.CartItemState`
- Classification: **MIGRATE TO PYFLINK DATASTREAM**
- Proposed API: explicit typed PyFlink state record.
- Reason: it is the persisted business state for stale-event protection.
- Feature-parity status: **API FEASIBLE, NOT PROVEN**.
- Connector/dependency impact: no external connector impact; state serializer changes.
- Tests protecting behavior: `ActiveCartProjectorTest`.
- Migration risk: critical for savepoint compatibility; high for accidentally
  dropping inactive tombstones.
- Rollback path: do not consume/overwrite the Java savepoint; retain Java state.

### 16. Lifecycle transition carrier

- Current exact path: `flink-jobs/taobao-stream-job/src/main/java/com/taobao/behavior/model/CartLifecycleTransition.java`
- Current Java class: `com.taobao.behavior.model.CartLifecycleTransition`
- Classification: **REMOVE AFTER PARITY**
- Proposed API: Python return tuple `(next_state, optional_mutation)`.
- Reason: local helper carrier only.
- Feature-parity status: **NOT PROVEN**.
- Connector/dependency impact: none.
- Tests protecting behavior: `ActiveCartProjectorTest`.
- Migration risk: low.
- Rollback path: retain the helper with the Java projector.

### 17. Cassandra mutation carrier

- Current exact path: `flink-jobs/taobao-stream-job/src/main/java/com/taobao/behavior/model/CartMutation.java`
- Current Java class: `com.taobao.behavior.model.CartMutation`
- Classification: **KEEP AS MINIMAL JAVA ADAPTER**
- Proposed API: stable adapter-boundary Row with explicit
  `UPSERT_CART_ITEM`/`DELETE_CART_ITEM` operation and item fields.
- Reason: Cassandra requires both inserts and deletes; an explicit command
  stream avoids pretending the append-only adapter input is a Table append sink.
- Feature-parity status: **PARTIAL**; Java command behavior exists, cross-language schema does not.
- Connector/dependency impact: retained Java driver adapter.
- Tests protecting behavior: `ActiveCartProjectorTest`, `CassandraActiveCartSinkTest`.
- Migration risk: medium; operation codes must not be lost during conversion.
- Rollback path: keep the current Java class and sink input.

## CDC rules, broadcast state, and timers

### 18. Broadcast rule/timer processor

- Current exact path: `flink-jobs/taobao-stream-job/src/main/java/com/taobao/behavior/processing/CartAbandonmentRuleProcessor.java`
- Current Java class: `com.taobao.behavior.processing.CartAbandonmentRuleProcessor`
- Classification: **MIGRATE TO PYFLINK DATASTREAM**
- Proposed API: PyFlink `KeyedBroadcastProcessFunction` with broadcast
  `MapState`, keyed pending-cart `MapState`, and `on_timer`.
- Reason: PyFlink 1.20 exposes all APIs currently used.
- Feature-parity status: **API FEASIBLE, NOT PROVEN**.
- Connector/dependency impact: rule Kafka/Avro decoding can move to Table API;
  no Java-only `applyToKeyedState` dependency exists.
- Tests protecting behavior: only `RuleVersionPolicyTest` and
  `AvroContractTest`; there is no operator-harness test for broadcast state or timers.
- Migration risk: critical due to missing end-to-end timer tests and threshold-update ambiguity.
- Rollback path: retain the Java processor and its state descriptor/UID.

### 19. Rule version policy

- Current exact path: `flink-jobs/taobao-stream-job/src/main/java/com/taobao/behavior/processing/RuleVersionPolicy.java`
- Current Java class: `com.taobao.behavior.processing.RuleVersionPolicy`
- Classification: **MIGRATE TO PYFLINK DATASTREAM**
- Proposed API: pure Python rule predicate helpers called by the broadcast operator.
- Reason: small deterministic business rules with no JVM dependency.
- Feature-parity status: **NOT PROVEN**.
- Connector/dependency impact: none.
- Tests protecting behavior: `RuleVersionPolicyTest`.
- Migration risk: medium; null/type behavior and millisecond multiplication must match.
- Rollback path: retain the Java static policy.

### 20. Behavior alert carrier

- Current exact path: `flink-jobs/taobao-stream-job/src/main/java/com/taobao/behavior/model/BehaviorAlert.java`
- Current Java class: `com.taobao.behavior.model.BehaviorAlert`
- Classification: **REMOVE AFTER PARITY**
- Proposed API: typed PyFlink Row/dataclass; append-only output.
- Reason: serializable output carrier only.
- Feature-parity status: **NOT PROVEN**.
- Connector/dependency impact: none; current output is a print sink.
- Tests protecting behavior: `RuleVersionPolicyTest` covers due arithmetic only;
  no alert shape/output assertion exists.
- Migration risk: medium due to weak existing tests.
- Rollback path: retain the Java carrier and print output.

## ClickHouse adapters

### 21. ClickHouse sink factory

- Current exact path: `flink-jobs/taobao-stream-job/src/main/java/com/taobao/behavior/sink/ClickHouseSinkFactory.java`
- Current Java class: `com.taobao.behavior.sink.ClickHouseSinkFactory`
- Classification: **KEEP AS MINIMAL JAVA ADAPTER**
- Proposed API: slim Row-based JVM adapter accepting insert-only streams from
  `to_data_stream`.
- Reason: the current ClickHouse 0.2.0 artifact has no Table factory and provides
  its async sink only as a JVM `Sink`.
- Feature-parity status: **CURRENT CODE EXISTS; LIVE CONNECTOR NOT VERIFIED**.
- Connector/dependency impact: keep
  `flink-connector-clickhouse-1.17:0.2.0:all`; avoid duplicate cluster copies.
- Tests protecting behavior: `ClickHouseMappingTest`, `ClickHouseDdlContractTest`.
- Migration risk: high for classloading, batching, retries, and the connector's
  Flink-1.17 artifact line on a 1.20.2 runtime.
- Rollback path: use the current factory unchanged in the Java job.

### 22. ClickHouse row mapper

- Current exact path: `flink-jobs/taobao-stream-job/src/main/java/com/taobao/behavior/sink/ClickHouseRowMapper.java`
- Current Java class: `com.taobao.behavior.sink.ClickHouseRowMapper`
- Classification: **KEEP AS MINIMAL JAVA ADAPTER**
- Proposed API: Row-to-ClickHouse binding code inside the slim adapter.
- Reason: it defines the exact UTC and column mapping at the connector boundary.
- Feature-parity status: **CURRENT STATIC MAPPING VERIFIED; LIVE WRITE NOT VERIFIED**.
- Connector/dependency impact: ClickHouse connector and Java time types remain JVM-side.
- Tests protecting behavior: `ClickHouseMappingTest`, `ClickHouseDdlContractTest`.
- Migration risk: medium for Table timestamp and `Row` conversion.
- Rollback path: retain current POJO mapping.

## Cassandra/Astra adapters and lookup

### 23. Cassandra configuration

- Current exact path: `flink-jobs/taobao-stream-job/src/main/java/com/taobao/behavior/sink/CassandraConfig.java`
- Current Java class: `com.taobao.behavior.sink.CassandraConfig`
- Classification: **KEEP AS MINIMAL JAVA ADAPTER**
- Proposed API: adapter-owned immutable configuration built from the existing
  environment contract.
- Reason: it validates Astra provider, SCB path, token, and safe CQL identifiers.
- Feature-parity status: **STATICALLY VERIFIED; ASTRA NOT VERIFIED**.
- Connector/dependency impact: Apache Cassandra Java Driver 4.19.2.
- Tests protecting behavior: `CassandraConfigTest`, `RuntimeProfileConfigTest`.
- Migration risk: medium; Python/JVM process boundaries must receive the same secret file.
- Rollback path: retain the current configuration class.

### 24. Cassandra session factory

- Current exact path: `flink-jobs/taobao-stream-job/src/main/java/com/taobao/behavior/sink/CassandraSessionFactory.java`
- Current Java class: `com.taobao.behavior.sink.CassandraSessionFactory`
- Classification: **KEEP AS MINIMAL JAVA ADAPTER**
- Proposed API: unchanged Java Driver session lifecycle behind the sink adapter.
- Reason: direct Astra SCB/token/keyspace support is already implemented; the
  PyFlink Cassandra wrapper does not provide a verified Flink-1.20/Astra equivalent.
- Feature-parity status: **ASTRA NOT VERIFIED**.
- Connector/dependency impact: Java Driver 4.19.2 plus Netty/native dependencies.
- Tests protecting behavior: mocked only through `CassandraActiveCartSinkTest`;
  no real session test exists.
- Migration risk: high for remote connectivity and shaded class conflicts.
- Rollback path: keep the current factory and shaded dependency closure.

### 25. Cassandra active-cart sink

- Current exact path: `flink-jobs/taobao-stream-job/src/main/java/com/taobao/behavior/sink/CassandraActiveCartSink.java`
- Current Java class: `com.taobao.behavior.sink.CassandraActiveCartSink`
- Classification: **KEEP AS MINIMAL JAVA ADAPTER**
- Proposed API: Row/command-aware JVM sink invoked from the PyFlink mutation stream.
- Reason: it owns prepared upsert/delete CQL, one session per subtask, and
  deterministic close; replacing it with Python is not clean parity.
- Feature-parity status: **STATIC/MOCK PARITY ONLY; ASTRA NOT VERIFIED**.
- Connector/dependency impact: Java Driver 4.19.2; no Flink Cassandra connector.
- Tests protecting behavior: `CassandraActiveCartSinkTest`,
  `CassandraCqlContractTest`, `CassandraConfigTest`.
- Migration risk: high; synchronous per-record writes, at-least-once duplicates,
  blocking/backpressure, and real failure behavior remain unverified.
- Rollback path: keep the Java sink and current mutation stream.

### 26. Active-cart lookup CLI

- Current exact path: `flink-jobs/taobao-stream-job/src/main/java/com/taobao/behavior/ActiveCartLookupCli.java`
- Current Java class: `com.taobao.behavior.ActiveCartLookupCli`
- Classification: **REMOVE AFTER PARITY**
- Proposed API: standalone Python CLI using the Apache Cassandra Python driver,
  SCB/token authentication, and the same fixed `WHERE user_id = ?` query.
- Reason: lookup is not a Flink operator and the Python driver supports Astra.
- Feature-parity status: **DESIGN FEASIBLE, NOT PROVEN AGAINST ASTRA**.
- Connector/dependency impact: adds a pinned Cassandra Python driver only to the
  lookup/runtime tools, not necessarily to PyFlink workers.
- Tests protecting behavior: `ActiveCartLookupCliTest`, `CassandraCqlContractTest`.
- Migration risk: medium for driver type/timestamp rendering and secret handling.
- Rollback path: retain `scripts/lookup_active_cart.sh` and the shaded Java CLI.

## Generated Avro classes

`com.taobao.behavior.avro.UserBehaviorEvent`, `BehaviorType`, and
`BehaviorRule` are generated from `schemas/user-behavior-event.avsc` and
`schemas/behavior-rule.avsc` into Maven's generated-sources directory. They are
not hand-written active Java files.

- Classification: **REMOVE AFTER PARITY** from the Python business path, but
  keep Avro schemas and Java generation if the minimal adapters need typed input.
- Proposed API: Table schema/`avro-confluent` decoding and explicit PyFlink Row types.
- Feature-parity status: **NOT PROVEN**.
- Connector/dependency impact: retain `flink-avro-confluent-registry` and
  `flink-avro`; the standalone Avro Java dependency may shrink with the adapter.
- Tests protecting behavior: `AvroContractTest` and producer schema tests.
- Migration risk: high for enum, logical timestamp, record-name alias, and
  Debezium writer/reader compatibility.
- Rollback path: keep canonical `.avsc` files and Maven generation unchanged.

## Java test utilities and parity disposition

### Shared test factory

- Current exact path: `flink-jobs/taobao-stream-job/src/test/java/com/taobao/behavior/EventTestSupport.java`
- Current Java class: `com.taobao.behavior.EventTestSupport`
- Classification: **REMOVE AFTER PARITY**
- Proposed API: one Python fixture factory reading the same canonical schemas.
- Reason: Java-only test boilerplate.
- Feature-parity status: **NOT PROVEN**.
- Connector/dependency impact: none.
- Tests protecting behavior: consumed by validation, watermark, aggregation,
  cart, mapping, and Avro tests.
- Migration risk: medium if new test events diverge from canonical Avro values.
- Rollback path: keep all Java tests runnable until the Java runtime is removed.

### Existing Java test suite

The following exact test classes must remain until equivalent SQL/PyFlink tests
exist and pass:

| Exact path/class | Behavior protected | Disposition |
| --- | --- | --- |
| `flink-jobs/taobao-stream-job/src/test/java/com/taobao/behavior/AvroContractTest.java` / `AvroContractTest` | Event/rule round trips and Debezium reader/writer compatibility | Keep permanently as a JVM adapter/schema contract or reproduce byte-level tests in Python before removal. |
| `flink-jobs/taobao-stream-job/src/test/java/com/taobao/behavior/EventValidationTest.java` / `EventValidationTest` | Semantic validation | Replace with SQL result assertions; then remove after parity. |
| `flink-jobs/taobao-stream-job/src/test/java/com/taobao/behavior/WatermarkAndLateDataTest.java` / `WatermarkAndLateDataTest` | Immediate watermark and late predicate | Keep as the decisive watermark adapter test. |
| `flink-jobs/taobao-stream-job/src/test/java/com/taobao/behavior/ItemMetricsAggregationTest.java` / `ItemMetricsAggregationTest` | Counts and exact distinct users | Replace with bounded Table API test; then remove after parity. |
| `flink-jobs/taobao-stream-job/src/test/java/com/taobao/behavior/ActiveCartProjectorTest.java` / `ActiveCartProjectorTest` | Cart/buy/idempotency/stale ordering | Port case-for-case to PyFlink state tests. |
| `flink-jobs/taobao-stream-job/src/test/java/com/taobao/behavior/RuleVersionPolicyTest.java` / `RuleVersionPolicyTest` | Rule versions and due threshold | Port and add broadcast/timer harness cases. |
| `flink-jobs/taobao-stream-job/src/test/java/com/taobao/behavior/CheckpointPolicyTest.java` / `CheckpointPolicyTest` | Opt-in checkpoint validation | Port to Python configuration tests. |
| `flink-jobs/taobao-stream-job/src/test/java/com/taobao/behavior/RuntimeProfileConfigTest.java` / `RuntimeProfileConfigTest` | Profile isolation and Kafka security | Port before changing submission packaging. |
| `flink-jobs/taobao-stream-job/src/test/java/com/taobao/behavior/ClickHouseDdlContractTest.java` / `ClickHouseDdlContractTest` | DDL/sink shape | Keep with JVM adapter or replace with language-neutral contract test. |
| `flink-jobs/taobao-stream-job/src/test/java/com/taobao/behavior/sink/ClickHouseMappingTest.java` / `ClickHouseMappingTest` | UTC/column mapping | Keep with JVM adapter. |
| `flink-jobs/taobao-stream-job/src/test/java/com/taobao/behavior/sink/CassandraConfigTest.java` / `CassandraConfigTest` | Astra config and secret-safe errors | Keep with JVM adapter. |
| `flink-jobs/taobao-stream-job/src/test/java/com/taobao/behavior/sink/CassandraCqlContractTest.java` / `CassandraCqlContractTest` | Partition and prepared CQL | Keep with JVM adapter. |
| `flink-jobs/taobao-stream-job/src/test/java/com/taobao/behavior/sink/CassandraActiveCartSinkTest.java` / `CassandraActiveCartSinkTest` | Bindings/session close | Keep with JVM adapter. |
| `flink-jobs/taobao-stream-job/src/test/java/com/taobao/behavior/ActiveCartLookupCliTest.java` / `ActiveCartLookupCliTest` | Fixed lookup interface | Replace with Python CLI test before removal. |

The current suite does not protect the composed topology, actual side outputs,
keyed/broadcast operator state, timer firing, SQL changelog mode, connector
runtime, savepoint compatibility, or real remote services. Those gaps are
migration risks, not evidence of parity.

## Deployment packaging component

- Current exact paths: `pom.xml`, `flink-jobs/taobao-stream-job/pom.xml`,
  `scripts/run_flink.sh`, and `infra/terraform/cloud_user_data.sh`
- Current Java class: `com.taobao.behavior.TaobaoStreamJob` (manifest main class)
- Classification: **KEEP TEMPORARILY UNTIL PARITY IS PROVEN**
- Proposed API: PyFlink job package plus a slim adapter JAR, submitted as one job.
- Reason: deployment currently expects one versioned shaded JAR and Java main class.
- Feature-parity status: **NOT PROVEN**; bootstrap installs Flink 1.20.1 while
  the build pins 1.20.2.
- Connector/dependency impact: add exact PyFlink 1.20.2 and Python dependency
  distribution; split connector dependencies carefully to avoid duplicate JARs.
- Tests protecting behavior: Maven package, shaded-JAR inspection, script syntax,
  Compose/Terraform checks in Phase B reports; no PyFlink submission test.
- Migration risk: critical for version skew, Python worker provisioning,
  classloading, and artifact checksums.
- Rollback path: preserve the current shaded JAR URL/path, manifest entrypoint,
  and `run_flink.sh` until the hybrid artifact passes a remote bounded run.
