# SQL / PyFlink Migration Risks

> Archived pre-core-refactor audit; not an active implementation contract.

Status: historical feasibility audit only. The current release strategy keeps
the single Java DataStream job.

Audit date: 2026-07-23

No migration was implemented. All runtime, connector, cloud, recovery, and
performance outcomes below remain `NOT VERIFIED` unless the repository already
contains direct evidence to the contrary.

## Risk scale

- Critical: can change business results, lose state, or prevent rollback.
- High: likely to block deployment or connector parity.
- Medium: bounded but requires an explicit test or decision.
- Low: mechanical with existing contract coverage.

## Risk register

| ID | Severity | Risk and evidence | Required mitigation / proof | Rollback |
| --- | --- | --- | --- | --- |
| R1 | Critical | **Blueprint conflict.** `docs/PROJECT1_BLUEPRINT_FINAL.md` requires one Java DataStream job and says not to replace the Java core with PyFlink. The audit request proposes a different long-term API policy. | Approve and record a blueprint change before implementation. Keep one hybrid job; do not create a permanent second topology. | Current blueprint and Java job remain authoritative. |
| R2 | Critical | **Flink patch-version mismatch already exists.** Maven pins 1.20.2, but Terraform bootstrap defaults to and downloads Flink 1.20.1. | Before migration testing, prove one exact 1.20.2 client/cluster/PyFlink runtime. This audit does not change the version or Terraform. | Submit the current JAR only to its verified compatible runtime. |
| R3 | Critical | **Immediate watermark parity gap.** Java emits a watermark on every event at `max - 5000 - 1`; SQL and built-in PyFlink watermarks emit periodically. | Run the exact `+30s` then `+15s` fixture order, equality-boundary cases, multi-partition cases, and idleness cases. Retain a minimal JVM watermark adapter if exact behavior is required. | Keep `ImmediateBoundedOutOfOrdernessGenerator` and `LateEventRouter`. |
| R4 | High | **Late routing can change raw/rollup reconciliation.** Current raw history receives all valid rows, including late rows; rollups and stateful branches receive only on-time rows. | Assert branch counts independently. Do not place the raw sink after the late filter. | Revert to current branch order in `TaobaoStreamJob`. |
| R5 | High | **SQL can hide category inconsistency.** Java fails if one item changes category in a minute. Grouping SQL by category silently produces multiple rows instead. | Add a category-change fixture and choose one explicit policy: fail, reject, or quarantine. Group primarily by item/run/window and assert category cardinality. | Keep `ItemMetricsAggregator` until policy and tests pass. |
| R6 | High | **Avro decoding errors are not semantic invalid rows.** SQL filters run only after a record is successfully deserialized. | Test incompatible writer schema, unknown enum, null required fields, registry outage, and poison messages. Define failure vs dead-letter policy without fabricating successful routing. | Keep current deserializer behavior and registration gate. |
| R7 | High | **Managed Schema Registry authentication is not wired into the current Java source.** Scripts/templates carry auth variables, but `TaobaoStreamJob` passes only the URL. | The SQL source must explicitly use `avro-confluent` authentication options and prove Confluent Cloud access. Treat this as a pre-existing defect, not proven parity. | Use the current local/plain registry path; do not claim managed compatibility. |
| R8 | Critical | **Java-to-Python/SQL state is not savepoint-compatible by default.** Java POJO/Avro serializers, SQL planner state, and PyFlink state formats differ. | Choose and document fresh-state cutover or implement a separately tested state migration. Verify operator UIDs and serializer snapshots. | Preserve Java savepoints and the current JAR; never overwrite the only rollback state. |
| R9 | Critical | **Active-cart stale protection is stateful and order-sensitive.** A newer buy leaves an inactive tombstone; equal timestamps use `source_sequence`. | Port all existing cases plus cross-checkpoint, reordered, repeated, and per-user/item isolation cases. | Revert to `ActiveCartProjector` and Java state. |
| R10 | Medium | **Inactive cart tombstones may grow without bound.** Java keyed state retains one state entry per encountered user/item after buys. | Preserve current behavior for parity. Any TTL/compaction is a later state-semantics change with its own phase. | Restore the current no-TTL state behavior. |
| R11 | Critical | **Broadcast/timer behavior lacks an operator-harness test.** Existing tests cover helper predicates, not Broadcast State, timer registration, timer restore, or output. | Add bounded PyFlink tests and a remote checkpoint/recovery case before replacing Java. | Keep `CartAbandonmentRuleProcessor`. |
| R12 | High | **Rule threshold updates have ambiguous current behavior.** If a threshold increases after a timer is registered, the old timer can fire, find the cart not due, leave it pending, and register no new timer. | Decide whether migration must reproduce this behavior or intentionally fix it; add cases for enable, disable, threshold increase/decrease, stale version, buy, and restart. | Use the Java behavior as the current oracle until the decision is recorded. |
| R13 | Medium | **`applyToKeyedState` is not used now, but future designs may need it.** PyFlink 1.20 does not support `apply_to_keyed_state`. | Keep broadcast updates deterministic and lazy as today. Reassess JVM code if a rule update must eagerly traverse every key. | Retain or reintroduce a minimal Java keyed-broadcast operator. |
| R14 | High | **No safe ClickHouse Table sink exists in the current dependency set.** Local JAR inspection found no ClickHouse `DynamicTableSinkFactory`. | Retain a slim Java adapter and feed only insert-only rows via `to_data_stream`. Do not assume generic JDBC is supported or equivalent. | Use the current `ClickHouseSinkFactory`. |
| R15 | High | **ClickHouse artifact/runtime boundary is unproven.** The artifact is named for Flink 1.17, version 0.2.0, used on Flink 1.20.2, and live runtime remains `NOT VERIFIED`. | Run a real bounded write, retry, checkpoint, and restart test before and after packaging changes. | Preserve current shaded artifact and dependency selection. |
| R16 | Medium | **ClickHouse flush configuration is misleading.** `createSink` accepts `maxTimeInBufferMs` but always calls `setMaxTimeInBufferMS(1_000L)`. | Record and test the actual one-second behavior; do not claim the metrics sink uses ten seconds. A fix would require a separate code phase. | Preserve the current hard-coded behavior for parity. |
| R17 | Critical | **Cassandra/Astra cannot be declared cleanly as a supported Flink 1.20 SQL sink.** Current integration is a custom Java Driver 4.19.2 sink. PyFlink's historical Cassandra wrapper has no Maven connector release for Flink 1.20 and is not the current Astra adapter. | Keep the Java adapter. Only replace it after real SCB/token, upsert/delete, retry, restart, and lookup evidence with the selected Python driver or connector. | Use `CassandraActiveCartSink` and Java lookup CLI. |
| R18 | High | **Cassandra sink is synchronous and at-least-once.** Each mutation blocks on `session.execute`; no transactional or checkpoint-aware sink is implemented. | Measure backpressure and failure windows remotely. Preserve idempotent row mutations and never claim exactly-once. | Revert to current sink; replay bounded input with reconciliation. |
| R19 | High | **Cross-language adapter boundary is undefined.** Current sinks accept generated Avro/POJO classes, not PyFlink `Row`. | Define stable Row schemas and explicit op codes, then test type, null, unsigned integer, and timestamp conversions. | Keep the Java job feeding its existing typed sinks. |
| R20 | High | **Table changelog misuse can corrupt append-only sinks.** A non-window aggregation or `upsert-kafka` source can produce updates/deletes. | Inspect planner changelog mode. Use `to_data_stream` only for insert-only tables; use `to_changelog_stream` with explicit `RowKind` handling otherwise. | Revert the query or use current DataStream branch. |
| R21 | Medium | **Rules topic semantics can change if switched to `upsert-kafka`.** Current Java consumes every append event and applies explicit version ordering. | First migration must use ordinary Kafka append mode. Treat `upsert-kafka` as a separate semantic change with delete/tombstone tests. | Keep the current Kafka value-only rule source. |
| R22 | High | **Python worker packaging is absent.** Bootstrap installs Python but not PyFlink, job dependencies, Python archives, or worker environment configuration. | Produce a reproducible wheel/requirements lock and verify it on every TaskManager. Do not modify Terraform in this audit. | Continue using the shaded Java JAR submission. |
| R23 | High | **Connector classloading may regress when the shaded JAR is split.** The current build deliberately selects ClickHouse-bundled Guava and documents overlaps. | Choose one placement per JAR (`lib`, `pipeline.jars`, or adapter bundle), inspect service files, and run classloading smoke tests. | Restore the current shaded JAR. |
| R24 | Medium | **Kafka key parity can change.** Current Flink source ignores the Kafka key; SQL may expose and validate it. | Preserve the value contract first. If key/value equality is desired, make it a separately approved validation rule with tests. | Omit the key from processing as the Java source does. |
| R25 | Medium | **Starting offsets and consumer identity can change.** Current source specifies earliest reset behavior and fixed group defaults. | Pin SQL connector `scan.startup.mode`, group ID, and security properties; test committed-offset restart. | Reuse current Java source configuration. |
| R26 | Medium | **Observability parity is weak.** Current business outputs are print sinks; no explicit Java business counters exist. | Reproduce invalid/late/alert observability and connector metrics before adding new claims or dashboards. | Keep current operator names/UIDs and print outputs. |
| R27 | High | **Tests are mostly unit/static contracts.** There is no composed Java topology test, PyFlink test, live connector proof, timer harness, or final E2E evidence. | Build a deterministic parity suite and store bounded evidence under the existing evidence rules before deletion. | Treat current Java tests as the minimum oracle and do not delete them. |
| R28 | Critical | **Rollback can be defeated by writing both implementations into the same tables/run IDs.** Duplicate raw/metric/Cassandra mutations would make comparison ambiguous. | Compare sequentially or use isolated run IDs and explicitly isolated test tables/keyspaces on a disposable host. Never run two active production topologies against the same outputs. | Stop the candidate, clear only its isolated test outputs under an approved procedure, and resume Java. |
| R29 | High | **Current deployment itself is not release-verified.** Reports mark Kafka/Flink/ClickHouse/Astra/CDC/recovery/teardown `NOT VERIFIED`. | Migration parity must not be described as production parity; first establish bounded evidence for both baseline and candidate. | Retain the codebase-ready boundary and existing prohibited claims. |

## Stop conditions

Stop the migration and retain Java if any of these occur:

- the deterministic late-event result changes without an approved policy change;
- the Table planner reports a changelog for a path connected to an append-only adapter;
- Java state cannot be retained for rollback;
- the candidate cannot run on exact Flink/PyFlink 1.20.2;
- ClickHouse or Cassandra adapter classloading differs across TaskManagers;
- the bounded reconciliation does not balance source, accepted, rejected, late,
  raw, rollup, cart mutations, and alerts; or
- a required behavior has only synthetic reasoning and no executable parity test.

## Claims that remain prohibited

This audit does not establish deployment, production readiness, zero Java,
exactly-once delivery, savepoint compatibility, Astra compatibility,
ClickHouse connector compatibility, performance, latency, throughput, or cloud
cost. No source component should be removed merely because a SQL or Python
sketch can be written.
