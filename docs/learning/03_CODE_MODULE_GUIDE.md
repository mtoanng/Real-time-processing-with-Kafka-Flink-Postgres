# Layer 3: Code-Module Guide

This layer maps repository paths to the behavior they implement. Start from the
entrypoints, then follow the call chain into contracts, processing, models,
sinks, tests, and operations.

## 1. Repository layout

```text
producer/                         Python source preparation and replay
flink-jobs/taobao-stream-job/     One Java Flink job
schemas/                          Avro source contracts
infra/                            SQL, CQL, Compose, Grafana, Terraform, legacy CDC
scripts/                          Setup, submission, verification, recovery, teardown
tests/fixtures/                   Deterministic bounded CSV fixture
docs/                             Active contracts, operations, learning, evidence
```

Root build/control files:

| File | Role |
| --- | --- |
| `pyproject.toml` | Python package, optional Kafka dependencies, CLI entrypoint, Ruff rules |
| `pom.xml` | Java parent build and dependency/plugin versions |
| `Makefile` | Composite credential-independent checks and operator shortcuts |
| `.env.example` | Local profile variables without real secrets |
| `.env.cloud.example` | Managed demo variable template |
| `README.md` | Project status and shortest supported workflows |
| `CODEBASE_INDEX.md` | Directory-level navigation |
| `AGENTS.md` | Repository working, architecture, verification, and learning rules |

## 2. Python package call graph

```text
python -m taobao_replay / taobao-replay
  -> __main__.py
  -> cli.main
       -> profile_file
       -> audit_source_dataset
       -> replay_file
            -> iter_event_batches
                 -> iter_source_rows
                 -> parse_event
                      -> deterministic_event_id
            -> emit JSONL or KafkaEventPublisher.publish
```

### `__init__.py`

Exports the small public library surface: event model, batch iterator, replay
statistics, and replay function. It lets tests or scripts import stable package
names without reaching into every internal module.

### `__main__.py`

Calls `cli.main()` and exits with its result. This makes
`python -m taobao_replay` equivalent to the installed `taobao-replay` command.

### `cli.py`

`build_parser()` declares four subcommands and their arguments.

`_validate_input()` checks that the source exists.

`_validate_outputs()` prevents input overwrite, duplicate accepted/rejected
paths, and accidental replacement without `--force`.

`_open_output()` selects stdout, exclusive creation, or explicit replacement.

`main()` wires commands to pure modules, owns output streams with `ExitStack`,
prints machine-readable statistics, and converts expected operational errors
into CLI parser errors.

### `contracts.py`

`UserBehaviorEvent` is an immutable slotted dataclass matching the Avro fields.

`deterministic_event_id()` serializes six stable values with `\x1f` separators
and returns SHA-256 hex.

`parse_event()` enforces five columns, integer conversion, known behavior,
non-negative sequence, and nonblank replay ID. It converts source seconds to
milliseconds.

Important nuance: it does not reject all semantic problems. An integer such as
`user_id=0` remains Avro-encodable and is intentionally left for Flink
validation.

### `reader.py`

`iter_source_rows()` handles UTF-8 BOM, recognizes one canonical optional
header, assigns source sequence only to data rows, and yields without loading
the file.

`iter_event_batches()` validates positive batch size, accumulates at most one
batch, catches `RowValidationError`, and emits immutable `ReplayBatch` values.

`ParseIssue` retains source sequence, original field strings, and rejection
reason for producer-side accounting.

### `replay.py`

`replay_file()` is transport-agnostic because `emit` is injected. Its pacing
uses an injected monotonic clock and sleep function, which makes time behavior
unit-testable.

The delay formula is:

```text
target wall time =
  replay start + (event time - first event time) / speed
```

Backward event-time jumps never sleep a negative duration and input order is
never changed.

### `kafka.py`

`event_to_avro()` returns the dataclass dictionary.

`kafka_key()` returns `str(user_id)`.

`kafka_security_options()` validates and maps `PLAINTEXT` or `SASL_SSL`.

`_load_schema_registry_client()` delays optional imports so non-Kafka tests do
not require the Confluent package.

`build_schema_registry_producer()` loads the committed schema, configures the
registry serializer, disables automatic registration, and builds an idempotent
producer with `acks=all`.

`KafkaEventPublisher` owns topic publication, delivery-error collection, poll,
flush, and close-time failure reporting.

### `profile.py`

`sha256_file()` hashes the file in 1 MiB blocks.

`profile_file()` streams batches to compute counts, invalid reasons, event-time
range, behavior distribution, and backward time jumps. Its memory use depends
on batch size rather than full file size.

### `source_audit.py`

`load_manifest()` validates manifest shape and provenance metadata.

`_is_official_source_url()` accepts official Tianchi dataset ID 649 URL forms.

`_reject_header()` enforces the raw acquisition contract.

`audit_source_dataset()` combines filename, provenance, shape, hash, row-count,
and profile checks into a `SourceAudit(status="SATISFIED")`.

### `clickhouse.py`

`ClickHouseHttpClient.execute()` posts SQL over an explicit absolute HTTP(S)
endpoint using basic authentication, optionally appends `FORMAT JSON`, and
wraps HTTP/network failures with useful context.

`_url()` preserves existing endpoint query parameters and adds the database.

`split_sql_statements()` removes full-line SQL comments and splits the
repository’s simple bootstrap DDL on semicolons.

This client is used by operator scripts, not by the Java streaming sink.

## 3. Java application call graph

```text
TaobaoStreamJob.main
  -> RuntimeProfileConfig.fromEnvironment
  -> CheckpointPolicy.fromValues
  -> DeduplicationConfig.fromHours
  -> configure StreamExecutionEnvironment
  -> KafkaSource<UserBehaviorEvent>
  -> EventValidator
  -> keyBy(event_id) -> EventDeduplicator
  -> ClickHouse raw sink
  -> timestamps/watermarks -> LateEventRouter
  -> keyBy(ItemCategoryKey) -> one-minute aggregate -> ClickHouse metric sink
  -> union invalid/duplicate/late quality -> ClickHouse quality sink
  -> optional keyBy(user_id) -> ActiveCartProjector -> Cassandra sink
  -> deprecated rules source/broadcast/timers -> alert sink
```

### `TaobaoStreamJob.java`

This is the composition root. It should contain topology wiring rather than
detailed business algorithms.

It:

- reads profile-scoped configuration;
- refuses to submit the `checks` profile;
- configures parallelism, checkpoints, storage, retained checkpoints, and
  restart strategy;
- builds the Avro Kafka source;
- attaches stable names and UIDs to stateful/source/sink operators;
- wires core and optional branches;
- executes one named Flink job.

The raw sink attaches directly to `acceptedUnique`, before timestamp assignment
and late routing.

### `RuntimeProfileConfig.java`

Package-private configuration boundary for supported profile names, optional
branch flags, default values, Cassandra validation, Kafka security properties,
and Schema Registry properties.

`isCassandraEnabled()` is true only for `serving`.
`isCdcEnabled()` is true only for `cdc`.
`observability` does not add a Java branch.

### `CheckpointPolicy.java`

Parses five environment values and validates:

- boolean enable flag;
- interval of at least one second;
- required storage when enabled;
- zero to ten restart attempts;
- delay from zero to five minutes.

The value object itself does not configure Flink; `TaobaoStreamJob` applies it.

### `DeduplicationConfig.java`

Converts retention hours to `Duration`, with a range of 1 to 8,760 hours and a
168-hour default.

### `EventValidator.java`

`ProcessFunction<UserBehaviorEvent, UserBehaviorEvent>`.

`invalidReason()` is independently testable. `processElement()` converts an
invalid reason into a deterministic quality record on `INVALID_EVENTS`;
otherwise it collects the original event unchanged.

### `EventDeduplicator.java`

`KeyedProcessFunction<String, UserBehaviorEvent, UserBehaviorEvent>`.

`open()` creates State-TTL configuration and obtains keyed `ValueState`.

`processElement()` reads state for the current event-ID key. An unexpired value
routes a duplicate quality record. Otherwise it writes current processing time
and collects the event.

### `ImmediateBoundedOutOfOrdernessGenerator.java`

Tracks the maximum assigned event timestamp. `onEvent()` emits a watermark
immediately; `onPeriodicEmit()` intentionally does nothing.

The extra subtraction of one millisecond follows Flink’s watermark boundary:
timestamps at or below the watermark are complete/late.

### `LateEventRouter.java`

`ProcessFunction` with a `late-events` side output. `isLate()` avoids treating
events as late before the first real watermark.

## 4. Aggregation package

### `ItemCategoryKey`

Mutable no-argument JavaBean shape for Flink serialization, plus value
constructor, getters, equality, and hash code. It contains only item and source
category, never replay run.

### `ItemMetricsAccumulator`

Package-private mutable accumulator:

- four long counters;
- `Set<Long>` for distinct users;
- deterministic lineage candidate and its source sequence.

### `ItemMetricsAggregator`

Implements Flink `AggregateFunction`.

`add()` chooses lineage, increments exactly one behavior counter, and inserts
the user ID.

`merge()` combines counters and sets and applies the same lineage ordering.
Window operation may require merge semantics even though the current tumbling
assigner normally uses non-merging windows.

### `ItemMetricsWindowFunction`

Adds the key and window start to the accumulator result. `toMetrics()` is
static so unit tests can verify mapping without running a Flink cluster.

### `ItemMetrics1m`

Serializable sink model with JavaBean getters/setters. `replayRunId` is stored
lineage; the other fields define business results.

## 5. Core model package

### `StreamQualityEvent`

Serializable quality sink model and deterministic quality-ID factory. It
normalizes missing values for invalid observations while preserving a genuinely
missing event ID as null.

### `ItemCategoryKey` and `ItemMetrics1m`

These represent the metric key and sink row described above.

## 6. Optional cart model and processor

### `CartItemState`

Checkpointed per-user/per-item state: active flag, source category, original
active-period add time, latest update time, latest event time, and source
sequence.

### `ActiveCartItem`

External mutation payload: user, item, category, added time, and last updated
time.

### `CartMutation`

Pairs an `ActiveCartItem` with `UPSERT_CART_ITEM` or `DELETE_CART_ITEM`.

### `CartLifecycleTransition`

Returns both the next Flink state and an optional external mutation.

### `ActiveCartProjector`

`KeyedProcessFunction<Long, UserBehaviorEvent, CartMutation>`.

`open()` obtains `MapState<Long, CartItemState>` named
`user-active-cart-items`.

`transition()` is the pure business function:

- ignore `pv`/`fav`;
- ignore older `(event time, source sequence)`;
- make `cart` active and emit upsert;
- make `buy` inactive and emit delete;
- suppress an exact repeated active cart transition.

`processElement()` stores the next state and emits the optional mutation.

## 7. Cassandra sink package

### `CassandraConfig`

Validates `local` or `astra` mode, lowercase CQL identifiers, required
`user_active_cart` table name, timeouts, local contact points/datacenter, or
Astra bundle/token. Secret values are not included in failure messages.

### `CassandraSessionFactory`

Builds one DataStax Java Driver session. Both modes apply connection/request
timeouts and select the keyspace. Astra uses cloud bundle and token; local uses
contact points, datacenter, and optional credentials.

### `CassandraActiveCartSink`

Legacy Flink `RichSinkFunction` used only in `serving`.

`open()` creates a session and prepares insert/delete statements.
`invoke()` validates the mutation and synchronously executes the bound
statement.
`close()` closes the session and clears transient fields.

Because writes are synchronous, this is understandable and bounded but not a
throughput-optimized implementation.

### `ActiveCartLookupCli`

Parses exactly `--user-id <positive-id>`, prepares a fixed partition query, and
prints returned rows or `NOT FOUND`.

## 8. ClickHouse sink package

### `ClickHouseRowMapper`

Defines exact Java-to-DDL column maps for:

- raw behavior events;
- one-minute metrics;
- stream quality;
- deprecated behavior alerts.

All time columns expected by ClickHouse are converted to UTC
`ZonedDateTime`.

### `ClickHouseSinkFactory`

Creates typed `ClickHouseAsyncSink` instances and column bindings. The private
generic builder centralizes endpoint, authentication, database/table, format,
timeouts, buffering, in-flight request, and record-size settings.

The nested mapper classes bridge Flink connector bindings to
`ClickHouseRowMapper`.

## 9. Deprecated Java modules

### `BehaviorAlert`

Output model for old cart-abandonment alerts. It is not a
`StreamQualityEvent`.

### `RuleVersionPolicy`

Pure helpers for accepting only a greater rule version, recognizing an enabled
cart-abandonment rule, and deciding whether a timer is due.

### `CartAbandonmentRuleProcessor`

`KeyedBroadcastProcessFunction` connecting user-keyed on-time events with
broadcast `BehaviorRule` updates.

It stores:

- broadcast rules by rule ID;
- pending carts by item for the current user key;
- event-time timers at cart time plus threshold.

This code is deprecated. Understanding it helps read the repository, but
extending it would violate the current architecture.

## 10. Avro schema modules

| File | Meaning |
| --- | --- |
| `schemas/user-behavior-event.avsc` | Active behavior-event value contract |
| `schemas/behavior-rule.avsc` | Deprecated Flink rule reader contract |
| `schemas/behavior-rule-debezium-writer.avsc` | Deprecated writer compatibility contract |
| `src/test/resources/debezium/behavior-rule-writer.avsc` | Test resource for writer-reader resolution |

Schema evolution is tested through Avro serialization and reader/writer
resolution. A live registry compatibility check remains separate.

## 11. SQL and CQL modules

### `infra/clickhouse/schema.sql`

Creates the database, three core replacement tables and canonical views, plus
deprecated alert table/view. The script is additive and uses `IF NOT EXISTS`;
it deliberately does not migrate incompatible legacy tables.

### `infra/clickhouse/verify.sql`

Human-readable canonical counts, metrics, quality, duplicate lineage, and
physical-versus-canonical diagnostics.

### `infra/cassandra/local-keyspace.cql`

Creates a single-node development keyspace. It must not be used as the managed
replication design.

### `infra/cassandra/schema.cql`

Creates only `user_active_cart`. Application code does not create keyspaces.

### `infra/cassandra/verify.cql`

Fixed operator verification query for active cart.

### Deprecated SQL/configuration

`infra/postgres/schema.sql`, `infra/debezium/`, and `infra/kafka/` implement or
describe the old behavior-rule branch only.

## 12. Compose and Grafana modules

`infra/docker-compose.yml` declares named profiles and services. YAML anchors
share Flink environment settings. Named volumes retain ClickHouse, checkpoints,
Cassandra, and PostgreSQL data.

The Grafana provisioning directory contains:

- datasource configuration pointing to ClickHouse;
- dashboard provider configuration;
- the checked-in Taobao overview dashboard JSON.

Grafana does not modify the Flink job.

## 13. Terraform modules

| Path | Responsibility |
| --- | --- |
| `versions.tf` | Terraform/provider versions and provider configuration |
| `variables.tf` | AWS, artifact, runtime profile, Confluent, and Astra inputs |
| `network.tf` | Temporary VPC, subnet, route, SSH-only ingress, EIP |
| `ec2.tf` | Disposable host and user-data template |
| `cloud_user_data.sh` | Install Docker/Java/Python/Flink, fetch checked artifacts, write operator environment and start/stop wrappers |
| `confluent.tf` | Optional Kafka environment/cluster, identities, topics, ACLs, schemas; includes deprecated CDC resources |
| `main.tf` | Optional Astra module call |
| `modules/astra/` | Optional non-vector Astra database and initial keyspace |
| `outputs.tf` | Host, network, Kafka endpoint, and Astra identifiers |
| `terraform.tfvars.example` | Non-secret input example |

The bootstrap never embeds runtime credentials. Operators add them later to a
root-readable environment file. Optional artifact checksums can make downloads
content-addressed.

No Terraform apply or destroy is authorized by documentation work.

## 14. Operational script inventory

### Active core setup and execution

| Script | Responsibility |
| --- | --- |
| `apply_clickhouse_schema.py` | Apply SQL statements through ClickHouse HTTP |
| `register_schemas.sh` | Register and verify active event Avro schema |
| `run.sh` | Validate profile inputs and start selected local dependencies |
| `run_flink.sh` | Submit the one shaded Java JAR |
| `replay.sh` | Publish the committed fixture under a replay run ID |
| `runtime_healthcheck.sh` | Profile-aware bounded health checks |
| `stop.sh`, `runtime_stop.sh` | Stop services; volume deletion requires explicit option |

### Dataset and deterministic evidence

| Script | Responsibility |
| --- | --- |
| `generate_fixture.py` | Regenerate the exact 1,000-row committed fixture |
| `prepare_demo_subset.py` | Copy a deterministic bounded prefix of an audited raw source |
| `reconcile_final_e2e.py` | Independently compute expected accounting, metrics, digests, and cart |
| `verify_clickhouse.py` | Compare live canonical ClickHouse results with expected evidence |
| `verify_bounded_pipeline.sh` | Coordinate two-run canonical verification |
| `run_replay_identity_experiment.sh` | Publish the same fixture under two run IDs |

### Recovery

| Script | Responsibility |
| --- | --- |
| `canonical_snapshot.py` | Capture/compare stable raw, metric, and optional cart snapshots |
| `run_checkpoint_experiment.sh` | Print the controlled two-environment workflow |
| `run_flink_recovery_test.sh` | Wait for checkpoint, execute approved TaskManager failure, wait for recovery, compare |

### Optional serving

| Script | Responsibility |
| --- | --- |
| `apply_cassandra_schema.sh` | Apply local or Astra cart table schema |
| `lookup_active_cart.sh` | Invoke fixed Java lookup for one user |

### Cloud and release evidence

| Script | Responsibility |
| --- | --- |
| `cloud_preflight.sh` | Validate required remote inputs without creating resources |
| `run_cloud_smoke.sh` | Coordinate bounded cloud smoke operations |
| `collect_cloud_evidence.sh` | Store live evidence when services actually run |
| `healthcheck.sh` | Verify final cloud endpoints |
| `run_release_e2e.sh` | Guarded final release workflow |
| `verify_cloud_teardown.sh` | Check teardown state |
| `teardown_demo.sh` | Explicitly guarded Terraform destruction |

These scripts do not prove that cloud actions occurred. Evidence must record a
real successful run.

### Deprecated scripts

`apply_behavior_rules_schema.sh`, `create_behavior_rules_topic.sh`, and
`run_phase6_demo.sh` belong to the legacy CDC branch.

## 15. Test-module map

Python tests cover:

- source provenance, hash, shape, and header rules;
- bounded reader behavior and fixture reproducibility;
- stable event identity and semantic-invalid pass-through;
- replay ordering and pacing;
- Kafka key, security, delivery errors, and Avro wire round trip;
- ClickHouse HTTP helpers;
- profile isolation;
- independent reconciliation and canonical snapshot comparison.

Java tests cover:

- Avro specific-record and deprecated writer/reader compatibility;
- runtime profile isolation and managed security;
- checkpoint requirements, stable UIDs, and restart configuration;
- validation reason paths;
- State-TTL deduplication, expiry, and checkpoint restoration;
- watermark/late behavior;
- item/category metric aggregation and lineage independence;
- ClickHouse DDL, delivery, and column mapping;
- cart ordering/idempotence;
- Cassandra configuration, CQL, session lifecycle, and input validation;
- deprecated rule version policy.

Use [Tests and experiments](05_TESTS_AND_EXPERIMENTS.md) to distinguish unit,
contract, static, and live evidence.

