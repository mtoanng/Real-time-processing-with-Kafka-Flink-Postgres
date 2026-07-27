# Redis Serving Simplification Migration

Date: 2026-07-27

## 1. Phase implemented

Implemented exactly one phase: replace the optional Cassandra/Astra
active-cart serving path with Valkey/Redis while preserving the
Kafka -> Flink -> ClickHouse core.

Redis implementation and local contract status: **SATISFIED**.

`CODEBASE-READY`: **NOT VERIFIED** because Terraform providers were not
initialized for `terraform validate`.

Live Redis, connector recovery, cloud deployment, and performance:
**NOT VERIFIED**.

No cloud resource was planned, applied, created, or changed. No later product
CDC, scale-tuning, or cloud-deployment phase was started.

## 2. Implemented contract

- The `core` profile remains independent of Redis.
- The `serving` profile adds one Redis service and one branch in the existing
  Java Flink job.
- The branch stores one Hash per user:
  `taobao:active_cart:{user_id}`.
- Hash field is the decimal `item_id`.
- Hash value is
  `category_id|added_at_ms|last_updated_at_ms`.
- `cart` produces deterministic `HSET`; `buy` produces deterministic `HDEL`;
  `pv` and `fav` produce no serving mutation.
- The required TTL defaults to 604800 seconds, is constrained to
  60..31536000 seconds, and is refreshed after each mutation.
- Local and managed endpoints use the same host, port, TLS, ACL, password, and
  timeout configuration path.
- Sink writes are synchronous and at least once. Repeated logical mutations
  converge, but Redis does not participate in a Flink checkpoint transaction.
- Async batching, Redis Cluster/Sentinel policy, performance tuning, and
  managed Redis provisioning are deferred.

## 3. Files changed

Runtime and tests:

- `pom.xml`
- `flink-jobs/taobao-stream-job/pom.xml`
- `flink-jobs/taobao-stream-job/src/main/java/com/taobao/behavior/`
- `flink-jobs/taobao-stream-job/src/test/java/com/taobao/behavior/`
- `producer/tests/test_runtime_profiles.py`

Profiles, infrastructure, and scripts:

- `.env.example`
- `.env.cloud.example`
- `infra/docker-compose.yml`
- `infra/terraform/`
- `Makefile`
- `scripts/run.sh`
- `scripts/run_flink.sh`
- `scripts/run_fixture_demo.sh`
- `scripts/cloud_preflight.sh`
- `scripts/lookup_active_cart.sh`
- `scripts/verify_bounded_pipeline.sh`

Active documentation:

- `AGENTS.md`
- `README.md`
- `CODEBASE_INDEX.md`
- `docs/PROJECT1_BLUEPRINT_FINAL.md`
- `docs/ARCHITECTURE.md`
- `docs/SEMANTICS.md`
- `docs/RUNBOOK.md`
- `docs/OPERATIONS.md`
- all five documents and the index under `docs/learning/`
- active evidence READMEs and this report

The learning package now explains the implemented system at all three requested
levels: general architecture, component boundaries, and code modules. It also
contains execution traces, test-to-decision mapping, and controlled
experiments.

## 4. Cleanup Report

Removed:

- `CassandraActiveCartSink.java`
- `CassandraConfig.java`
- `CassandraSessionFactory.java`
- the three corresponding Cassandra Java tests
- `infra/cassandra/`
- `infra/terraform/modules/astra/`
- obsolete Astra-only `infra/terraform/main.tf`
- Astra provider, variables, outputs, examples, and lockfile entry
- `scripts/apply_cassandra_schema.sh`
- Cassandra driver dependency and active Cassandra/Astra environment variables

Archived:

- None in this phase.

Kept:

- Kafka, Schema Registry, the single Java Flink job, and ClickHouse core
- Python raw-dataset preparation and replay path
- deprecated PostgreSQL/Debezium `behavior_rules` compatibility artifacts
- historical evidence and archived documentation

Reason:

- The blueprint requires Redis only for optional bounded hot cart state.
- Cassandra/Astra duplicated that narrow serving responsibility and added
  schema, driver, credential, and infrastructure paths that no longer had a
  target role.
- Deprecated CDC remains isolated because replacing it with `product_catalog`
  CDC is a separately approved future phase, not this phase.
- Historical evidence remains evidence of earlier repository states and is not
  presented as current architecture.

## 5. Commands and results

| Command | Result |
| --- | --- |
| `mvn -B -pl flink-jobs/taobao-stream-job -am test` | PASS; 49 tests |
| `mvn -B clean test` | PASS from an empty target; 49 tests |
| `mvn -B clean package` | Final rerun PASS; 49 tests and shaded JAR built |
| `PYTHONPATH=producer/src python -m unittest discover -s producer/tests -v` | PASS; 43 tests |
| `ruff check producer scripts` | PASS |
| `ruff format --check producer scripts` | PASS; 29 files already formatted |
| four `docker compose ... --profile <profile> config --quiet` commands | PASS for core, serving, cdc, and observability; no containers started |
| Git Bash `bash -n scripts/*.sh infra/terraform/cloud_user_data.sh` | PASS |
| Maven dependency tree filtered to Jedis/Cassandra/DataStax | PASS; Jedis 7.1.0 present, Cassandra/DataStax absent |
| shaded-JAR entry scan | PASS; Redis/Jedis present, Cassandra/DataStax absent |
| active Markdown local-link check | PASS; 17 files, no missing local targets |
| `git diff --check` | PASS |
| Avro JSON parse | PASS |
| tracked secret/state/raw-dataset path check | PASS |
| `terraform -chdir=infra/terraform fmt -check -recursive` | PASS |
| `terraform -chdir=infra/terraform validate` | NOT VERIFIED; local AWS and Confluent provider packages are not initialized |

Two non-final failures were retained in the audit trail:

1. a transitional Maven run failed after the Cassandra dependency was removed
   but before obsolete Cassandra source files were deleted;
2. the first `mvn -B clean package` attempt reported missing generated Avro
   classes during test compilation. An immediate clean test and a second clean
   package both compiled from an empty target and passed. Treat recurrence in
   CI as a build-environment risk rather than hiding the transient result.

The shaded JAR still emits known third-party overlapping-resource/class
warnings, and `TaobaoStreamJob` uses a deprecated Flink sink API. The final
build succeeds; neither warning is evidence of runtime correctness.

## 6. Verified acceptance criteria

- `serving` selects Redis and no active Cassandra service.
- `core` does not require Redis configuration.
- Redis configuration rejects missing host, partial ACL authentication,
  invalid TLS values, invalid prefixes, and unbounded/invalid TTL values.
- Cart and buy events map to the expected Hash mutations.
- Stale event ordering remains rejected by the existing Flink projector.
- Redis mutation retry behavior is deterministic at the logical key/field
  boundary.
- Lookup reads one user's Hash and prints decoded items in numeric item order.
- Local and managed Redis configuration share one Java client path.
- Cassandra source, tests, dependency, CQL, Compose service, and Astra
  Terraform artifacts are removed from the active implementation.
- Active architecture, operations, runbook, semantics, index, and learning
  documentation agree with the Redis implementation.
- The final shaded JAR contains Redis/Jedis code and no project Cassandra or
  DataStax classes.

## 7. NOT VERIFIED

- live Redis/Valkey connection and authentication;
- a real Flink -> Redis serving-profile run;
- TTL expiry observed on a live Redis key;
- checkpoint/restart behavior while Redis is unavailable;
- live Kafka, Schema Registry, and ClickHouse end-to-end execution;
- Terraform validation after provider initialization;
- cloud deployment or teardown;
- throughput, latency, memory capacity, or comparative Cassandra/Redis
  performance;
- global exactly-once behavior.

## 8. Blockers and risks

- Terraform validation requires `terraform init` to download the locked AWS
  and Confluent providers. Network permission was not granted in this run.
- `HSET`/`HDEL` and `EXPIRE` are separate synchronous commands. A command
  failure throws so Flink can retry, but live failure behavior must be tested.
- Refreshing the TTL on every mutation bounds inactive-user keys; it does not
  cap the item count of one continuously active user's Hash.
- The synchronous sink deliberately favors simplicity. Throughput claims need
  real measurements before async/pipelining is considered.
- Managed endpoint ACL/TLS compatibility is configuration-tested only.

## 9. Student learning task

Explain and manually verify the Redis key/TTL decision:

1. read `RedisConfig`, `RedisCartCodec`, `ActiveCartProjector`, and
   `RedisActiveCartSink`;
2. trace one `cart`, an older repeated `buy`, and a newer `buy` for the same
   `(user_id, item_id)`;
3. state the final Hash field and why the key remains bounded after the user
   becomes inactive.

## 10. Controlled failure experiment

Run the no-service JShell experiment in
`docs/learning/05_TESTS_AND_EXPERIMENTS.md`: construct `RedisConfig` with
`REDIS_CART_TTL_SECONDS=0`.

Expected: `IllegalArgumentException` before any network call. Repeat with `60`;
configuration construction should succeed because 60 seconds is the minimum.

## 11. Teach-back questions

1. Why is one Redis Hash per user with `item_id` fields a better fit for this
   query than storing every raw cart event?
2. Why must TTL be required, and what state can still grow even when TTL is
   refreshed correctly?
3. After a checkpoint recovery repeats `HSET` or `HDEL`, why does the serving
   result converge even though the Redis sink is not globally exactly once?

## 12. Phase boundary confirmation

No later phase was started. Product catalog CDC, async Redis tuning, live cloud
deployment, and final end-to-end release verification remain future work and
cannot begin while the `NOT VERIFIED` items above remain unless the user
explicitly overrides the phase gate.
