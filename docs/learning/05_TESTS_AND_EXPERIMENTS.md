# Tests, Evidence, and Learning Experiments

## 1. Evidence levels

| Level | What it demonstrates | What it does not demonstrate |
| --- | --- | --- |
| Pure unit test | One function/class behavior | Real connector or service behavior |
| Flink harness/state test | Operator output, TTL, state snapshot/restore | Multi-process cluster recovery |
| Contract/static test | Code, schema, DDL/Redis key and TTL, Compose, or Terraform agreement | Live service compatibility |
| Package build | Java compiles and shaded JAR is created | JAR executes successfully against services |
| Deterministic independent computation | Expected bounded business results | Actual pipeline produced them |
| Live bounded integration | Real services process deterministic input | Scale, performance, or production readiness |
| Controlled recovery experiment | Business result survives a documented failure | All failure modes or global exactly once |

## 2. Credential-independent checks

Predicted result before running: the Redis migration should keep the core tests
green, replace Cassandra serving tests with Redis key/TTL/sink tests, render
all profiles without starting services, and create a shaded JAR containing
Jedis but no Cassandra driver.

The standard checks are:

```bash
PYTHONPATH=producer/src python -m unittest discover -s producer/tests -v
ruff check producer scripts
ruff format --check producer scripts
mvn -B -pl flink-jobs/taobao-stream-job -am package
docker compose -f infra/docker-compose.yml --profile core config --quiet
docker compose -f infra/docker-compose.yml --profile serving config --quiet
docker compose -f infra/docker-compose.yml --profile cdc config --quiet
docker compose -f infra/docker-compose.yml --profile observability config --quiet
terraform -chdir=infra/terraform fmt -check -recursive
terraform -chdir=infra/terraform validate
```

The `cdc` render is compatibility evidence only, not release approval.

## 3. Tests by decision

### Dataset and event grain

Read:

- `test_source_audit.py`
- `test_reader.py`
- `test_profile.py`
- `test_prepare_demo_subset.py`

These tests prove official-source metadata rules, exact five-column shape,
header handling, stable sequence assignment, bounded batching, fixture
reproducibility, and deterministic subset copying.

### Event identity

Read:

- `test_contracts.py`
- `test_reconciliation.py`
- `ClickHouseDeliveryContractTest.java`

These tests show that replay run does not affect event ID, while source
sequence or a stable source field does. They also enforce replay-independent
canonical results.

### Kafka key and schema

Read:

- `test_kafka.py`
- `test_schema.py`
- `AvroContractTest.java`

These cover user-key mapping, security validation, delivery failures, Confluent
wire format, generated Java record round trips, and deprecated rule
reader/writer resolution.

### Watermark

Read:

- `WatermarkAndLateDataTest.java`
- `test_reconciliation.py`

The Java test uses the fixture’s backward jump and custom generator. The
independent Python computation reproduces the same five-second watermark rule
without calling Java.

### Deduplication

Read:

- `DeduplicationConfigTest.java`
- `EventDeduplicatorTest.java`

The operator harness tests first acceptance, duplicate side output, different
IDs, TTL expiry, and restoration of seen IDs from a snapshot.

### Metric grain

Read:

- `ItemMetricsAggregationTest.java`
- `ClickHouseDdlContractTest.java`
- `test_reconciliation.py`

Together they enforce `(window_start, item_id, source_category_id)`, behavior
counts, distinct users, lineage independence, and matching ClickHouse order
keys.

### Redis key, TTL, and ordering

Read:

- `ActiveCartProjectorTest.java`
- `RedisConfigTest.java`
- `RedisCartCodecTest.java`
- `RedisActiveCartSinkTest.java`
- `ActiveCartLookupCliTest.java`

These enforce the per-user Hash key, item field/value encoding, required TTL,
cart/buy transitions, stale rejection, fixed lookup shape, and client
lifecycle.

### Recovery

Read:

- `CheckpointPolicyTest.java`
- `EventDeduplicatorTest.java`
- `test_canonical_snapshot.py`
- `scripts/run_flink_recovery_test.sh`

Static tests verify the configuration and comparison logic. Only a real
controlled run can verify multi-process recovery.

## 4. Independent expected-result computation

Run:

```bash
REPLAY_RUN_IDS=fixture-run-a,fixture-run-b \
PYTHONPATH=producer/src python scripts/reconcile_final_e2e.py
```

The script independently applies:

- semantic-invalid classification;
- set-based event-ID deduplication;
- immediate five-second watermark logic;
- one-minute item/source-category aggregation;
- cart/buy ordering.

It writes counts and SHA-256 digests to
`docs/evidence/latest/expected-reconciliation.json`.

Because this file is generated without a live pipeline, label its values
“expected” rather than “observed.”

## 5. Controlled experiment A: replay identity

Purpose: demonstrate why replay run is lineage rather than business identity.

Prerequisites: one running checkpointed job, fresh ClickHouse schema, registered
event schema, and the bounded fixture.

Run:

```bash
RUN_A_ID=fixture-run-a RUN_B_ID=fixture-run-b \
  bash scripts/run_replay_identity_experiment.sh
```

Expected:

- canonical raw count/digest do not change because of the second attempt;
- canonical metric count/digest do not change;
- second-attempt valid rows become duplicate quality evidence;
- quality remains filterable by replay run.

Student verification: inspect `contracts.deterministic_event_id()` and explain
which one-line change would incorrectly make the two attempts distinct.

## 6. Controlled experiment B: checkpoint recovery

Purpose: distinguish managed-state recovery from external sink transactions.

Use two fresh environments:

1. uninterrupted baseline;
2. recovery run with a TaskManager restart after a completed checkpoint.

The guarded failure command runs only when:

```text
RECOVERY_TEST_CONFIRM=YES
```

The script:

1. starts paced replay;
2. polls Flink until a completed checkpoint exists;
3. records its ID;
4. executes the operator-supplied TaskManager failure command;
5. waits for the job to return to `RUNNING`;
6. waits for replay completion;
7. captures stable canonical outputs;
8. compares them with the uninterrupted baseline.

The failure must restart/fail a TaskManager, not cancel the job. Cancellation
would test a different lifecycle.

## 7. Controlled failure experiment for this learning phase

Safe local experiment: demonstrate why the Redis TTL is a required boundedness
contract without starting Redis.

After `mvn -B clean package`, start JShell with the compiled classes:

```bash
jshell --class-path flink-jobs/taobao-stream-job/target/classes
```

Then enter:

```java
import com.taobao.behavior.sink.RedisConfig;
import java.util.Map;
RedisConfig.fromEnvironment(
    Map.of("REDIS_HOST", "redis", "REDIS_CART_TTL_SECONDS", "0"));
```

Predict before running: configuration construction throws
`IllegalArgumentException`; no network connection is attempted. Repeat with
`"60"` and predict that construction succeeds because 60 seconds is the
minimum accepted TTL. Exit JShell without changing repository files.

## 8. How to read a test effectively

For each test:

1. identify the business decision in the test name;
2. identify whether inputs are source rows, Avro events, state, or storage
   contracts;
3. predict main output and side outputs;
4. identify what is mocked or absent;
5. state the strongest claim the test supports;
6. state one stronger claim it does not support.

This prevents a passing unit test from being mistaken for deployed-system
evidence.

## 9. Known verification boundary

Currently verified by recorded credential-independent evidence:

- Python tests and formatting/lint contracts;
- Java tests and package build;
- schema, DDL, and Redis key/TTL contracts;
- Compose profile rendering;
- Terraform formatting;
- deterministic expected reconciliation.

Still **NOT VERIFIED** without new real evidence:

- Kafka and Schema Registry permissions/connectivity;
- execution of the shaded JAR on a live Flink cluster;
- completed-checkpoint recovery across a real TaskManager failure;
- ClickHouse physical merge/canonical behavior on a live server;
- Redis connectivity and recovered serving state;
- Terraform validation after provider initialization;
- deprecated CDC runtime;
- cloud deployment and teardown;
- performance, latency, or throughput.

## 10. Teach-back questions

1. Why is the Kafka key `user_id`, while deduplication and metrics use different
   Flink keys?
2. Why does an accepted late event belong in raw history but not in a closed
   metric window?
3. Why does `CheckpointingMode.EXACTLY_ONCE` not justify claiming global
   exactly-once results in this repository?
4. What is the maximum duration of the deduplication guarantee, and what
   happens after it?
5. Why does the Redis design use one Hash key per `user_id` with a required
   TTL?
6. Which fields must be excluded when comparing two replay attempts?
7. What is the difference between a producer rejection and an `INVALID`
   quality event?
8. Why must future product catalog data not replace `source_category_id` in
   core metric identity?
