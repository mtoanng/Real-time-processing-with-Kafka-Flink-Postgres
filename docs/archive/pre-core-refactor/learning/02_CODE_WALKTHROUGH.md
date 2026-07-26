# Layer 2: Code and Data Walkthrough

> Archived learning material; see `docs/SEMANTICS.md`.

## Component Map

| Component | Exact path and entrypoint | Input/output | Configuration and infrastructure | Tests | Commands, verification, teardown | Status |
| --- | --- | --- | --- | --- | --- | --- |
| Source audit and profiling | `producer/src/taobao_replay/cli.py`, `source_audit.py`, `profile.py` | Raw CSV -> audit/profile JSON | `config/dataset-source.example.json` and CLI args | `producer/tests/test_source_audit.py`, `test_profile.py` | `PYTHONPATH=producer/src python -m taobao_replay source-audit ...`; no cloud teardown | Local tests pass |
| Replay parser/model | `reader.py`, `contracts.py`, `replay.py` | CSV rows -> `UserBehaviorEvent` or `ParseIssue` | `--batch-size`, `--run-id`, `--speed` | `test_reader.py`, `test_contracts.py`, `test_replay.py` | `PYTHONPATH=producer/src python -m taobao_replay replay ...`; output files are local artifacts | Local tests pass |
| Kafka/Avro producer | `kafka.py`, `cli.py publish` | Event -> Avro value and string user key | `KAFKA_*`, `SCHEMA_REGISTRY_*`, `KAFKA_TOPIC` | `test_kafka.py`, `test_schema.py` | `bash scripts/register_schemas.sh`; `bash scripts/replay.sh`; remote services must be stopped separately | Code/static only; broker not verified |
| Avro contracts | `schemas/user-behavior-event.avsc`, behavior-rule schemas | Typed event/rule -> registry subjects | Schema Registry URL/auth | `AvroContractTest`, `test_schema.py` | `bash scripts/register_schemas.sh`; no teardown in schema script | Static compatibility checks pass |
| Flink job | `flink-jobs/taobao-stream-job/.../TaobaoStreamJob.java` | Kafka Avro events -> ClickHouse and mandatory Cassandra; full adds alerts | `RUNTIME_PROFILE`, `KAFKA_*`, `SCHEMA_REGISTRY_*`, `FLINK_*`, `CLICKHOUSE_*`, `CASSANDRA_*` | Maven suite under `src/test/java` | `mvn -B -pl flink-jobs/taobao-stream-job -am package`; `bash scripts/run_flink.sh` | Build/tests pass; live Flink not verified |
| Event validation/late routing | `EventValidator.java`, `LateEventRouter.java`, `ImmediateBoundedOutOfOrdernessGenerator.java` | Valid events plus durable ClickHouse invalid/late audits | Five-second out-of-orderness, 30-second idleness | `EventValidationTest`, `WatermarkAndLateDataTest`, ClickHouse contracts | Covered by Maven tests and bounded verification | Local/static verified |
| Item analytics | `ItemMetricsAggregator.java`, `ItemMetricsWindowFunction.java` | On-time events -> one-minute `ItemMetrics1m` | `CLICKHOUSE_*`; window is event-time, zero allowed lateness | `ItemMetricsAggregationTest`, ClickHouse contract tests | `PYTHONPATH=producer/src python scripts/apply_clickhouse_schema.py`; `python scripts/verify_clickhouse.py --run-id ...` | Schema/mapping verified; live results not verified |
| ClickHouse sinks | `ClickHouseSinkFactory.java`, `ClickHouseRowMapper.java` | Raw, metrics, invalid, late, and alert records -> stable-key replacement tables | `CLICKHOUSE_ENDPOINT`, user/password/database | Mapping, DDL, and delivery contract tests | Apply schema, run Flink, then `verify_clickhouse.py`; use deduplicated views | Static only |
| Active-cart projection | `ActiveCartProjector.java`, model classes under `model/` | User-keyed events -> `CartMutation` | Mandatory in `core` and `full` | `ActiveCartProjectorTest` | Inspect through fixed lookup CLI | Local/static verified |
| Cassandra sink | `CassandraConfig.java`, `CassandraSessionFactory.java`, `CassandraActiveCartSink.java` | Cart mutation -> prepared CQL upsert/delete | `CASSANDRA_*`, `ASTRA_DB_*`; SCB is an ignored runtime secret | `CassandraConfigTest`, `CassandraActiveCartSinkTest`, `CassandraCqlContractTest` | `bash scripts/apply_cassandra_schema.sh`; no real Astra command was run | Code/static verified; Astra not verified |
| Active-cart lookup | `ActiveCartLookupCli.java`, `scripts/lookup_active_cart.sh` | `--user-id` -> rows from one fixed SELECT | Same Cassandra/Astra variables | `ActiveCartLookupCliTest` | `bash scripts/lookup_active_cart.sh 100`; teardown is external Astra cleanup | Code/static only |
| CDC control plane | `infra/postgres/schema.sql`, `infra/debezium/postgres-behavior-rules.json`, `CartAbandonmentRuleProcessor.java` | PostgreSQL changes -> compacted Kafka rules -> durable alerts | `RUNTIME_PROFILE=full`, `RULES_*`, `POSTGRES_*`, `CONNECT_URL` | `RuleVersionPolicyTest`, `RuntimeProfileConfigTest`, Debezium Avro resource | `docker compose --profile full ...`; `scripts/create_behavior_rules_topic.sh` | Static/code present; live CDC not verified |
| Grafana | `infra/grafana/provisioning/`, `taobao-overview.json` | ClickHouse queries -> dashboards | `RUNTIME_PROFILE=full`, `CLICKHOUSE_GRAFANA_*` | JSON/provisioning validation | `docker compose --profile full ...`; stop with `scripts/stop.sh` | Optional visualization; rendering not verified |
| Runtime/deployment | `infra/docker-compose.yml`, `infra/terraform/`, `infra/terraform/cloud_user_data.sh` | Profiles and host resources -> runnable environment | Terraform variables, runtime bundle/JAR URLs, injected secrets | Terraform fmt; Compose config | `terraform -chdir=infra/terraform fmt -check`; `init`/`validate` pending provider; teardown requires `CONFIRM_TEARDOWN=YES` | No apply/destroy; cloud not verified |

## Configuration Boundary

`RUNTIME_PROFILE=checks` opens no external services. `core` and `full` require
Cassandra and default checkpointing on. The local Cassandra contract is:

```text
CASSANDRA_MODE=local
CASSANDRA_HOSTS=localhost
CASSANDRA_PORT=9042
CASSANDRA_DATACENTER=datacenter1
CASSANDRA_KEYSPACE=taobao_streaming
CASSANDRA_TABLE=user_active_cart
```

For Astra, use `CASSANDRA_MODE=astra`, an ignored Secure Connect Bundle, and a
secret-managed application token. Both modes use the same cart logic and CQL.

## Representative Row Trace

Use fixture row 1:

```text
100,500,50,cart,1511658000
```

1. `reader.py` yields source sequence `0` and `contracts.parse_event` converts
   the timestamp to `1511658000000` milliseconds.
2. `UserBehaviorEvent` receives a deterministic SHA-256 `event_id`, user `100`,
   item `500`, category `50`, behavior `cart`, and replay run ID.
3. `event_to_avro` produces the record matching
   `schemas/user-behavior-event.avsc`. The producer publishes it to
   `user-behavior-events` with Kafka key `"100"`.
4. Kafka hashes key `"100"` to one partition according to the broker’s
   partitioner. The exact partition number is runtime metadata, not a value
   hard-coded by this repository.
5. `KafkaSource` reads the Avro value through the Schema Registry deserializer
   and produces a generated `UserBehaviorEvent`.
6. `EventValidator` accepts positive IDs, a valid behavior enum, a non-negative
   event time, source sequence, event ID, and replay run ID.
7. The timestamp assigner uses `1511658000000`; the watermark generator applies
   the five-second bound. `LateEventRouter` forwards it because it is on time.
8. The item branch keys by `(replay_run_id, item_id)`, adds one cart count to
   the event-time one-minute window, and eventually emits an `ItemMetrics1m`
   row to ClickHouse. The accepted event also goes to `raw_behavior_events`.
9. The mandatory active-cart branch keys by user `100`, stores
   item `500` in `MapState`, and emits `UPSERT_CART_ITEM`.
10. `CassandraActiveCartSink` binds the mutation to the prepared INSERT for
    `user_active_cart` with partition key `user_id=100` and clustering key
    `item_id=500`.

The later fixture row `100,500,50,buy,...` emits `DELETE_CART_ITEM`; the later
cart for item `501` remains. Thus the fixed lookup for user `100` returns item
`501` only.

## Java Flink Concepts Used

- `StreamExecutionEnvironment`: builds the lazy job graph and controls
  parallelism/checkpointing.
- `KafkaSource`: source connector with group ID, earliest offsets, and Avro
  deserialization.
- `DataStream` and `SingleOutputStreamOperator`: typed stream nodes and
  process-function outputs.
- `ProcessFunction`: validation, late routing, side outputs, and keyed
  broadcast processing.
- `OutputTag`: typed side output for invalid or late events.
- `WatermarkStrategy`, timestamp assigner, and `WatermarkGenerator`: event-time
  progress and bounded out-of-order handling.
- `keyBy`: partitions logical state by user or item/run key.
- `AggregateFunction` plus `ProcessWindowFunction`: incremental item metrics
  followed by window-context enrichment.
- `KeyedProcessFunction` and `MapState`: per-user active-cart lifecycle and
  stale-event ordering.
- `KeyedBroadcastProcessFunction`, `BroadcastState`, and
  `ReadOnlyBroadcastState`: distribute versioned rules to keyed event
  processing.
- `RichSinkFunction.open/close`: create, prepare, reuse, and close Cassandra
  resources per operator instance.
- Operator names and stable UIDs: identify topology nodes and support future
  state continuity exercises.
