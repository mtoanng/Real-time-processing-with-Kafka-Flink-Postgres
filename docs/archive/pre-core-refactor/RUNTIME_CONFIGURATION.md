# Runtime Configuration

> Archived pre-core-refactor contract. See `docs/OPERATIONS.md`.

Copy `.env.example` or `.env.cloud.example` to a private ignored file. Do not
commit passwords, application tokens, Secure Connect Bundles, or Terraform
state.

## Profile and Flink

| Variable | Required | Meaning |
| --- | --- | --- |
| `RUNTIME_PROFILE` | Yes | `checks`, `core`, or `full`; the Java job accepts only `core` or `full`. |
| `RUNTIME_DEPENDENCIES` | Operator scripts | `local` starts Compose dependencies; `managed` starts none. |
| `FLINK_PARALLELISM` | No | Job parallelism, default `1`. |
| `FLINK_CHECKPOINTING_ENABLED` | Runtime default `true` | May be `false` for focused local tests. |
| `FLINK_CHECKPOINT_INTERVAL_MS` | When checkpoint policy is parsed | Minimum 1,000 ms; default 60,000. |
| `FLINK_CHECKPOINT_DIR` | When checkpointing is enabled | Durable filesystem/plugin URI or an explicit local demo path. |

Runtime checkpoints use `AT_LEAST_ONCE` and are retained on cancellation. A
local path is not durable if the host is destroyed.

## Kafka and Schema Registry

| Variable | Required | Meaning |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | Runtime | Broker endpoints. |
| `KAFKA_TOPIC` | No | Defaults to `user-behavior-events`. |
| `KAFKA_CONSUMER_GROUP` | No | Defaults to `taobao-stream-job`. |
| `KAFKA_SECURITY_PROTOCOL` | No | `PLAINTEXT` or `SASL_SSL`. |
| `KAFKA_SASL_MECHANISM` | For `SASL_SSL` | Usually `PLAIN`. |
| `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD` | Pair for managed Kafka | Inject privately. |
| `KAFKA_SASL_JAAS_CONFIG` | Alternative SASL input | Never print or commit. |
| `SCHEMA_REGISTRY_URL` | Runtime | Confluent-compatible API endpoint. |
| `SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO` | Managed registry | Private `key:secret` value. |

## ClickHouse

`CLICKHOUSE_ENDPOINT`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, and
`CLICKHOUSE_DATABASE` select the analytical destination. Local Compose exposes
HTTP at `http://localhost:8123`. The Flink sink also uses bounded connection and
socket timeouts.

## Cassandra

Common required variables:

```text
CASSANDRA_MODE=local|astra
CASSANDRA_KEYSPACE=taobao_streaming
CASSANDRA_TABLE=user_active_cart
CASSANDRA_CONNECT_TIMEOUT_MS=10000
CASSANDRA_REQUEST_TIMEOUT_MS=5000
```

Local mode:

```text
CASSANDRA_HOSTS=localhost
CASSANDRA_PORT=9042
CASSANDRA_DATACENTER=datacenter1
CASSANDRA_USERNAME=       # optional pair
CASSANDRA_PASSWORD=
```

Astra mode:

```text
ASTRA_DB_SECURE_BUNDLE_PATH=/private/path/secure-connect.zip
ASTRA_DB_APPLICATION_TOKEN=inject-privately
```

The Java driver reuses one session per Flink sink subtask. Keyspace provisioning
is separate; `scripts/apply_cassandra_schema.sh` creates the local demo keyspace
only in local mode and only creates the table in Astra mode.

## Full profile

`RULES_KAFKA_TOPIC` is required by the Java full profile. PostgreSQL, Debezium,
and Grafana operator scripts additionally require the `POSTGRES_*`,
`CONNECT_*`, `DEBEZIUM_CONFIG_FILE`, `CLICKHOUSE_GRAFANA_URL`, and Grafana
values shown in `.env.example`.
