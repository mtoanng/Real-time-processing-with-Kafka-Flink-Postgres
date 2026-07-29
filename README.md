# Taobao Python-SQL streaming platform

Project 1 is a recoverable event-time pipeline whose Data Engineer authoring
surface is Python and SQL. Flink still runs on the JVM and uses packaged JVM
connectors; that is intentionally different from authoring business logic in
Java.

```text
Taobao fixture -> Python replay or thin HTTP boundary
-> Kafka Avro + Schema Registry
-> one SQL/PyFlink Flink job
   -> ClickHouse canonical raw events, 1-minute metrics, quality events
   -> compacted cart mutations -> Python adapter -> Redis active cart

PostgreSQL product_catalog -> Debezium -> compacted Kafka topic
-> non-enriching branch in the same Flink job
-> ClickHouse canonical current catalog
```

The catalog branch never changes source-faithful raw behavior or the metric
grain `(window_start, item_id, source_category_id)`. The API reads Redis carts
and joins canonical ClickHouse metrics to current catalog metadata.

The five committed products are only the bounded test fixture. For the real
`UserBehavior.csv`, `taobao-catalog` scans the file with bounded Python memory,
derives every valid `(item_id, category_id)`, and produces a deterministic
synthetic operational catalog plus a coverage manifest. Names and prices are
explicitly synthetic; Alibaba does not provide them in this dataset.

## Profiles

- `checks`: Python contracts, connector packaging, lint and Compose rendering;
  no service connections.
- `core`: Kafka, Schema Registry, one Flink job, ClickHouse, Redis and the thin
  Redis materializer.
- `catalog`: optional PostgreSQL/Debezium catalog source; use together with
  `core`.
- `api`: optional HTTP ingress/query boundary; use together with `core`.

The former Java DataStream job is excluded from the active Maven reactor and
remains only as rollback evidence until live SQL/PyFlink parity and checkpoint
restoration are proven. The active pipeline contains no authored Java source.
`flink-connectors/` is a dependency manifest, not a Java pipeline. CI packages
those pinned JVM connectors into a versioned custom Flink runtime image.

## Checks and local core

```bash
make checks
make start
make replay
make verify
make stop
```

For a repeatable AWS EC2 deployment and recovery procedure, follow
[the runbook](docs/RUNBOOK.md). Operational checks, rollback, secrets and the
self-hosted EC2 boundary are in [operations](docs/OPERATIONS.md).

The standard remote path is GitHub Actions followed by one small SSH deployment
script. After checks pass, a protected `aws-demo` environment deploys the exact
Git commit, builds SHA-tagged runtime images on EC2, and starts Compose. This
intentionally avoids a container registry and extra AWS control-plane services
for the single-host portfolio deployment.

The Flink Python image is intentionally not built during `make checks`; it is
large and requires a disposable EC2 host. The committed ClickHouse Kafka Engine
queues target the self-hosted Kafka broker on the same Docker network.
Full-dataset catalog:

```bash
PYTHONPATH=producer/src python -m taobao_catalog data/UserBehavior.csv
docker compose -f infra/docker-compose.yml --profile catalog up -d postgres
bash scripts/load_product_catalog.sh
docker compose -f infra/docker-compose.yml --profile core --profile catalog up -d kafka-connect
POSTGRES_PASSWORD=local-catalog python scripts/register_connector.py
```

Load PostgreSQL before registering Debezium so its initial snapshot contains
the complete generated catalog. The runbook also retains the five-row
fixture-only path.

HTTP replay:

```bash
docker compose -f infra/docker-compose.yml --profile core --profile api up -d api
PYTHONPATH=producer/src python -m taobao_replay http \
  tests/fixtures/user_behavior_fixture.csv --run-id http-a
```

Canonical reads use `raw_behavior_events_canonical`,
`item_metrics_1m_canonical`, `stream_quality_events_canonical`, and
`product_catalog_current_canonical`. They use ClickHouse `FINAL`; ordinary
table reads are not immediately duplicate-free.

Checkpoint-consistent Flink state/Kafka offsets plus deterministic output keys
provide recoverable, effectively-once canonical results. This is not a global
transaction and not end-to-end exactly once.

Status: credential-independent checks are the CODEBASE gate. Full Kafka/Flink/
ClickHouse/Redis execution, catalog convergence, and failure recovery remain
`NOT VERIFIED` until real evidence is captured.
