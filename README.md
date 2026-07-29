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

## Checks and local core

```bash
make checks
make start
make replay
make verify
make stop
```

The Flink Python image is intentionally not built during `make checks`; it is
large and requires a disposable runtime host. The committed ClickHouse Kafka
Engine queues target the local broker; secure Confluent Cloud-to-ClickHouse
ingestion remains a documented deployment gate, not a verified claim. Catalog:

```bash
docker compose -f infra/docker-compose.yml --profile core --profile catalog up -d
POSTGRES_PASSWORD=local-catalog python -m scripts.register_connector
```

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
