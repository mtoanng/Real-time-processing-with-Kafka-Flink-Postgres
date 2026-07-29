# Runbook

Prerequisites: Python 3.11+, Maven/Java 11, Docker Compose and enough disk/RAM
for the PyFlink image and the core services/one-shot containers. Use a disposable remote host
when the laptop is constrained.

```bash
cp .env.example .env
make checks
make start
make replay
make verify
```

`make start` packages the Flink 1.20 connector bundle, starts core services,
registers Avro and submits one detached SQL/PyFlink job. Check the job and last
checkpoint at `http://localhost:8082`.

The committed ClickHouse Kafka Engine queues are a local-smoke contract and
connect to `kafka:29092`. A final Confluent Cloud run still requires a
credential-injected ClickHouse Kafka configuration; that path is not
implemented or verified in this phase.

The normal core uses an unbounded Kafka source because the job starts before
replay. For a bounded experiment, publish the fixture first and submit a fresh
job with `KAFKA_SOURCE_BOUNDED=true`.

Catalog extension:

```bash
docker compose -f infra/docker-compose.yml \
  --profile core --profile catalog up -d postgres kafka-connect
POSTGRES_PASSWORD=local-catalog python -m scripts.register_connector
bash scripts/update_catalog.sh
PYTHONPATH=producer/src python scripts/verify.py --with-catalog
```

API extension:

```bash
docker compose -f infra/docker-compose.yml \
  --profile core --profile api up -d api
```

Recovery requires a completed checkpoint. Stop a TaskManager during replay,
allow the fixed-delay restart, then compare canonical business columns with an
uninterrupted run. Do not compare ingestion timestamps. This experiment is
`NOT VERIFIED` unless evidence from real services is saved.

Teardown preserves named volumes:

```bash
make stop
```
