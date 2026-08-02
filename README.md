# Real-time e-commerce behavior pipeline

This repository processes Alibaba/Taobao user-behavior events into durable
analytical history, event-time metrics, data-quality evidence and low-latency
serving state. It demonstrates how a streaming platform separates transport,
stateful computation, analytical storage and operational serving without
claiming global transactional exactly-once delivery.

The event workload contains page views, cart additions, favorites and
purchases. A deterministic replay client is included for reproducible testing;
it represents an external event producer and is not part of the deployed
processing pipeline.

## Architecture


The behavior job is one Flink SQL plan. Flink publishes explicit Kafka output
contracts instead of coupling computation directly to each database:

- ClickHouse Kafka Engine tables consume raw, metric, quality and feature
  topics in batches and materialize analytical tables.
- Small Python adapters consume compacted cart and feature topics and apply
  idempotent Redis mutations.
- The optional product-catalog path bypasses Flink because it is current-state
  CDC replication, not behavior-stream computation.

This separation lets Kafka absorb bursts and allows each serving system to use
an ingestion pattern suited to its workload.

## Processing model

Each source event carries a deterministic `event_id` derived from stable source
fields and `source_sequence`. `replay_run_id` records lineage only and never
changes canonical identity or metric grain.

The Flink plan performs the following flow:

```text
decoded events
  -> semantic validation
  -> event_id deduplication within a bounded state TTL
  -> canonical raw history
  -> event-time watermark and late classification
  -> on-time one-minute item metrics
  -> on-time one-minute user feature snapshots
  -> latest active-cart projection
```

Important semantics:

- Canonical raw history contains every valid unique event, including events
  that arrived too late for an already closed metric window.
- Item metrics use `(window_start, item_id, source_category_id)` and never use
  replay lineage or synthetic catalog metadata as business keys.
- `INVALID` and `LATE` classifications are durable quality records. Duplicate
  volume is independently reconciled as valid input minus accepted unique
  output.
- `cart` upserts an item, `buy` removes it, and `pv`/`fav` do not mutate the
  active cart. Event ordering prevents an older cart action from recreating an
  item after a newer purchase.
- ClickHouse retains historical feature snapshots, while Redis serves only the
  latest version with a TTL. No model or recommendation system is claimed.

The complete contracts are documented in [stream semantics](docs/SEMANTICS.md).

## Storage responsibilities

| System | Responsibility |
|---|---|
| Kafka | Durable ingress, buffering and explicit materialization contracts |
| Flink | Stateful correctness, event-time processing and streaming projections |
| ClickHouse | Canonical history, one-minute metrics, quality evidence and offline feature history |
| Redis | Current active carts and latest online feature snapshots |
| PostgreSQL | Optional operational product catalog source |
| Debezium | Optional catalog CDC transport into Kafka |

Product catalog data is never required to produce canonical behavior history
or metrics. Query clients may join historical metrics with current catalog
metadata, but this is intentionally not a historical as-of enrichment.

## Recovery and delivery guarantees

- Flink state and Kafka source offsets recover consistently from completed
  checkpoints.
- Kafka output and external database materialization are at least once.
- Stable logical keys, ClickHouse `ReplacingMergeTree` tables and idempotent
  Redis mutations make canonical reads converge after retries and replay.
- ClickHouse canonical views use `FINAL`; ordinary physical-table reads are not
  immediately duplicate-free.
- The complete system is not a distributed transaction and is not end-to-end
  exactly once.

## Runtime profiles

- `checks`: credential-independent tests, lint, packaging and configuration
  validation; opens no external service connections.
- `core`: Kafka, Confluent Schema Registry, Flink, ClickHouse, Redis and the two
  Redis materializers.
- `catalog`: optional PostgreSQL and Debezium catalog replication; run with
  `core`.
- `api`: optional HTTP ingress and query boundary; run with `core`.

## Run the bounded demonstration

Prerequisites are Docker with Compose, Python 3.11 or 3.12, Maven and Java 17.

```bash
make checks
make start
make replay
make verify
make stop
```

`make verify` independently reconciles source accounting, canonical raw events,
metrics, quality records, Redis carts and online/offline feature state against a
committed deterministic fixture.

The full Alibaba dataset is intentionally not committed. Follow the
[deployment and verification runbook](docs/RUNBOOK.md) for dataset placement,
the EC2 deployment, full-source capacity replay, evidence capture and recovery
experiment. Operational diagnostics and rollback are in
[operations](docs/OPERATIONS.md).

The documented single-host demonstration uses an AWS EC2 `m6i.xlarge`
(4 vCPU, 16 GiB RAM) with encrypted gp3 storage. It is a bounded portfolio
deployment, not a production topology or sizing recommendation.

## Repository map

| Path | Purpose |
|---|---|
| `clients/taobao_replay` | External deterministic workload generator |
| `libs/taobao_events` | Shared event identity and ingress contracts |
| `flink-python-pipeline` | Flink Table/SQL processing artifact and thin submission runner |
| `services` | HTTP boundary and Redis materialization adapters |
| `tools/taobao_catalog` | Deterministic optional catalog bootstrap utility |
| `schemas` | Avro, SQL and output contracts |
| `infra` | Docker Compose, ClickHouse DDL and CDC configuration |
| `scripts` | Startup, verification, recovery and deployment workflows |
| `tests` | Credential-independent behavioral and contract checks |
| `docs` | Architecture, semantics, operations and runbooks |

See [architecture](docs/ARCHITECTURE.md) for component boundaries and
[CODEBASE_INDEX.md](CODEBASE_INDEX.md) for a guided reading order.

## Verification status

- Credential-independent codebase checks: implemented and statically verified.
- Bounded Kafka/Flink/ClickHouse/Redis fixture: implemented and verified on the
  self-hosted EC2 demonstration.
- Full-source capacity replay: evidence collection in progress; final result
  not yet verified.
- Failure-and-checkpoint recovery experiment: implemented but not yet verified
  with retained live evidence.
- Product catalog CDC convergence: implemented; live evidence must be captured
  separately before claiming deployment verification.

Measured throughput belongs in retained evidence, not as an unconditional
README promise: ingress burst rate and end-to-end canonical processing rate are
reported separately.
