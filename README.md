# Taobao event-time streaming platform

This repository implements a recoverable event-time pipeline. A thin Python
runner submits one native Flink Table/SQL plan; packaged JVM connectors handle
Kafka and Avro without Java-authored business logic.

```text
Taobao fixture -> Python replay or thin HTTP boundary
-> Kafka Avro + Schema Registry
-> one Flink Table/SQL job submitted by Python
   -> ClickHouse canonical raw events, 1-minute metrics, quality events
   -> 1-minute user feature history -> ClickHouse offline feature store
   -> latest user features -> Python adapter -> Redis online feature store
   -> compacted cart mutations -> Python adapter -> Redis active cart

PostgreSQL product_catalog -> Debezium -> compacted Kafka topic
-> ClickHouse Kafka Engine -> canonical current catalog
```

The independent catalog CDC path never changes source-faithful raw behavior or the metric
grain `(window_start, item_id, source_category_id)`. The API reads Redis carts
and online features, and joins canonical ClickHouse metrics to current catalog
metadata.

The five committed products are only the bounded test fixture. For the real
`UserBehavior.csv`, `taobao-catalog` scans the file with bounded Python memory,
derives every valid item and produces a deterministic synthetic operational
catalog plus a coverage manifest. If an item appears under multiple source
categories, the dominant category wins with the lowest ID as a deterministic
tie-breaker. Names and prices are explicitly synthetic; Alibaba does not
provide them in this dataset.

## Profiles

- `checks`: Python contracts, connector packaging, lint and Compose rendering;
  no service connections.
- `core`: Kafka, Schema Registry, one Flink job, ClickHouse, Redis and thin cart
  and online-feature materializers.
- `catalog`: optional PostgreSQL/Debezium catalog source; use together with
  `core`.
- `api`: optional HTTP ingress/query boundary; use together with `core`.

`flink-connectors/` packages the pinned runtime dependencies used by the custom
Flink image. Only the Compose-defined pipeline is an active runtime; do not
launch inactive jobs against the same topics or sinks.

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

The documented EC2 target is `m6i.xlarge` (4 vCPU, 16 GiB RAM) in Sydney with
80 GiB gp3 for the bounded fixture or 200 GiB gp3 for the full-source capacity
run. Terraform is not required.

## Repository boundaries

- `clients/taobao_replay`: external deterministic traffic simulator; not a
  deployed processing component.
- `libs/taobao_events`: shared event identity and Kafka/Schema Registry
  transport contract used by approved ingress clients.
- `flink-python-pipeline`: the Flink Table/SQL processing artifact.
- `services`: deployed HTTP query/ingress and Redis materialization adapters.
- `tools/taobao_catalog`: deterministic catalog bootstrap utility, not a
  continuously running pipeline service.
- `infra`: Kafka, Schema Registry, Flink, ClickHouse, Redis and optional CDC
  runtime definitions.

This layout deliberately prevents pipeline services from importing code owned
by the replay simulator. Both depend only on the shared event contract.

The standard remote path is GitHub Actions followed by one small SSH deployment
script. After checks pass, a protected `aws-demo` environment deploys the exact
Git commit, builds SHA-tagged runtime images on EC2, and starts Compose. This
intentionally avoids a container registry and extra AWS control-plane services
for the bounded single-host deployment.

The Flink Python image is intentionally not built during `make checks`; it is
large and requires a disposable EC2 host. The committed ClickHouse Kafka Engine
queues target the self-hosted Kafka broker on the same Docker network.
Full-dataset catalog:

```bash
PYTHONPATH=clients:services:tools:libs python -m taobao_catalog data/UserBehavior.csv
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
PYTHONPATH=clients:services:tools:libs python -m taobao_replay http \
  tests/fixtures/user_behavior_fixture.csv --run-id http-a
```

Canonical reads use `raw_behavior_events_canonical`,
`item_metrics_1m_canonical`, `user_features_1m_canonical`,
`stream_quality_events_canonical`, and `product_catalog_current_canonical`.
They use ClickHouse `FINAL`; ordinary table reads are not immediately
duplicate-free.

The ML-ready feature contract is intentionally small: Flink SQL computes one
closed event-time snapshot per user/minute, ClickHouse retains offline history,
and Redis atomically retains only the greatest `feature_version` with a TTL.
No model, Feast integration or vector index is claimed in this release.

Checkpoint-consistent Flink state/Kafka offsets plus deterministic output keys
provide recoverable, effectively-once canonical results. This is not a global
transaction and not end-to-end exactly once.

Duplicate `event_id` values are removed by Flink SQL within the configured
state TTL. Per-duplicate audit rows are intentionally not produced; the
independent verifier reconciles duplicate count as valid decoded input minus
accepted unique output. INVALID and LATE remain durable quality records.

Status: credential-independent checks are the CODEBASE gate. Full Kafka/Flink/
ClickHouse/Redis execution, catalog convergence, and failure recovery remain
`NOT VERIFIED` until real evidence is captured.
