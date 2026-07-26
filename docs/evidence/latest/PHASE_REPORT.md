# Core Correctness and Recovery Refactor

Date: 2026-07-26

## Status

- CODEBASE-READY: PASS
- DEPLOYMENT-VERIFIED: NOT VERIFIED
- Product Catalog CDC Enrichment: NOT IMPLEMENTED / NOT VERIFIED

No cloud resource was planned, applied, created, or changed.

## Baseline

The original repository baseline was commit `c9dba02` (`validated`). Before the
core refactor, 38 Python tests and 40 Java tests passed and the previous Compose
profiles rendered. When this run resumed, the worktree contained the interrupted
core refactor and no live service evidence.

Resume-time gaps:

- active documentation still presented `behavior_rules` as the CDC target;
- the metric contract still used the ambiguous name `category_id`;
- Terraform providers were not locally initialized;
- uninterrupted/recovered live evidence did not exist.

Pre-existing non-failing warnings remain:

- the shaded JAR reports overlapping resources/classes from third-party
  all-in-one connector dependencies;
- the Cassandra driver path compiles with a deprecated-API warning.

## Implemented core contract

- deterministic, replay-independent `event_id`;
- checkpointed event-ID deduplication with 168-hour default State TTL;
- durable `INVALID`, `DUPLICATE`, and `LATE` ClickHouse quality events;
- source-faithful canonical raw history keyed logically by `event_id`;
- one-minute metrics keyed by
  `(window_start, item_id, source_category_id)`;
- `replay_run_id` retained only as lineage;
- replacement-capable ClickHouse tables and explicit `FINAL` canonical views;
- EXACTLY_ONCE Flink-managed state/Kafka-offset checkpoints, persistent
  checkpoint storage, fixed-delay restart policy, and stable operator UIDs;
- executable repeated-replay and failure/recovery comparison scripts;
- core profile limited to Kafka, Schema Registry, one Flink job, and
  ClickHouse.

The old behavior-rule CDC/timer branch remains compiled but is deprecated,
isolated from core, and excluded from the release target. The approved Product
Catalog CDC replacement has no implementation files in this release.

## Commands and results

| Command | Result |
| --- | --- |
| `PYTHONPATH=producer/src python scripts/reconcile_final_e2e.py` with the two fixture run IDs | PASS; wrote `expected-reconciliation.json` |
| `PYTHONPATH=producer/src python -m unittest discover -s producer/tests -v` | PASS; 42 tests |
| `ruff check producer scripts` | PASS |
| `ruff format --check producer scripts` | PASS; 29 files already formatted |
| `mvn -B test` | PASS; 51 tests |
| `mvn -B -pl flink-jobs/taobao-stream-job -am package` | PASS; shaded JAR built |
| four `docker compose ... --profile <profile> config --quiet` commands | PASS; no containers started |
| Git Bash syntax check for all `scripts/*.sh` and `cloud_user_data.sh` | PASS |
| JSON and Avro parsing plus SQL/CQL static assertions | PASS |
| active Markdown local-link contract | PASS |
| tracked secret-artifact path contract | PASS; local ignored `.env` was not read |
| `terraform -chdir=infra/terraform fmt -check -recursive` | PASS |
| `terraform -chdir=infra/terraform init -backend=false -input=false` | PASS; providers only, no backend/plan/apply |
| `terraform -chdir=infra/terraform validate` | PASS |
| `git diff --check` | PASS |

The generated 687 MiB Terraform provider cache was removed after validation to
respect the constrained C: drive. It is recoverable with `terraform init`.

## Fixture reconciliation

Two attempts of the same 1,000-row fixture independently predict:

- attempted source rows: 2,000;
- producer-rejected rows: 0;
- Flink-decoded rows: 2,000;
- invalid events: 2;
- duplicate events: 999;
- accepted unique events / canonical raw events: 999;
- late-for-aggregation events: 2;
- on-time events: 997;
- canonical metric rows: 995.

The expected raw and metric digests exclude replay lineage and ingestion
timestamps. They are contract evidence, not live ClickHouse evidence.

## Verification boundary

Credential-independent implementation, tests, packaging, schemas, profile
rendering, and Terraform configuration are verified. Live Kafka publication,
Schema Registry compatibility against a service, Flink checkpoint recovery,
ClickHouse replacement/`FINAL` behavior, Cassandra serving, legacy CDC, cloud
deployment, and performance remain NOT VERIFIED.
