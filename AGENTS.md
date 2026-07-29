# AGENTS.md — Taobao Real-Time Customer Behavior Platform

## Source of truth

- Read `docs/PROJECT1_BLUEPRINT_FINAL.md` before editing.
- The blueprint overrides obsolete F1 documentation.
- Report important conflicts before changing code.

## Dataset Contract

Required source dataset:

Alibaba Tianchi Taobao UserBehavior raw event dataset.

Expected input:

UserBehavior.csv

Required columns:

- user_id
- item_id
- category_id
- behavior_type
- timestamp


Do NOT use:

- Kaggle cleaned versions
- aggregated clickstream tables
- feature-engineered datasets
- ML-ready datasets
- recommendation datasets derived from Taobao


The replay engine must consume raw event rows and perform transformations downstream.

## Delivery priority

The current approved migration is SQL/Python-first and includes the separately
approved HTTP and product-catalog CDC extension. Do not restore behavior rules,
cart timers, alert logic, enrichment, or unrelated technologies.

## Working mode

- Implement exactly one blueprint phase per run.
- Inspect relevant code and tests before editing.
- Prefer small, reviewable diffs.
- Preserve the previous runnable path until the replacement passes.
- Do not modify unrelated files.
- Do not provision cloud resources without explicit user approval.

# Repository Cleanup Rules

Before implementing any new phase:

1. Inspect existing code, docs, configs and infrastructure files.

2. Identify:
   - obsolete implementations
   - previous dataset references
   - outdated architecture documents
   - unused dependencies
   - dead code
   - duplicate scripts
   - deprecated Docker files

3. Do not preserve old components only for historical reasons.

4. If a previous implementation conflicts with the current blueprint:
   - remove it,
   - archive it,
   - or replace it.

5. Keep repository clean:
   - no unused Java classes
   - no abandoned notebooks
   - no outdated README sections
   - no obsolete diagrams
   - no unused dependencies

6. Every phase report must include:

## Cleanup Report

Removed:
- file/path

Archived:
- file/path

Kept:
- file/path

Reason:
- why this artifact remains

## Locked architecture

```text
Taobao -> Python/HTTP -> Kafka/Schema Registry -> one SQL/PyFlink Flink job
        -> ClickHouse canonical raw/metrics/quality
        -> Redis active-cart serving projection

PostgreSQL product_catalog -> Debezium -> isolated Kafka topic
        -> non-enriching branch in the same Flink job
        -> ClickHouse canonical current catalog
```

Rules:

- Python owns replay, stateful custom logic, orchestration and verification.
- SQL owns relational stream transformations.
- Java/JVM is allowed for runtime, drivers and minimal platform adapters, but
  Data Engineers do not author business pipeline logic in Java.
- ClickHouse stores analytical history and rollups.
- Redis stores bounded per-user active-cart hot state only.
- Product catalog CDC is isolated current-state replication. It does not enrich
  behavior history or change the source-category metric grain.
- `behavior_rules`, Broadcast State, cart timers and alerts are not active
  architecture.
- Confluent Cloud provides Kafka and Schema Registry for final cloud E2E; do not
  add Amazon MSK or self-hosted Kafka to the final topology.
- Do not add Spark, Airflow, MongoDB, Cassandra, Elasticsearch, Kubernetes, ML,
  recommendation, arbitrary SQL APIs, or another serving database.
- Keep the former Java job only as rollback evidence until live parity is
  proven; new business logic belongs in SQL/PyFlink.

## Resource limits

- The laptop must not run the complete stack.
- Use committed fixtures for local tests.
- Use a disposable remote machine for Kafka/Flink integration.
- Use managed trials only after code, schemas, DDL, smoke tests, and teardown steps are ready.
- Use bounded-demo data by default; full-scale runs are optional.



## Learning rule

For each phase:

1. identify one core decision the user must understand;
2. let Codex implement boilerplate and tests;
3. require the user to explain or manually verify that core decision;
4. end with three teach-back questions and one controlled failure experiment.

Core decisions include event grain, Kafka key, watermark, state/timer semantics,
ClickHouse ordering, Redis key/TTL design, and checkpoint recovery.

Do not block all implementation waiting for the user; instead clearly mark the single student learning task for the phase.

## Phase Gate Rule

A phase cannot start if previous phase report contains:

- NOT SATISFIED
- NOT VERIFIED
- FAILED

unless the user explicitly overrides.(Learning can be saved for later)

## Verification

- Predict expected behavior before running important tests.
- Run all credential-independent checks relevant to the diff.
- Report exact commands and concise results.
- Mark cloud, connector, recovery, and performance claims `NOT VERIFIED` until real evidence exists.
- Never fabricate logs, counts, latency, throughput, screenshots, or cloud resources.

Typical checks:

- Python tests for fixture/replay code;
- Maven package and Java tests;
- Avro compatibility;
- ClickHouse/Redis contract tests where available;
- fixture end-to-end smoke test;
- Terraform `fmt -check` and `validate` when IaC exists.

## Documentation and secrets

- Keep README and diagrams aligned with implemented reality.
- Archive obsolete F1 docs only in the designated migration phase.
- Do not commit the raw Taobao dataset, `.env`, credentials, cloud keys, or Terraform state.
- Do not claim `production-ready`, `deployed`, `exactly-once`, or `benchmarked` without evidence.

## Required run report

End every run with:

1. phase implemented;
2. files changed;
3. commands and results;
4. verified acceptance criteria;
5. `NOT VERIFIED` items;
6. blockers and risks;
7. the student learning task;
8. three teach-back questions;
9. confirmation that no later phase was started.



# Final Release Gates

The project is not complete when individual implementation phases pass.

After all implementation phases, the agent must complete:

1. Final End-to-End Release Verification
2. Deployment and Operational Handover

A release must not be tagged as complete while the core execution path contains:

- NOT VERIFIED
- NOT SATISFIED
- FAILED
- SKIPPED without an approved reason

## End-to-End Rules

- Test the complete data path, not isolated components.
- Use deterministic bounded input.
- Reconcile input, accepted, rejected, and output records.
- Validate business results using an independent computation.
- Verify retry or recovery behavior.
- Store evidence under docs/evidence/final-e2e/.
- Do not claim exactly-once behavior without supporting evidence.
- Do not claim production readiness.

## Deployment Rules

- Provide local-smoke and cloud-demo profiles.
- Do not add new infrastructure technologies.
- Document prerequisites, secrets, deployment, verification,
  rollback, troubleshooting, and teardown.
- Deployment must be reproducible from a fresh environment.
- Secrets must never be committed.
- Cloud deployment is VERIFIED only after a real successful run.
- A successful Terraform plan is not proof of deployment.
- A service starting is not proof that the data pipeline works.
