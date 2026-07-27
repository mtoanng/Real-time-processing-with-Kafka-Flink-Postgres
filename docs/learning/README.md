# Current Codebase Learning Guide

This package explains the repository as it is implemented now. It is written
for a reader who wants to understand the system at three levels:

1. the general architecture and the decisions behind it;
2. each component, its boundary, and how components interact;
3. the code modules, classes, functions, state, tests, and operational scripts.

The active source of truth is
[`PROJECT1_BLUEPRINT_FINAL.md`](../PROJECT1_BLUEPRINT_FINAL.md). If this guide
and the blueprint disagree, follow the blueprint and treat the disagreement as
a documentation defect.

Everything under `docs/archive/` describes an earlier implementation. In
particular, the archived learning package predates replay-independent
deduplication, optional Valkey/Redis, the current quality-event model, and the
recovery contract.

## Reading order

| Order | Document | What you should understand afterward |
| --- | --- | --- |
| 1 | [General architecture](01_GENERAL_ARCHITECTURE.md) | What the platform does, why each technology exists, the event lifecycle, storage ownership, and recovery boundary |
| 2 | [Component guide](02_COMPONENT_GUIDE.md) | The contract, input, output, configuration, state, failure boundary, and verification of every component |
| 3 | [Code-module guide](03_CODE_MODULE_GUIDE.md) | Where behavior is implemented and how the Python, Java, schema, infrastructure, and script modules fit together |
| 4 | [Execution traces](04_EXECUTION_TRACES.md) | How valid, invalid, duplicate, late, cart, buy, restart, and repeated-replay scenarios execute line by line |
| 5 | [Tests and learning exercises](05_TESTS_AND_EXPERIMENTS.md) | What is proved locally, what remains unverified, and how to test your understanding safely |

## The shortest accurate description

```text
raw Taobao CSV rows
  -> deterministic Python replay
  -> one Kafka Avro topic
  -> one Java Flink DataStream job
  -> ClickHouse canonical raw history, one-minute metrics, and quality events
```

Optional boundaries:

```text
serving       -> Valkey/Redis bounded per-user active-cart state
observability -> Grafana queries ClickHouse
cdc           -> deprecated behavior-rule compatibility branch
```

The project is not a recommendation system. It has no machine learning,
PyFlink, Spark, Airflow, Kubernetes, S3 event archive, arbitrary query API, or
frontend.

## Vocabulary used throughout the guide

| Term | Meaning |
| --- | --- |
| Source row | One five-column row from raw `UserBehavior.csv` |
| Source sequence | Zero-based row position after an optional fixture header; part of stable event identity |
| Event ID | SHA-256 identity of stable row fields plus source sequence |
| Replay run ID | Name of one replay attempt; lineage only |
| Decoded event | An Avro record successfully read by Flink |
| Valid event | A decoded event that passes semantic validation |
| Accepted unique | First valid occurrence of an event ID within State TTL |
| Late event | Accepted unique event whose event time is at or behind the current watermark |
| Canonical read | A ClickHouse query through a committed `*_canonical` view using `FINAL` |
| Physical row | A transport write stored in a replacement-capable ClickHouse table before canonical collapse |
| Core | Kafka, Schema Registry, one Flink job, and ClickHouse |
| Serving | Core plus optional Valkey/Redis active-cart projection |

## Claims boundary

Credential-independent tests verify source parsing, stable identity, Avro
contracts, Flink operator logic, checkpoint configuration, SQL and Redis
key/TTL contracts,
profile isolation, packaging, and deterministic expected results.

Live Kafka delivery, live Flink recovery, ClickHouse merge behavior, Redis
connectivity, cloud deployment, throughput, and end-to-end latency are **NOT
VERIFIED** unless new evidence is recorded under `docs/evidence/`.

The correct delivery statement is:

```text
Flink state and Kafka offsets: checkpoint-consistent recovery
ClickHouse writes: at least once
ClickHouse canonical reads: effectively once for deterministic logical keys
whole platform: not transactional global exactly once
```
