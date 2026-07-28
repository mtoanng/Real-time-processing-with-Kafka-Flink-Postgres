# Taobao event-time streaming core

This repository demonstrates five mechanisms: contract, identity, state, event
time, and materialized outputs.

```text
Taobao UserBehavior.csv rows
  -> deterministic Python replay
  -> Kafka Avro + Schema Registry
  -> one Java Flink DataStream job
       -> validation -> bounded event_id deduplication
       -> watermarks -> late classification -> one-minute metrics
       -> active-cart transitions
  -> ClickHouse canonical history, metrics, and quality
  -> Redis/Valkey bounded active-cart state
```

Python validates raw five-column source rows and creates stable event IDs.
Kafka and Schema Registry form the transport contract. Flink owns stream
semantics and recovery state. ClickHouse owns analytical history. Redis owns
only the rebuildable current cart for each user.

## Read this first

- [Stream semantics](docs/SEMANTICS.md)
- [Runbook](docs/RUNBOOK.md)
- [Active blueprint](docs/PROJECT1_BLUEPRINT_FINAL.md)

The committed 12-row fixture is
`tests/fixtures/user_behavior_fixture.csv`. Its manually declared exact output
is `tests/fixtures/golden_outputs.json`.

## Quick start

Use a disposable runtime host; the complete stack is not intended for a
resource-constrained laptop.

```bash
cp .env.example .env
make checks
make start
make replay
make verify
make stop
```

`make replay` publishes the fixture as `golden-a` and `golden-b`, then submits
one bounded Flink execution. The second attempt has the same event IDs and is
classified as duplicate within the configured state-retention horizon.

## Materialized outputs

| Responsibility | Canonical output |
| --- | --- |
| Accepted unique history | `raw_behavior_events_canonical` |
| One-minute item/category metrics | `item_metrics_1m_canonical` |
| Invalid, duplicate, and late evidence | `stream_quality_events_canonical` |
| Current cart | Redis Hash `taobao:active_cart:{user_id}` |

The ClickHouse views use `FINAL`, so consumer results do not wait for background
replacement merges.

## Verification status

The credential-independent Python and Java suites verify event identity, Avro,
validation, deduplication, watermarks/lateness, exact metrics, canonical
ClickHouse keys, cart convergence, TTL configuration, checkpoint policy,
Compose rendering, and packaging.

Live Kafka/Flink/ClickHouse/Redis output and checkpoint recovery remain
`NOT VERIFIED` until `make replay`, `make verify`, and `make recovery-test`
complete on a real disposable runtime host. This platform does not claim
cross-system transactional exactly-once delivery.
