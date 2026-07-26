# Failures, Trade-offs, and Claims

> Archived learning material; see `docs/OPERATIONS.md`.

## Failure Scenarios

| Failure | Expected boundary | What is not proven |
| --- | --- | --- |
| Malformed CSV row | Python records a `ParseIssue`; valid rows continue | Source repair or replay of rejected data |
| Kafka unavailable | Producer delivery/flush fails; publish is not reported as complete | Broker recovery and duplicate delivery behavior |
| Registry incompatibility | Registration or Flink deserialization fails the contract | Managed registry compatibility under live evolution |
| Out-of-order event beyond five seconds | Event is routed late and excluded from on-time windows/projector | Whether a later correction process is desired |
| ClickHouse unavailable | Sink delivery can fail or lag independently | Live connector retry/reconciliation |
| Cassandra/Astra unavailable | Core/full fail to update the mandatory active cart | Real connection, retry, and restart behavior |
| Flink restart | Core/full restore at-least-once checkpoints and may repeat external writes | A captured cross-sink recovery experiment |
| Older CDC rule | `RuleVersionPolicy` rejects replacement | Live Debezium ordering under failure |
| Grafana unavailable | Processing and serving continue; visualization is absent | Dashboard rendering against a live ClickHouse |
| Terraform teardown requested without confirmation | Guarded script refuses unless explicitly confirmed | Actual cleanup evidence |

## Design Trade-offs

- **Python replay instead of direct ingestion:** keeps dataset preparation and
  replay concerns separate from the Java streaming job, but adds a producer
  runtime and Kafka boundary.
- **Avro and Schema Registry instead of schemaless JSON:** provides a typed
  contract and compatibility checks, but requires registry availability and
  subject management.
- **User Kafka key:** preserves per-user partition affinity for stateful
  processing, but can create hot partitions for very high-volume users.
- **Five-second watermark bound:** makes fixture behavior deterministic and
  limits window delay, but events later than the bound become late side output.
- **ClickHouse plus Cassandra:** each store has a focused query shape; this
  duplicates materialization work and requires consistency reasoning between
  history and operational state.
- **Flink state before Cassandra:** makes ordering decisions in the stream
  processor, but recovery depends on checkpoint configuration and Cassandra is
  not part of a claimed exactly-once transaction.
- **Astra SCB plus CQL driver:** uses native Cassandra semantics and a narrow
  query contract, but requires runtime secret files and cloud provisioning.
- **Broadcast rules:** avoids redeploying for rule updates, but requires a
  compacted topic, version policy, and careful state semantics.
- **Local Compose data-plane dependencies:** make bounded demos repeatable but
  are too heavy for the normal laptop checks profile; rendering alone cannot
  prove end to end.

## Current Limitations

- No real Astra deployment or CQL connection has been performed.
- Terraform formatting is checked, but provider initialization/validation and
  cloud planning remain status-bound by the project reports.
- Live Kafka, Schema Registry, Flink, ClickHouse, CDC, Grafana, recovery, and
  teardown evidence is not claimed.
- Checkpointing defaults on in core/full and remains configurable/off for
  credential-independent checks; the mode is at-least-once.
- Cassandra stores bounded active-cart state only; expiration is not included.
- No arbitrary CQL, mutation API, REST API, or second Flink SQL pipeline exists.
- The deployment bundle and remote host are operational artifacts; a service
  starting is not equivalent to a verified data-path run.

## Prohibited Claims Before Cloud Verification

Do not say:

- “deployed on Astra”;
- “Astra cloud E2E verified”;
- “Cassandra recovery verified”;
- “teardown verified”;
- “exactly once end to end” or “exactly once Cassandra delivery”;
- “production ready,” “benchmarked,” or a measured throughput/latency claim.

Use “implemented and locally/statistically verified” only for the evidence
listed in the status reports.
