# Architecture

The active behavior job is one native Flink Table/SQL plan submitted by a thin
Python runner. Kafka/Avro connector I/O, validation, bounded deduplication,
watermarks, streaming Top-1 state and windows execute in the Flink JVM runtime.

The replay executable under `clients/` is an external workload generator. It
is not part of the deployed processing DAG. Runtime adapters live under
`services/`, while `libs/taobao_events` owns the only shared ingress contract;
runtime services never import the replay client.

```text
Python replay/API -> Kafka Avro -> Flink Table/SQL
  -> raw, metric and quality Kafka contracts -> ClickHouse
  -> user feature snapshots -> ClickHouse history + Redis latest features
  -> compacted latest-cart mutations -> Python adapter -> Redis
```

The Kafka Table source defines a bounded-out-of-orderness watermark and uses
Flink 2.2's `scan.watermark.emit.strategy='on-event'`. Late classification is
therefore driven by event arrival rather than Python bundle or periodic timer
timing. SQL `ROW_NUMBER` ordered by processing time retains the first occurrence
of each `event_id` observed by Flink within the configured Table state TTL. This
dedup boundary is insert-only; event time remains independent for late routing
and windows. Individual duplicate audit rows are not emitted; reconciliation
derives their count from valid input and accepted unique output.

No Python UDF or DataStream callback exists in the record path. This removes
the Beam worker serialization boundary while retaining Python for submission,
replay, serving adapters and verification. No throughput or latency claim is
made until the remote run is measured.

Product catalog CDC is isolated current-state replication:
PostgreSQL -> Debezium -> Kafka -> ClickHouse Kafka Engine. It bypasses Flink
because it does not enrich behavior history or perform stream computation.

The committed five-row catalog is a bounded contract fixture. Full-dataset
catalog preparation is a Python ingestion concern: a bounded-memory generator
selects one deterministic dominant source category per valid item and
bulk-loads PostgreSQL before the Debezium initial snapshot. It records category
ambiguity in the manifest. Synthetic product names and prices are never
presented as fields from the Alibaba dataset and never alter canonical behavior
or metric attribution.

The Redis adapters and ClickHouse Kafka Engine tables are delivery adapters,
not additional business-processing systems. Flink SQL computes canonical
classification, metrics, one-minute user features and latest cart mutations.
The feature topic is append-only: ClickHouse consumes every closed snapshot as
offline history, while an independent Redis consumer atomically rejects any
snapshot whose `feature_version` is not newer than the currently served value.
