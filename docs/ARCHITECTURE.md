# Architecture

The job uses Table/SQL for relational transformations and PyFlink keyed
operators for stateful event processing. Kafka/Avro connector I/O runs through
the Flink runtime and packaged connector dependencies.

The accepted stream uses Flink's built-in bounded-out-of-orderness watermark
strategy. Late classification is relative to the watermark actually emitted
by the runtime; the exact bounded-fixture timing therefore remains a live
verification item.

PyFlink keyed operators cross the Python worker boundary, so they add CPU and
serialization overhead relative to JVM operators. No throughput or latency
claim is made until the remote bounded run is measured. SQL planning and
connector I/O remain inside Flink.

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

The Redis adapter and ClickHouse Kafka Engine tables are delivery adapters, not
additional business-processing systems. Flink still computes canonical
classification, metrics and cart mutations.
