# Architecture decision

The portfolio demonstrates a Python/SQL data authoring surface over a JVM
streaming engine. Declarative transformations stay visible in SQL; custom
stateful behavior stays in small PyFlink operators.

The accepted stream uses Flink's built-in bounded-out-of-orderness watermark
strategy. Late classification is relative to the watermark actually emitted
by the runtime; the exact bounded-fixture timing therefore remains a live
verification item. No custom Java source is part of the active pipeline.

PyFlink keyed operators cross the Python worker boundary, so they are expected
to cost more CPU and serialization than equivalent JVM operators. No throughput
or latency claim is made until the remote bounded run is measured. The chosen
boundary optimizes authoring clarity; SQL planning and JVM connector I/O remain
inside Flink.

The legacy Java job is excluded from the Maven reactor. It remains solely
because removing the only previously tested runtime before live parity would
eliminate the rollback path.

Product catalog CDC is implemented as isolated current-state replication. It
replaces the old `behavior_rules` idea; behavior rules, Broadcast State,
cart-abandonment timers and alerts are not active architecture.

The committed five-row catalog is a bounded contract fixture. Full-dataset
catalog preparation is a Python ingestion concern: a bounded-memory generator
extracts every semantically valid `(item_id, category_id)` and bulk-loads
PostgreSQL before the Debezium initial snapshot. Synthetic product names and
prices are never presented as fields from the Alibaba dataset and never alter
canonical behavior or metric attribution.

The Redis adapter and ClickHouse Kafka Engine tables are delivery adapters, not
additional business-processing systems. Flink still computes canonical
classification, metrics and cart mutations.
