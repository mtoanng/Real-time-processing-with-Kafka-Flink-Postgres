# Architecture decision

The portfolio demonstrates a Python/SQL data authoring surface over a JVM
streaming engine. Declarative transformations stay visible in SQL; custom
stateful behavior stays in small PyFlink operators.

Flink 1.20 does not expose a custom Python `WatermarkGenerator`. The connector
bundle therefore contains one minimal JVM adapter that emits the bounded-demo
watermark after each event. It contains no validation, deduplication, cart, or
aggregation business logic and is loaded from the Flink platform classpath.

PyFlink keyed operators cross the Python worker boundary, so they are expected
to cost more CPU and serialization than equivalent JVM operators. No throughput
or latency claim is made until the remote bounded run is measured. The chosen
boundary optimizes authoring clarity; SQL planning and JVM connector I/O remain
inside Flink.

The legacy Java job is not the target architecture. It remains solely because
removing the only previously tested runtime before live parity would eliminate
the rollback path.

Product catalog CDC is implemented as isolated current-state replication. It
replaces the old `behavior_rules` idea; behavior rules, Broadcast State,
cart-abandonment timers and alerts are not active architecture.

The Redis adapter and ClickHouse Kafka Engine tables are delivery adapters, not
additional business-processing systems. Flink still computes canonical
classification, metrics and cart mutations.
