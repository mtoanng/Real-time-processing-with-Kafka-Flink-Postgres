# Java-to-Python/SQL migration map

| Capability | Target authoring API | Status |
| --- | --- | --- |
| Kafka/Confluent Avro source | SQL + packaged JVM connector | implemented, static |
| Validation | Flink SQL | implemented, static |
| Event-ID dedup + TTL | PyFlink keyed state | implemented, static |
| Watermarks | minimal JVM runtime adapter | implemented, JVM tested |
| Late routing | PyFlink DataStream | implemented, static |
| One-minute metrics | Flink SQL | implemented, static |
| Raw/metric/quality delivery | SQL Kafka sinks + ClickHouse adapter | implemented, static |
| Active-cart state machine | PyFlink keyed state | implemented, unit tested |
| Redis materialization | Python platform adapter | implemented, unit tested |
| Product Debezium decoding | Flink SQL `debezium-json` | implemented, static |
| Current product catalog | SQL upsert + ClickHouse replacement | implemented, static |
| Legacy Java DataStream job | rollback only | remove after live parity |

Live SQL/PyFlink execution and checkpoint restore are not verified in this
environment.
