# Runtime capability map

| Capability | Active implementation | Status |
| --- | --- | --- |
| Kafka/Confluent Avro source | SQL + packaged JVM connector | implemented, static |
| Validation | Flink SQL | implemented, static |
| Event-ID dedup + TTL | PyFlink keyed state | implemented, static |
| Watermarks | built-in bounded-out-of-orderness strategy | implemented, static |
| Late routing | PyFlink DataStream | implemented, static |
| One-minute metrics | Flink SQL | implemented, static |
| Raw/metric/quality delivery | SQL Kafka sinks + ClickHouse adapter | implemented, static |
| Active-cart state machine | PyFlink keyed state | implemented, unit tested |
| Redis materialization | Python platform adapter | implemented, unit tested |
| Product CDC transport | Debezium unwrap + compacted Kafka topic | implemented, static |
| Current product catalog | ClickHouse Kafka Engine + replacement | implemented, static |

Live execution and checkpoint restoration are not verified in this environment.
