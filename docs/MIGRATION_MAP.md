# Runtime capability map

| Capability | Active implementation | Status |
| --- | --- | --- |
| Kafka/Confluent Avro source | SQL + packaged JVM connector | implemented, static |
| Validation | Flink SQL | implemented, static |
| Event-ID dedup + TTL | Processing-time keep-first SQL `ROW_NUMBER`, Table state TTL | implemented, static |
| Watermarks | SQL DDL, bounded disorder, on-event emission | implemented, static |
| Late routing | SQL `CURRENT_WATERMARK` | implemented, static |
| One-minute metrics | Flink SQL | implemented, static |
| One-minute user features | Flink SQL | implemented, unit/contract tested |
| Raw/metric/quality delivery | SQL Kafka sinks + ClickHouse adapter | implemented, static |
| Offline feature history | ClickHouse replacement table/view | implemented, static |
| Online feature store | Redis Hash + TTL + atomic version guard | implemented, unit tested |
| Active-cart latest state | SQL streaming Top-1 | implemented, static |
| Redis materialization | Python platform adapter | implemented, unit tested |
| Product CDC transport | Debezium unwrap + compacted Kafka topic | implemented, static |
| Current product catalog | ClickHouse Kafka Engine + replacement | implemented, static |

Per-duplicate durable audit rows and custom PyFlink callbacks were deliberately
removed. Duplicate volume remains independently reconcilable from valid input
and accepted unique output.

Live execution of this revised SQL/feature plan and checkpoint restoration are not yet
verified. Earlier mixed DataStream/Table executions are historical diagnostics,
not evidence for this plan.
