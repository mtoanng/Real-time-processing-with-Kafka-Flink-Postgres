# Active architecture blueprint

## Release architecture

Core:

```text
Taobao -> Python -> Kafka/Avro -> one SQL/PyFlink Flink job
-> ClickHouse canonical raw/metrics/quality
-> Redis active-cart serving projection
```

Approved, isolated extensions:

```text
HTTP -> same Kafka behavior contract
PostgreSQL product_catalog -> Debezium -> Kafka -> ClickHouse Kafka Engine
-> ClickHouse current catalog
API -> Redis carts + ClickHouse metrics/current-catalog query
```

Catalog is current-state replication, not temporal enrichment. It cannot alter
behavior identity, canonical raw, or source-category metrics.

Table/SQL owns behavior validation, table contracts, windows and aggregation.
PyFlink DataStream owns event-ID state TTL,
duplicate/late evidence, watermarks and active-cart transitions. Kafka,
ClickHouse and Redis integrations are runtime boundaries.
