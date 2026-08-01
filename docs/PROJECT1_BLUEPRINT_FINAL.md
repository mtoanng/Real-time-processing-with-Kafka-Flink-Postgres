# Active architecture blueprint

## Release architecture

Core:

```text
Taobao -> Python -> Kafka/Avro -> one Flink Table/SQL job submitted by Python
-> ClickHouse canonical raw/metrics/quality
-> ClickHouse one-minute offline feature history
-> Redis latest online user features with version guard and TTL
-> Redis active-cart serving projection
```

Approved, isolated extensions:

```text
HTTP -> same Kafka behavior contract
PostgreSQL product_catalog -> Debezium -> Kafka -> ClickHouse Kafka Engine
-> ClickHouse current catalog
API -> Redis carts/features + ClickHouse metrics/current-catalog query
```

Catalog is current-state replication, not temporal enrichment. It cannot alter
behavior identity, canonical raw, or source-category metrics.

Table/SQL owns behavior validation, bounded event-ID deduplication, native
watermarks, late evidence, windows, aggregation, one-minute user features and
latest cart transitions. Python owns submission, replay, Redis materialization
and verification. Kafka, ClickHouse and Redis integrations remain runtime
boundaries.

The current ML boundary is feature infrastructure only. Model training, Feast
and vector retrieval are not implemented or claimed.

Per-duplicate durable audit rows are outside the active target. Duplicate
counts are reconciled independently without adding a Python callback boundary.
