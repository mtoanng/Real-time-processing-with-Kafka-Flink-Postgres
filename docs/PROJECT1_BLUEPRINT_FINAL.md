# Active Project 1 blueprint

The approved authoring strategy is SQL/Python-first. "No Java" means the Data
Engineer does not author business pipelines in Java; JVM runtimes, drivers,
connector JARs and genuinely minimal platform adapters are allowed.

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
PostgreSQL product_catalog -> Debezium -> Kafka -> same Flink artifact
-> ClickHouse current catalog
API -> Redis carts + ClickHouse metrics/current-catalog query
```

Catalog is current-state replication, not temporal enrichment. It cannot alter
behavior identity, canonical raw, or source-category metrics.

SQL owns relational validation, table contracts, windows, aggregation and
catalog changelog handling. PyFlink DataStream owns event-ID state TTL,
duplicate/late evidence, watermarks and active-cart transitions. Connector JARs
and Kafka/ClickHouse/Redis adapters are platform boundaries.

The Java DataStream implementation is excluded from the active build and kept
only as rollback evidence. Remove it after a live repeated-replay and
checkpoint-restoration parity run.
