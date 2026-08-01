-- Canonical Taobao analytical storage.
-- Kafka/Flink transport and external writes are at least once. Canonical views
-- explicitly use FINAL; ordinary table reads are not guaranteed duplicate-free.

CREATE DATABASE IF NOT EXISTS taobao_behavior;

CREATE TABLE IF NOT EXISTS taobao_behavior.raw_behavior_events (
    event_id String,
    user_id UInt64,
    item_id UInt64,
    category_id UInt64,
    behavior_type LowCardinality(String),
    event_time DateTime64(3, 'UTC'),
    source_sequence UInt64,
    replay_run_id String,
    record_version UInt64,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(record_version)
PARTITION BY toYYYYMM(event_time)
ORDER BY (toDate(event_time), event_id);

CREATE VIEW IF NOT EXISTS taobao_behavior.raw_behavior_events_canonical AS
SELECT
    event_id,
    user_id,
    item_id,
    category_id,
    behavior_type,
    event_time,
    source_sequence,
    replay_run_id,
    ingested_at
FROM taobao_behavior.raw_behavior_events FINAL;

CREATE TABLE IF NOT EXISTS taobao_behavior.item_metrics_1m (
    window_start DateTime64(3, 'UTC'),
    item_id UInt64,
    source_category_id UInt64,
    pv_count UInt64,
    cart_count UInt64,
    fav_count UInt64,
    buy_count UInt64,
    unique_users UInt64,
    replay_run_id String,
    record_version UInt64,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(record_version)
PARTITION BY toYYYYMM(window_start)
ORDER BY (window_start, item_id, source_category_id);

CREATE VIEW IF NOT EXISTS taobao_behavior.item_metrics_1m_canonical AS
SELECT
    window_start,
    item_id,
    source_category_id,
    pv_count,
    cart_count,
    fav_count,
    buy_count,
    unique_users,
    replay_run_id,
    ingested_at
FROM taobao_behavior.item_metrics_1m FINAL;

CREATE TABLE IF NOT EXISTS taobao_behavior.stream_quality_events (
    quality_event_id String,
    quality_type LowCardinality(String),
    event_id Nullable(String),
    user_id Int64,
    item_id Int64,
    category_id Int64,
    behavior_type LowCardinality(String),
    event_time Int64,
    replay_run_id String,
    source_sequence Int64,
    reason_code LowCardinality(String),
    reason_message String,
    observed_at DateTime64(3, 'UTC'),
    record_version UInt64
)
ENGINE = ReplacingMergeTree(record_version)
PARTITION BY tuple()
ORDER BY quality_event_id;

CREATE VIEW IF NOT EXISTS taobao_behavior.stream_quality_events_canonical AS
SELECT
    quality_event_id,
    quality_type,
    event_id,
    user_id,
    item_id,
    category_id,
    behavior_type,
    event_time,
    replay_run_id,
    source_sequence,
    reason_code,
    reason_message,
    observed_at
FROM taobao_behavior.stream_quality_events FINAL;

-- SQL/PyFlink writes replay-safe JSON contracts to Kafka. These Kafka Engine
-- tables are platform adapters; canonical storage remains ReplacingMergeTree.
CREATE TABLE IF NOT EXISTS taobao_behavior.raw_behavior_events_queue (
    event_id String, user_id Int64, item_id Int64, category_id Int64,
    behavior_type String, event_time_ms Int64, source_sequence Int64,
    replay_run_id String, record_version Int64
) ENGINE = Kafka SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'taobao-raw-events',
    kafka_group_name = 'clickhouse-taobao-raw-v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS taobao_behavior.raw_behavior_events_ingest
TO taobao_behavior.raw_behavior_events AS
SELECT event_id, toUInt64(user_id) AS user_id, toUInt64(item_id) AS item_id,
    toUInt64(category_id) AS category_id, behavior_type,
    fromUnixTimestamp64Milli(event_time_ms) AS event_time,
    toUInt64(source_sequence) AS source_sequence, replay_run_id,
    toUInt64(record_version) AS record_version, now64(3) AS ingested_at
FROM taobao_behavior.raw_behavior_events_queue;

CREATE TABLE IF NOT EXISTS taobao_behavior.item_metrics_1m_queue (
    window_start String, item_id Int64, source_category_id Int64,
    pv_count Int64, cart_count Int64, fav_count Int64, buy_count Int64,
    unique_users Int64, replay_run_id String, record_version Int64
) ENGINE = Kafka SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'taobao-item-metrics-1m',
    kafka_group_name = 'clickhouse-taobao-metrics-v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS taobao_behavior.item_metrics_1m_ingest
TO taobao_behavior.item_metrics_1m AS
SELECT parseDateTime64BestEffort(window_start, 3, 'UTC') AS window_start,
    toUInt64(item_id) AS item_id, toUInt64(source_category_id) AS source_category_id,
    toUInt64(pv_count) AS pv_count, toUInt64(cart_count) AS cart_count,
    toUInt64(fav_count) AS fav_count, toUInt64(buy_count) AS buy_count,
    toUInt64(unique_users) AS unique_users, replay_run_id,
    toUInt64(record_version) AS record_version, now64(3) AS ingested_at
FROM taobao_behavior.item_metrics_1m_queue;

CREATE TABLE IF NOT EXISTS taobao_behavior.stream_quality_events_queue (
    quality_event_id String, quality_type String, event_id Nullable(String),
    user_id Int64, item_id Int64, category_id Int64, behavior_type String,
    event_time_ms Int64, replay_run_id String, source_sequence Int64,
    reason_code String, reason_message String, observed_at_ms Int64,
    record_version Int64
) ENGINE = Kafka SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'taobao-quality-events',
    kafka_group_name = 'clickhouse-taobao-quality-v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS taobao_behavior.stream_quality_events_ingest
TO taobao_behavior.stream_quality_events AS
SELECT quality_event_id, quality_type, event_id, user_id, item_id, category_id,
    behavior_type, event_time_ms AS event_time, replay_run_id, source_sequence, reason_code,
    reason_message, fromUnixTimestamp64Milli(observed_at_ms) AS observed_at,
    toUInt64(record_version) AS record_version
FROM taobao_behavior.stream_quality_events_queue;

CREATE TABLE IF NOT EXISTS taobao_behavior.product_catalog_current (
    product_id UInt64,
    category_id UInt64,
    product_name String,
    price Decimal(12, 2),
    is_active Bool,
    updated_at DateTime64(3, 'UTC'),
    catalog_version UInt64,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(catalog_version)
ORDER BY product_id;

CREATE VIEW IF NOT EXISTS taobao_behavior.product_catalog_current_canonical AS
SELECT product_id, category_id, product_name, price, is_active, updated_at,
    catalog_version, ingested_at
FROM taobao_behavior.product_catalog_current FINAL;

CREATE TABLE IF NOT EXISTS taobao_behavior.product_catalog_current_queue (
    product_id Int64, category_id Int64, product_name String, price Decimal(12, 2),
    is_active Bool, updated_at String, catalog_version Int64
) ENGINE = Kafka SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'product-catalog-cdc',
    kafka_group_name = 'clickhouse-taobao-catalog-v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS taobao_behavior.product_catalog_current_ingest
TO taobao_behavior.product_catalog_current AS
SELECT toUInt64(product_id) AS product_id, toUInt64(category_id) AS category_id,
    product_name, price, is_active,
    parseDateTime64BestEffort(updated_at, 3, 'UTC') AS updated_at,
    toUInt64(catalog_version) AS catalog_version, now64(3) AS ingested_at
FROM taobao_behavior.product_catalog_current_queue;
