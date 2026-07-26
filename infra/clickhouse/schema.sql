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

-- Deprecated legacy CDC/timer output. It is not part of core quality
-- accounting or the current release target.
CREATE TABLE IF NOT EXISTS taobao_behavior.behavior_alerts (
    event_id String,
    replay_run_id String,
    user_id UInt64,
    item_id UInt64,
    category_id UInt64,
    event_time DateTime64(3, 'UTC'),
    alert_time DateTime64(3, 'UTC'),
    rule_id String,
    rule_version UInt64,
    reason_code LowCardinality(String),
    reason_message String,
    record_version UInt64,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(record_version)
PARTITION BY toYYYYMM(alert_time)
ORDER BY (event_id, rule_id, rule_version);

CREATE VIEW IF NOT EXISTS taobao_behavior.behavior_alerts_canonical AS
SELECT * EXCEPT record_version
FROM taobao_behavior.behavior_alerts FINAL;
