-- Relational validation keeps the source payload intact and supplies stable
-- reason codes for the durable quality stream.
CREATE TEMPORARY VIEW classified_events AS
SELECT
    event_id,
    user_id,
    item_id,
    category_id,
    behavior_type,
    event_time_ms,
    source_sequence,
    replay_run_id,
    event_time,
    CASE
        WHEN event_id IS NULL OR CHAR_LENGTH(TRIM(event_id)) = 0 THEN 'INVALID_EVENT_ID'
        WHEN user_id <= 0 OR item_id <= 0 OR category_id <= 0 THEN 'INVALID_IDENTIFIER'
        WHEN event_time_ms <= 0 THEN 'INVALID_EVENT_TIME'
        WHEN behavior_type NOT IN ('pv', 'cart', 'fav', 'buy')
            THEN 'INVALID_BEHAVIOR_TYPE'
        ELSE NULL
    END AS validation_reason
FROM behavior_events;

CREATE TEMPORARY VIEW valid_events AS
SELECT
    event_id, user_id, item_id, category_id, behavior_type,
    event_time_ms, source_sequence, replay_run_id, event_time
FROM classified_events
WHERE validation_reason IS NULL;

-- This exact ROW_NUMBER pattern is Flink SQL's native streaming
-- deduplication. ORDER BY the rowtime attribute ASC keeps the first event_id;
-- table.exec.state.ttl bounds how long that identity is remembered.
CREATE TEMPORARY VIEW accepted_unique_events AS
SELECT
    event_id, user_id, item_id, category_id, behavior_type,
    event_time_ms, source_sequence, replay_run_id, event_time
FROM (
    SELECT
        event_id, user_id, item_id, category_id, behavior_type,
        event_time_ms, source_sequence, replay_run_id, event_time,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY event_time ASC
        ) AS row_num
    FROM valid_events
)
WHERE row_num = 1;

-- CURRENT_WATERMARK is evaluated by Flink's native Table runtime. A NULL
-- watermark means the stream has not observed enough data to classify the
-- event as late yet.
CREATE TEMPORARY VIEW on_time_events AS
SELECT *
FROM accepted_unique_events
WHERE CURRENT_WATERMARK(event_time) IS NULL
   OR event_time > CURRENT_WATERMARK(event_time);

CREATE TEMPORARY VIEW late_events AS
SELECT *
FROM accepted_unique_events
WHERE CURRENT_WATERMARK(event_time) IS NOT NULL
  AND event_time <= CURRENT_WATERMARK(event_time);

-- Closed one-minute snapshots are the first ML-ready feature contract. They
-- use the same accepted, on-time events as analytical metrics, preventing
-- replay duplicates and late records from changing an already closed feature.
-- ClickHouse stores history while Redis serves only the greatest version for
-- each user. No Python UDF runs in this aggregation path.
CREATE TEMPORARY VIEW user_features_1m AS
SELECT
    SHA256(CONCAT_WS('|', 'USER_FEATURES_1M', CAST(user_id AS STRING),
        DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss.SSS'))) AS feature_id,
    DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss.SSS') AS window_start,
    DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss.SSS') AS window_end,
    user_id,
    COUNT(*) AS event_count,
    COUNT(*) FILTER (WHERE behavior_type = 'pv') AS pv_count,
    COUNT(*) FILTER (WHERE behavior_type = 'cart') AS cart_count,
    COUNT(*) FILTER (WHERE behavior_type = 'fav') AS fav_count,
    COUNT(*) FILTER (WHERE behavior_type = 'buy') AS buy_count,
    COUNT(DISTINCT item_id) AS distinct_items,
    UNIX_TIMESTAMP(DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss')) * 1000
        AS feature_version,
    UNIX_TIMESTAMP(DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss')) * 1000
        AS record_version
FROM TABLE(
    TUMBLE(TABLE on_time_events, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end, user_id;

CREATE TEMPORARY VIEW quality_events AS
SELECT
    SHA256(CONCAT_WS('|', 'INVALID', COALESCE(event_id, ''), replay_run_id,
        CAST(source_sequence AS STRING), validation_reason)) AS quality_event_id,
    'INVALID' AS quality_type,
    event_id, user_id, item_id, category_id, behavior_type, event_time_ms,
    replay_run_id, source_sequence,
    validation_reason AS reason_code,
    'event failed the source-faithful semantic contract' AS reason_message,
    UNIX_TIMESTAMP() * 1000 AS observed_at_ms,
    UNIX_TIMESTAMP() * 1000 AS record_version
FROM classified_events
WHERE validation_reason IS NOT NULL
UNION ALL
SELECT
    SHA256(CONCAT_WS('|', 'LATE', COALESCE(event_id, ''), replay_run_id,
        CAST(source_sequence AS STRING), 'LATE_FOR_AGGREGATION')) AS quality_event_id,
    'LATE' AS quality_type,
    event_id, user_id, item_id, category_id, behavior_type, event_time_ms,
    replay_run_id, source_sequence,
    'LATE_FOR_AGGREGATION' AS reason_code,
    'event_time is at or behind the current watermark' AS reason_message,
    UNIX_TIMESTAMP() * 1000 AS observed_at_ms,
    UNIX_TIMESTAMP() * 1000 AS record_version
FROM late_events;

-- Active cart is a serving projection. Streaming Top-1 keeps only the newest
-- cart/buy transition for each user-item key, so an older cart cannot recreate
-- an item after a newer buy. added_at_ms denotes the latest cart action.
CREATE TEMPORARY VIEW latest_cart_state AS
SELECT
    behavior_type, user_id, item_id, category_id,
    event_time_ms, source_sequence
FROM (
    SELECT
        behavior_type, user_id, item_id, category_id,
        event_time_ms, source_sequence,
        ROW_NUMBER() OVER (
            PARTITION BY user_id, item_id
            ORDER BY event_time DESC, source_sequence DESC
        ) AS row_num
    FROM accepted_unique_events
    WHERE behavior_type IN ('cart', 'buy')
)
WHERE row_num = 1;

CREATE TEMPORARY VIEW cart_mutations AS
SELECT
    CASE behavior_type WHEN 'cart' THEN 'UPSERT' ELSE 'DELETE' END AS operation,
    user_id,
    item_id,
    category_id,
    CASE behavior_type WHEN 'cart' THEN event_time_ms ELSE CAST(0 AS BIGINT) END
        AS added_at_ms,
    event_time_ms AS last_updated_at_ms
FROM latest_cart_state;
