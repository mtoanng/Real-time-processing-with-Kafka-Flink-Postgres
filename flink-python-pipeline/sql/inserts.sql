-- Each named statement is attached to the same StreamExecutionEnvironment by
-- StatementSet.attach_as_datastream().
-- name: raw
INSERT INTO raw_events_out
SELECT
    event_id,
    user_id,
    item_id,
    category_id,
    behavior_type,
    event_time_ms,
    source_sequence,
    replay_run_id,
    event_time_ms AS record_version
FROM accepted_unique_events
;

-- name: metrics
INSERT INTO metrics_out
SELECT
    DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss.SSS'),
    item_id,
    category_id AS source_category_id,
    COUNT(*) FILTER (WHERE behavior_type = 'pv'),
    COUNT(*) FILTER (WHERE behavior_type = 'cart'),
    COUNT(*) FILTER (WHERE behavior_type = 'fav'),
    COUNT(*) FILTER (WHERE behavior_type = 'buy'),
    COUNT(DISTINCT user_id),
    MIN(replay_run_id),
    UNIX_TIMESTAMP(DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss')) * 1000
FROM TABLE(
    TUMBLE(TABLE on_time_events, DESCRIPTOR(rowtime), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end, item_id, category_id
;

-- name: quality
INSERT INTO quality_events_out
SELECT * FROM quality_events
;

-- name: cart
INSERT INTO cart_mutations_out
SELECT * FROM cart_mutations
;
