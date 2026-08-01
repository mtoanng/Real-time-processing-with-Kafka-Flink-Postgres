-- First relational stage. Invalid records are not discarded: the PyFlink
-- router converts them into durable quality events. Product catalog does not
-- enrich or alter this source-faithful behavior branch.
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
    CASE
        WHEN event_id IS NULL OR CHAR_LENGTH(TRIM(event_id)) = 0 THEN 'INVALID_EVENT_ID'
        WHEN user_id <= 0 OR item_id <= 0 OR category_id <= 0 THEN 'INVALID_IDENTIFIER'
        WHEN event_time_ms <= 0 THEN 'INVALID_EVENT_TIME'
        WHEN behavior_type NOT IN ('pv', 'cart', 'fav', 'buy')
            THEN 'INVALID_BEHAVIOR_TYPE'
        ELSE NULL
    END AS validation_reason
FROM behavior_events;
