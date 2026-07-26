-- Canonical verification query pack for a fresh bounded-demo database.
-- Replaying the same fixture under another run ID must not change the first two
-- query results. Lineage is metadata and is never a canonical filter.

SELECT count() AS canonical_raw_count
FROM taobao_behavior.raw_behavior_events_canonical;

SELECT
    window_start,
    item_id,
    source_category_id,
    pv_count,
    cart_count,
    fav_count,
    buy_count,
    unique_users
FROM taobao_behavior.item_metrics_1m_canonical
ORDER BY window_start, item_id, source_category_id;

SELECT quality_type, count() AS quality_count
FROM taobao_behavior.stream_quality_events_canonical
GROUP BY quality_type
ORDER BY quality_type;

SELECT
    event_id,
    groupUniqArray(replay_run_id) AS observed_replay_runs
FROM taobao_behavior.stream_quality_events_canonical
WHERE quality_type = 'DUPLICATE'
GROUP BY event_id
ORDER BY event_id
LIMIT 20;

-- Physical minus canonical rows exposes unmerged at-least-once writes.
SELECT
    (SELECT count() FROM taobao_behavior.raw_behavior_events)
      -
    (SELECT count() FROM taobao_behavior.raw_behavior_events_canonical)
      AS raw_transport_rows_above_canonical;
