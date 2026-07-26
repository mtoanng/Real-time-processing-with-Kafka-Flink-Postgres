package com.taobao.behavior.sink;

import com.taobao.behavior.avro.UserBehaviorEvent;
import com.taobao.behavior.model.BehaviorAlert;
import com.taobao.behavior.model.ItemMetrics1m;
import com.taobao.behavior.model.StreamQualityEvent;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

final class ClickHouseRowMapper {
    private ClickHouseRowMapper() {}

    static Map<String, Object> rawValues(UserBehaviorEvent event) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("event_id", event.getEventId().toString());
        row.put("user_id", event.getUserId());
        row.put("item_id", event.getItemId());
        row.put("category_id", event.getCategoryId());
        row.put("behavior_type", event.getBehaviorType().toString());
        row.put("event_time", utcTime(event.getEventTimeMs()));
        row.put("replay_run_id", event.getReplayRunId().toString());
        row.put("source_sequence", event.getSourceSequence());
        row.put("record_version", event.getSourceSequence());
        return row;
    }

    static Map<String, Object> itemMetricsValues(ItemMetrics1m metrics) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("window_start", utcTime(metrics.getWindowStart()));
        row.put("item_id", metrics.getItemId());
        row.put("source_category_id", metrics.getSourceCategoryId());
        row.put("pv_count", metrics.getPvCount());
        row.put("cart_count", metrics.getCartCount());
        row.put("fav_count", metrics.getFavCount());
        row.put("buy_count", metrics.getBuyCount());
        row.put("unique_users", metrics.getUniqueUsers());
        row.put("replay_run_id", metrics.getReplayRunId());
        row.put("record_version", metrics.getWindowStart());
        return row;
    }

    static Map<String, Object> qualityValues(StreamQualityEvent event) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("quality_event_id", event.getQualityEventId());
        row.put("quality_type", event.getQualityType());
        row.put("event_id", event.getEventId());
        row.put("user_id", event.getUserId());
        row.put("item_id", event.getItemId());
        row.put("category_id", event.getCategoryId());
        row.put("behavior_type", event.getBehaviorType());
        row.put("event_time", event.getEventTime());
        row.put("replay_run_id", event.getReplayRunId());
        row.put("source_sequence", event.getSourceSequence());
        row.put("reason_code", event.getReasonCode());
        row.put("reason_message", event.getReasonMessage());
        row.put("observed_at", utcTime(event.getObservedAt()));
        row.put("record_version", event.getObservedAt());
        return row;
    }

    static Map<String, Object> alertValues(BehaviorAlert alert) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("event_id", alert.getEventId());
        row.put("replay_run_id", alert.getReplayRunId());
        row.put("user_id", alert.getUserId());
        row.put("item_id", alert.getItemId());
        row.put("category_id", alert.getCategoryId());
        row.put("event_time", utcTime(alert.getCartEventTimeMs()));
        row.put("alert_time", utcTime(alert.getAlertTimeMs()));
        row.put("rule_id", alert.getRuleId());
        row.put("rule_version", alert.getRuleVersion());
        row.put("reason_code", alert.getReasonCode());
        row.put("reason_message", alert.getReasonMessage());
        row.put("record_version", alert.getAlertTimeMs());
        return row;
    }

    private static ZonedDateTime utcTime(long epochMillis) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.UTC);
    }
}
