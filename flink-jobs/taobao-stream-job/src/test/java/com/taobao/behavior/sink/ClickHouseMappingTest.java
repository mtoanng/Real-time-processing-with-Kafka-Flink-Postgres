package com.taobao.behavior.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.taobao.behavior.EventTestSupport;
import com.taobao.behavior.avro.BehaviorType;
import com.taobao.behavior.model.BehaviorAlert;
import com.taobao.behavior.model.ItemMetrics1m;
import com.taobao.behavior.model.StreamQualityEvent;
import java.time.ZonedDateTime;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ClickHouseMappingTest {
    @Test
    void mapsRawEventToTheDdlColumns() {
        Map<String, Object> row =
                ClickHouseRowMapper.rawValues(
                        EventTestSupport.event(
                                100L, 500L, 50L, BehaviorType.cart, 1511658000000L, 7L));

        assertEquals("event-7", row.get("event_id"));
        assertEquals(100L, row.get("user_id"));
        assertEquals(500L, row.get("item_id"));
        assertEquals(50L, row.get("category_id"));
        assertEquals("cart", row.get("behavior_type"));
        assertEquals("test-run", row.get("replay_run_id"));
        assertEquals(7L, row.get("source_sequence"));
        assertEquals(7L, row.get("record_version"));
        assertEquals(
                "2017-11-26T01:00Z",
                ((ZonedDateTime) row.get("event_time")).withNano(0).toString());
    }

    @Test
    void mapsMetricsToTheDdlColumns() {
        ItemMetrics1m metrics =
                new ItemMetrics1m(1511658000000L, 500L, 50L, 0L, 1L, 0L, 1L, 1L, "run-3");

        Map<String, Object> row = ClickHouseRowMapper.itemMetricsValues(metrics);

        assertEquals(10, row.size());
        assertEquals(1L, row.get("cart_count"));
        assertEquals(1L, row.get("buy_count"));
        assertEquals(1L, row.get("unique_users"));
        assertEquals(50L, row.get("source_category_id"));
        assertEquals("run-3", row.get("replay_run_id"));
        assertEquals(1_511_658_000_000L, row.get("record_version"));
    }

    @Test
    void mapsAuditAndAlertContextToDurableColumns() {
        StreamQualityEvent quality = StreamQualityEvent.fromEvent(
                EventTestSupport.event(
                        100L, 500L, 50L, BehaviorType.cart, 1_511_658_000_000L, 7L, "run-3"),
                StreamQualityEvent.QualityType.LATE,
                "LATE_FOR_AGGREGATION",
                "event is late",
                1_511_658_060_000L);
        Map<String, Object> auditRow = ClickHouseRowMapper.qualityValues(quality);

        assertEquals("LATE", auditRow.get("quality_type"));
        assertEquals("LATE_FOR_AGGREGATION", auditRow.get("reason_code"));
        assertEquals(1_511_658_060_000L, auditRow.get("record_version"));
        assertEquals(1_511_658_000_000L, auditRow.get("event_time"));

        BehaviorAlert alert = new BehaviorAlert(
                "cart-abandonment:event-7:1:1511658060000",
                "run-3",
                100L,
                500L,
                50L,
                1_511_658_000_000L,
                1_511_658_060_000L,
                "cart_abandonment",
                1L);
        Map<String, Object> alertRow = ClickHouseRowMapper.alertValues(alert);

        assertEquals("CART_ABANDONMENT", alertRow.get("reason_code"));
        assertEquals("run-3", alertRow.get("replay_run_id"));
        assertEquals(1_511_658_060_000L, alertRow.get("record_version"));
    }
}
