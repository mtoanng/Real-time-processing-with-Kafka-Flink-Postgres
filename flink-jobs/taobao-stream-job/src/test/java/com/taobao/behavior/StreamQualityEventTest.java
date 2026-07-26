package com.taobao.behavior;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.taobao.behavior.avro.BehaviorType;
import com.taobao.behavior.avro.UserBehaviorEvent;
import com.taobao.behavior.model.StreamQualityEvent;
import org.junit.jupiter.api.Test;

class StreamQualityEventTest {
    @Test
    void qualityIdentityIsDeterministicPerClassificationAndReplayAttempt() {
        UserBehaviorEvent event =
                EventTestSupport.event(
                        1L, 10L, 100L, BehaviorType.pv, 1_000L, 7L, "run-a");
        StreamQualityEvent first =
                StreamQualityEvent.fromEvent(
                        event,
                        StreamQualityEvent.QualityType.LATE,
                        "LATE_FOR_AGGREGATION",
                        "late",
                        10L);
        StreamQualityEvent retry =
                StreamQualityEvent.fromEvent(
                        event,
                        StreamQualityEvent.QualityType.LATE,
                        "LATE_FOR_AGGREGATION",
                        "late",
                        20L);
        UserBehaviorEvent otherRun =
                EventTestSupport.event(
                        1L, 10L, 100L, BehaviorType.pv, 1_000L, 7L, "run-b");
        StreamQualityEvent otherAttempt =
                StreamQualityEvent.fromEvent(
                        otherRun,
                        StreamQualityEvent.QualityType.DUPLICATE,
                        "DUPLICATE_WITHIN_RETENTION",
                        "duplicate",
                        20L);

        assertEquals(first.getQualityEventId(), retry.getQualityEventId());
        assertNotEquals(first.getQualityEventId(), otherAttempt.getQualityEventId());
        assertEquals(10L, first.getObservedAt());
    }

    @Test
    void genuinelyMissingEventIdRemainsNull() {
        StreamQualityEvent quality =
                StreamQualityEvent.fromEvent(
                        null,
                        StreamQualityEvent.QualityType.INVALID,
                        "INVALID_EVENT",
                        "missing event",
                        10L);

        assertNull(quality.getEventId());
        assertEquals("INVALID", quality.getQualityType());
    }
}
