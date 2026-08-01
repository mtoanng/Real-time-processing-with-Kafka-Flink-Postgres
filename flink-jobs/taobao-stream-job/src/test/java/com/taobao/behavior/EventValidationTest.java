package com.taobao.behavior;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.taobao.behavior.avro.BehaviorType;
import com.taobao.behavior.avro.UserBehaviorEvent;
import com.taobao.behavior.processing.EventValidator;
import org.junit.jupiter.api.Test;

class EventValidationTest {
    @Test
    void validEventPassesValidation() {
        UserBehaviorEvent event = EventTestSupport.event(
                42L, 500L, 50L, BehaviorType.pv, 1_511_658_000_000L, 0L);

        assertTrue(EventValidator.invalidReason(event).isEmpty());
    }

    @Test
    void nonPositiveIdIsRejectedExplicitly() {
        UserBehaviorEvent event = EventTestSupport.event(
                0L, 500L, 50L, BehaviorType.pv, 1_511_658_000_000L, 0L);

        assertEquals("IDs must be positive", EventValidator.invalidReason(event).orElseThrow());
    }

    @Test
    void negativeTimestampIsRejectedExplicitly() {
        UserBehaviorEvent event = EventTestSupport.event(
                42L, 500L, 50L, BehaviorType.pv, -1L, 0L);

        assertEquals(
                "event_time_ms must be non-negative",
                EventValidator.invalidReason(event).orElseThrow());
    }
}
