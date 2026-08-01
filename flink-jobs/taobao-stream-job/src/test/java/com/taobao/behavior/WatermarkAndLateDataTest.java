package com.taobao.behavior;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.taobao.behavior.avro.BehaviorType;
import com.taobao.behavior.avro.UserBehaviorEvent;
import com.taobao.behavior.processing.ImmediateBoundedOutOfOrdernessGenerator;
import com.taobao.behavior.processing.LateEventRouter;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.junit.jupiter.api.Test;

class WatermarkAndLateDataTest {
    @Test
    void fixtureBackwardJumpIsLateAfterFiveSecondWatermark() {
        long base = 1_511_658_000_000L;
        UserBehaviorEvent newest =
                EventTestSupport.event(104L, 504L, 52L, BehaviorType.pv, base + 30_000L, 5L);
        UserBehaviorEvent backward =
                EventTestSupport.event(105L, 505L, 53L, BehaviorType.pv, base + 15_000L, 6L);
        CapturingWatermarkOutput output = new CapturingWatermarkOutput();
        ImmediateBoundedOutOfOrdernessGenerator generator =
                new ImmediateBoundedOutOfOrdernessGenerator(5_000L);

        generator.onEvent(newest, newest.getEventTimeMs(), output);
        long watermark = output.watermarks.get(0);

        assertEquals(base + 24_999L, watermark);
        assertTrue(LateEventRouter.isLate(backward.getEventTimeMs(), watermark));
        assertFalse(LateEventRouter.isLate(newest.getEventTimeMs(), watermark));
        assertFalse(LateEventRouter.isLate(backward.getEventTimeMs(), Long.MIN_VALUE));
    }

    private static class CapturingWatermarkOutput implements WatermarkOutput {
        private final List<Long> watermarks = new ArrayList<>();

        @Override
        public void emitWatermark(Watermark watermark) {
            watermarks.add(watermark.getTimestamp());
        }

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {}
    }
}
