package com.taobao.platform;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.junit.jupiter.api.Test;

class ImmediateWatermarkStrategyTest {
    @Test
    void createsRuntimeStrategyForNonNegativeBound() {
        assertNotNull(ImmediateWatermarkStrategy.create(5_000L));
    }

    @Test
    void rejectsNegativeBound() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ImmediateWatermarkStrategy.create(-1L));
    }

    @Test
    void emitsMonotonicWatermarkAfterEveryEvent() {
        ImmediateWatermarkStrategy.ImmediateGenerator generator =
                new ImmediateWatermarkStrategy.ImmediateGenerator(5_000L);
        RecordingOutput output = new RecordingOutput();

        generator.onEvent(null, 10_000L, output);
        generator.onEvent(null, 7_000L, output);
        generator.onEvent(null, 12_000L, output);

        assertEquals(List.of(4_999L, 4_999L, 6_999L), output.timestamps);
    }

    private static final class RecordingOutput implements WatermarkOutput {
        private final List<Long> timestamps = new ArrayList<>();

        @Override
        public void emitWatermark(Watermark watermark) {
            timestamps.add(watermark.getTimestamp());
        }

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {}
    }
}
