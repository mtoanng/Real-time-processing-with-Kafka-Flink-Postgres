package com.taobao.platform;

import java.io.Serializable;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.types.Row;

/**
 * Minimal JVM adapter for deterministic per-event watermark emission.
 *
 * <p>PyFlink 1.20 exposes built-in periodic strategies but not custom Python
 * WatermarkGenerator implementations. Business classification remains in
 * PyFlink; this adapter only supplies the missing runtime primitive.
 */
public final class ImmediateWatermarkStrategy {
    private static final int EVENT_TIME_FIELD = 5;

    private ImmediateWatermarkStrategy() {}

    public static WatermarkStrategy<Row> create(long maxOutOfOrdernessMillis) {
        if (maxOutOfOrdernessMillis < 0) {
            throw new IllegalArgumentException("maxOutOfOrdernessMillis must be non-negative");
        }
        return WatermarkStrategy
                .<Row>forGenerator(
                        ignored -> new ImmediateGenerator(maxOutOfOrdernessMillis))
                .withTimestampAssigner(
                        (row, previousTimestamp) ->
                                ((Number) row.getField(EVENT_TIME_FIELD)).longValue());
    }

    static final class ImmediateGenerator
            implements WatermarkGenerator<Row>, Serializable {
        private final long maxOutOfOrdernessMillis;
        private long maximumTimestamp = Long.MIN_VALUE + 1;

        ImmediateGenerator(long maxOutOfOrdernessMillis) {
            this.maxOutOfOrdernessMillis = maxOutOfOrdernessMillis;
        }

        @Override
        public void onEvent(Row event, long eventTimestamp, WatermarkOutput output) {
            maximumTimestamp = Math.max(maximumTimestamp, eventTimestamp);
            output.emitWatermark(
                    new Watermark(maximumTimestamp - maxOutOfOrdernessMillis - 1));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // Per-event emission is deliberate for deterministic bounded demos.
        }
    }
}
