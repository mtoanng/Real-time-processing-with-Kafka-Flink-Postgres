package com.taobao.behavior.processing;

import com.taobao.behavior.avro.UserBehaviorEvent;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

public class ImmediateBoundedOutOfOrdernessGenerator
        implements WatermarkGenerator<UserBehaviorEvent> {
    private final long maxOutOfOrdernessMillis;
    private long maximumTimestamp;

    public ImmediateBoundedOutOfOrdernessGenerator(long maxOutOfOrdernessMillis) {
        if (maxOutOfOrdernessMillis < 0) {
            throw new IllegalArgumentException("maxOutOfOrdernessMillis must be non-negative");
        }
        this.maxOutOfOrdernessMillis = maxOutOfOrdernessMillis;
        this.maximumTimestamp = Long.MIN_VALUE + maxOutOfOrdernessMillis + 1;
    }

    @Override
    public void onEvent(UserBehaviorEvent event, long eventTimestamp, WatermarkOutput output) {
        maximumTimestamp = Math.max(maximumTimestamp, eventTimestamp);
        output.emitWatermark(new Watermark(maximumTimestamp - maxOutOfOrdernessMillis - 1));
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // Watermarks are emitted on every event to make fixture behavior deterministic.
    }
}
