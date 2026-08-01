package com.taobao.behavior.processing;

import com.taobao.behavior.avro.UserBehaviorEvent;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class LateEventRouter extends ProcessFunction<UserBehaviorEvent, UserBehaviorEvent> {
    public static final OutputTag<UserBehaviorEvent> LATE_EVENTS =
            new OutputTag<UserBehaviorEvent>("late-events") {};

    @Override
    public void processElement(
            UserBehaviorEvent event,
            Context context,
            Collector<UserBehaviorEvent> output) {
        if (isLate(event.getEventTimeMs(), context.timerService().currentWatermark())) {
            context.output(LATE_EVENTS, event);
            return;
        }
        output.collect(event);
    }

    public static boolean isLate(long eventTimeMillis, long currentWatermark) {
        return currentWatermark != Long.MIN_VALUE && eventTimeMillis <= currentWatermark;
    }
}
