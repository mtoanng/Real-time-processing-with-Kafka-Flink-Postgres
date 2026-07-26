package com.taobao.behavior.processing;

import com.taobao.behavior.avro.UserBehaviorEvent;
import com.taobao.behavior.model.StreamQualityEvent;
import java.time.Duration;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/** Keeps the first valid occurrence of an event_id within a bounded processing-time horizon. */
public class EventDeduplicator
        extends KeyedProcessFunction<String, UserBehaviorEvent, UserBehaviorEvent> {
    public static final OutputTag<StreamQualityEvent> DUPLICATE_EVENTS =
            new OutputTag<StreamQualityEvent>("duplicate-events") {};

    private final Duration retention;
    private transient ValueState<Long> seenAtProcessingTime;

    public EventDeduplicator(Duration retention) {
        this.retention = retention;
    }

    @Override
    public void open(Configuration parameters) {
        StateTtlConfig ttl =
                StateTtlConfig.newBuilder(retention)
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                        .cleanupFullSnapshot()
                        .build();
        ValueStateDescriptor<Long> descriptor =
                new ValueStateDescriptor<>("seen-event-id", Long.class);
        descriptor.enableTimeToLive(ttl);
        seenAtProcessingTime = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(
            UserBehaviorEvent event, Context context, Collector<UserBehaviorEvent> output)
            throws Exception {
        long now = context.timerService().currentProcessingTime();
        Long seenAt = seenAtProcessingTime.value();
        if (seenAt != null && now - seenAt < retention.toMillis()) {
            context.output(
                    DUPLICATE_EVENTS,
                    StreamQualityEvent.fromEvent(
                            event,
                            StreamQualityEvent.QualityType.DUPLICATE,
                            "DUPLICATE_WITHIN_RETENTION",
                            "event_id was already accepted within the configured retention horizon",
                            now));
            return;
        }
        seenAtProcessingTime.update(now);
        output.collect(event);
    }
}
