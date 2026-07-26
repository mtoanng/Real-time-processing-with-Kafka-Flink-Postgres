package com.taobao.behavior.processing;

import com.taobao.behavior.avro.UserBehaviorEvent;
import com.taobao.behavior.model.StreamQualityEvent;
import java.util.Optional;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class EventValidator extends ProcessFunction<UserBehaviorEvent, UserBehaviorEvent> {
    public static final OutputTag<StreamQualityEvent> INVALID_EVENTS =
            new OutputTag<StreamQualityEvent>("invalid-events") {};

    @Override
    public void processElement(
            UserBehaviorEvent event,
            Context context,
            Collector<UserBehaviorEvent> output) {
        Optional<String> invalidReason = invalidReason(event);
        if (invalidReason.isPresent()) {
            context.output(
                    INVALID_EVENTS,
                    StreamQualityEvent.fromEvent(
                            event,
                            StreamQualityEvent.QualityType.INVALID,
                            reasonCode(invalidReason.get()),
                            invalidReason.get(),
                            context.timerService().currentProcessingTime()));
            return;
        }
        output.collect(event);
    }

    public static Optional<String> invalidReason(UserBehaviorEvent event) {
        if (event == null) {
            return Optional.of("event must not be null");
        }
        if (event.getEventId() == null || event.getEventId().isBlank()) {
            return Optional.of("event_id must not be blank");
        }
        if (event.getUserId() <= 0 || event.getItemId() <= 0 || event.getCategoryId() <= 0) {
            return Optional.of("IDs must be positive");
        }
        if (event.getBehaviorType() == null) {
            return Optional.of("behavior_type must not be null");
        }
        if (event.getEventTimeMs() < 0) {
            return Optional.of("event_time_ms must be non-negative");
        }
        if (event.getSourceSequence() < 0) {
            return Optional.of("source_sequence must be non-negative");
        }
        if (event.getReplayRunId() == null || event.getReplayRunId().isBlank()) {
            return Optional.of("replay_run_id must not be blank");
        }
        return Optional.empty();
    }

    static String reasonCode(String reason) {
        if (reason.contains("event_id")) {
            return "INVALID_EVENT_ID";
        }
        if (reason.contains("IDs")) {
            return "INVALID_IDENTIFIER";
        }
        if (reason.contains("behavior_type")) {
            return "INVALID_BEHAVIOR_TYPE";
        }
        if (reason.contains("event_time")) {
            return "INVALID_EVENT_TIME";
        }
        if (reason.contains("source_sequence")) {
            return "INVALID_SOURCE_SEQUENCE";
        }
        if (reason.contains("replay_run_id")) {
            return "INVALID_REPLAY_RUN_ID";
        }
        return "INVALID_EVENT";
    }
}
