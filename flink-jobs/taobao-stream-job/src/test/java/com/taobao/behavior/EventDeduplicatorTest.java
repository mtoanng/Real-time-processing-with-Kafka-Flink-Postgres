package com.taobao.behavior;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.taobao.behavior.avro.BehaviorType;
import com.taobao.behavior.avro.UserBehaviorEvent;
import com.taobao.behavior.model.StreamQualityEvent;
import com.taobao.behavior.processing.EventDeduplicator;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class EventDeduplicatorTest {
    @Test
    void acceptsFirstOccurrenceRoutesDuplicateAndAcceptsDifferentEventId() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<
                        String, UserBehaviorEvent, UserBehaviorEvent>
                harness = harness(Duration.ofHours(1))) {
            harness.open();
            UserBehaviorEvent first =
                    EventTestSupport.event(
                            1L, 10L, 100L, BehaviorType.pv, 1_000L, 7L, "run-a");
            UserBehaviorEvent duplicate =
                    EventTestSupport.event(
                            1L, 10L, 100L, BehaviorType.pv, 1_000L, 7L, "run-b");
            UserBehaviorEvent different =
                    EventTestSupport.event(
                            1L, 10L, 100L, BehaviorType.pv, 1_000L, 8L, "run-b");

            harness.processElement(new StreamRecord<>(first));
            harness.processElement(new StreamRecord<>(duplicate));
            harness.processElement(new StreamRecord<>(different));

            assertEquals(List.of("event-7", "event-8"), outputEventIds(harness.getOutput()));
            List<StreamRecord<StreamQualityEvent>> duplicates =
                    List.copyOf(harness.getSideOutput(EventDeduplicator.DUPLICATE_EVENTS));
            assertEquals(1, duplicates.size());
            assertEquals("DUPLICATE", duplicates.get(0).getValue().getQualityType());
            assertEquals(
                    "DUPLICATE_WITHIN_RETENTION",
                    duplicates.get(0).getValue().getReasonCode());
        }
    }

    @Test
    void acceptsEventAgainAfterStateTtlExpires() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<
                        String, UserBehaviorEvent, UserBehaviorEvent>
                harness = harness(Duration.ofHours(1))) {
            harness.open();
            UserBehaviorEvent event =
                    EventTestSupport.event(
                            1L, 10L, 100L, BehaviorType.pv, 1_000L, 7L);

            harness.setProcessingTime(0L);
            harness.processElement(new StreamRecord<>(event));
            harness.setProcessingTime(Duration.ofHours(1).toMillis() + 1L);
            harness.processElement(new StreamRecord<>(event));

            assertEquals(List.of("event-7", "event-7"), outputEventIds(harness.getOutput()));
            assertEquals(null, harness.getSideOutput(EventDeduplicator.DUPLICATE_EVENTS));
        }
    }

    @Test
    void checkpointRestoresSeenEventIds() throws Exception {
        UserBehaviorEvent event =
                EventTestSupport.event(
                        1L, 10L, 100L, BehaviorType.cart, 1_000L, 7L);
        OperatorSubtaskState snapshot;
        try (KeyedOneInputStreamOperatorTestHarness<
                        String, UserBehaviorEvent, UserBehaviorEvent>
                first = harness(Duration.ofHours(1))) {
            first.open();
            first.processElement(new StreamRecord<>(event));
            snapshot = first.snapshot(1L, 1L);
        }

        try (KeyedOneInputStreamOperatorTestHarness<
                        String, UserBehaviorEvent, UserBehaviorEvent>
                restored = harness(Duration.ofHours(1))) {
            restored.initializeState(snapshot);
            restored.open();
            restored.processElement(new StreamRecord<>(event));

            assertEquals(List.of(), outputEventIds(restored.getOutput()));
            assertEquals(
                    1,
                    restored.getSideOutput(EventDeduplicator.DUPLICATE_EVENTS).size());
        }
    }

    private static KeyedOneInputStreamOperatorTestHarness<
                    String, UserBehaviorEvent, UserBehaviorEvent>
            harness(Duration retention) throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new EventDeduplicator(retention)),
                event -> event.getEventId().toString(),
                Types.STRING);
    }

    private static List<String> outputEventIds(ConcurrentLinkedQueue<Object> output) {
        return output.stream()
                .filter(StreamRecord.class::isInstance)
                .map(StreamRecord.class::cast)
                .map(record -> ((UserBehaviorEvent) record.getValue()).getEventId().toString())
                .collect(Collectors.toList());
    }
}
