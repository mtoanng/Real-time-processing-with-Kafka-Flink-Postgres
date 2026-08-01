package com.taobao.behavior;

import com.taobao.behavior.avro.BehaviorType;
import com.taobao.behavior.avro.UserBehaviorEvent;

public final class EventTestSupport {
    private EventTestSupport() {}

    public static UserBehaviorEvent event(
            long userId,
            long itemId,
            long categoryId,
            BehaviorType behaviorType,
            long eventTimeMillis,
            long sourceSequence) {
        return event(
                userId,
                itemId,
                categoryId,
                behaviorType,
                eventTimeMillis,
                sourceSequence,
                "test-run");
    }

    public static UserBehaviorEvent event(
            long userId,
            long itemId,
            long categoryId,
            BehaviorType behaviorType,
            long eventTimeMillis,
            long sourceSequence,
            String replayRunId) {
        return UserBehaviorEvent.newBuilder()
                .setEventId("event-" + sourceSequence)
                .setUserId(userId)
                .setItemId(itemId)
                .setCategoryId(categoryId)
                .setBehaviorType(behaviorType)
                .setEventTimeMs(eventTimeMillis)
                .setSourceSequence(sourceSequence)
                .setReplayRunId(replayRunId)
                .build();
    }
}
