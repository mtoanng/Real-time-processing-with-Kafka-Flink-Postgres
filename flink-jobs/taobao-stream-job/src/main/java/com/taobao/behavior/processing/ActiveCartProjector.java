package com.taobao.behavior.processing;

import com.taobao.behavior.avro.UserBehaviorEvent;
import com.taobao.behavior.model.CartItemState;
import com.taobao.behavior.model.CartMutation;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ActiveCartProjector extends KeyedProcessFunction<Long, UserBehaviorEvent, CartMutation> {
    private transient MapState<Long, CartItemState> itemStates;

    @Override
    public void open(Configuration parameters) {
        itemStates = getRuntimeContext().getMapState(new MapStateDescriptor<>(
                "user-active-cart-items", Long.class, CartItemState.class));
    }

    @Override
    public void processElement(UserBehaviorEvent event, Context context, Collector<CartMutation> out)
            throws Exception {
        CartItemState current = itemStates.get(event.getItemId());
        CartItemState next = nextState(current, event);
        if (next == null) {
            return;
        }
        itemStates.put(event.getItemId(), next);
        out.collect(mutation(event, next));
    }

    public static CartItemState nextState(CartItemState current, UserBehaviorEvent event) {
        String behavior = event.getBehaviorType().toString();
        if ((!"cart".equals(behavior) && !"buy".equals(behavior)) || isStale(current, event)) {
            return null;
        }
        boolean active = "cart".equals(behavior);
        long eventTime = event.getEventTimeMs();
        long addedAt =
                active && (current == null || !current.isActive())
                        ? eventTime
                        : current == null ? eventTime : current.getAddedAtMs();
        return new CartItemState(
                active,
                event.getCategoryId(),
                addedAt,
                eventTime,
                eventTime,
                event.getSourceSequence());
    }

    public static CartMutation mutation(UserBehaviorEvent event, CartItemState state) {
        return new CartMutation(
                state.isActive() ? CartMutation.Type.UPSERT : CartMutation.Type.DELETE,
                event.getUserId(),
                event.getItemId(),
                state.getCategoryId(),
                state.getAddedAtMs(),
                state.getLastUpdatedAtMs());
    }

    private static boolean isStale(CartItemState current, UserBehaviorEvent event) {
        return current != null
                && (event.getEventTimeMs() < current.getLastEventTimeMs()
                        || (event.getEventTimeMs() == current.getLastEventTimeMs()
                                && event.getSourceSequence() <= current.getLastSourceSequence()));
    }
}
