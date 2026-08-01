package com.taobao.behavior.aggregation;

import com.taobao.behavior.avro.UserBehaviorEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

public class ItemMetricsAggregator
        implements AggregateFunction<UserBehaviorEvent, ItemMetricsAccumulator, ItemMetricsAccumulator> {

    @Override
    public ItemMetricsAccumulator createAccumulator() {
        return new ItemMetricsAccumulator();
    }

    @Override
    public ItemMetricsAccumulator add(UserBehaviorEvent event, ItemMetricsAccumulator accumulator) {
        String replayRunId = event.getReplayRunId().toString();
        if (event.getSourceSequence() > accumulator.lineageSourceSequence
                || (event.getSourceSequence() == accumulator.lineageSourceSequence
                        && (accumulator.replayRunId == null
                                || replayRunId.compareTo(accumulator.replayRunId) > 0))) {
            accumulator.replayRunId = replayRunId;
            accumulator.lineageSourceSequence = event.getSourceSequence();
        }

        switch (event.getBehaviorType()) {
            case pv:
                accumulator.pvCount++;
                break;
            case cart:
                accumulator.cartCount++;
                break;
            case fav:
                accumulator.favCount++;
                break;
            case buy:
                accumulator.buyCount++;
                break;
            default:
                throw new IllegalArgumentException("unsupported behavior type");
        }
        accumulator.userIds.add(event.getUserId());
        return accumulator;
    }

    @Override
    public ItemMetricsAccumulator getResult(ItemMetricsAccumulator accumulator) {
        return accumulator;
    }

    @Override
    public ItemMetricsAccumulator merge(
            ItemMetricsAccumulator left, ItemMetricsAccumulator right) {
        if (right.lineageSourceSequence > left.lineageSourceSequence
                || (right.lineageSourceSequence == left.lineageSourceSequence
                        && right.replayRunId != null
                        && (left.replayRunId == null
                                || right.replayRunId.compareTo(left.replayRunId) > 0))) {
            left.replayRunId = right.replayRunId;
            left.lineageSourceSequence = right.lineageSourceSequence;
        }
        left.pvCount += right.pvCount;
        left.cartCount += right.cartCount;
        left.favCount += right.favCount;
        left.buyCount += right.buyCount;
        left.userIds.addAll(right.userIds);
        return left;
    }
}
