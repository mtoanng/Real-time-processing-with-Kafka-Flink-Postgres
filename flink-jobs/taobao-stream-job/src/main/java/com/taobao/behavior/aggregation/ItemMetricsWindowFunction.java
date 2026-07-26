package com.taobao.behavior.aggregation;

import com.taobao.behavior.model.ItemMetrics1m;
import com.taobao.behavior.model.ItemCategoryKey;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class ItemMetricsWindowFunction
        extends ProcessWindowFunction<
                ItemMetricsAccumulator, ItemMetrics1m, ItemCategoryKey, TimeWindow> {

    @Override
    public void process(
            ItemCategoryKey key,
            Context context,
            Iterable<ItemMetricsAccumulator> accumulators,
            Collector<ItemMetrics1m> output) {
        output.collect(
                toMetrics(
                        key.getItemId(),
                        key.getSourceCategoryId(),
                        context.window().getStart(),
                        accumulators.iterator().next()));
    }

    public static ItemMetrics1m toMetrics(
            long itemId,
            long sourceCategoryId,
            long windowStart,
            ItemMetricsAccumulator accumulator) {
        return new ItemMetrics1m(
                windowStart,
                itemId,
                sourceCategoryId,
                accumulator.pvCount,
                accumulator.cartCount,
                accumulator.favCount,
                accumulator.buyCount,
                accumulator.userIds.size(),
                accumulator.replayRunId);
    }
}
