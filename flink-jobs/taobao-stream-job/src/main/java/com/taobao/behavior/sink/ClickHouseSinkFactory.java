package com.taobao.behavior.sink;

import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseFormat;
import com.taobao.behavior.avro.UserBehaviorEvent;
import com.taobao.behavior.model.ItemMetrics1m;
import com.taobao.behavior.model.ProductCatalogRecord;
import com.taobao.behavior.model.StreamQualityEvent;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.clickhouse.convertor.ClickHouseConvertor;
import org.apache.flink.connector.clickhouse.convertor.ColumnBinding;
import org.apache.flink.connector.clickhouse.convertor.DataMapper;
import org.apache.flink.connector.clickhouse.sink.ClickHouseAsyncSink;
import org.apache.flink.connector.clickhouse.sink.ClickHouseClientConfig;

public final class ClickHouseSinkFactory {
    private ClickHouseSinkFactory() {}

    public static Sink<UserBehaviorEvent> createRawSink(
            String endpoint, String user, String password, String database, String table) {
        return createSink(
                endpoint,
                user,
                password,
                database,
                table,
                UserBehaviorEvent.class,
                new RawBehaviorEventMapper(),
                100,
                3,
                1_000);
    }

    public static Sink<ItemMetrics1m> createItemMetricsSink(
            String endpoint, String user, String password, String database, String table) {
        return createSink(
                endpoint,
                user,
                password,
                database,
                table,
                ItemMetrics1m.class,
                new ItemMetricsMapper(),
                1_000,
                2,
                10_000);
    }

    public static Sink<StreamQualityEvent> createQualitySink(
            String endpoint,
            String user,
            String password,
            String database,
            String table) {
        return createSink(
                endpoint,
                user,
                password,
                database,
                table,
                StreamQualityEvent.class,
                new QualityEventMapper(),
                100,
                2,
                1_000);
    }

    public static Sink<ProductCatalogRecord> createProductCatalogSink(
            String endpoint, String user, String password, String database, String table) {
        return createSink(
                endpoint,
                user,
                password,
                database,
                table,
                ProductCatalogRecord.class,
                new ProductCatalogMapper(),
                100,
                2,
                1_000);
    }

    private static <T> Sink<T> createSink(
            String endpoint,
            String user,
            String password,
            String database,
            String table,
            Class<T> elementType,
            DataMapper<T> mapper,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests) {
        ClickHouseClientConfig config =
                new ClickHouseClientConfig(
                        endpoint,
                        user,
                        password,
                        database,
                        table,
                        Map.of("connection_timeout", "5000", "socket_timeout", "10000"),
                        Map.of(),
                        false);
        ClickHouseConvertor<T> convertor = new ClickHouseConvertor<>(elementType, mapper);

        return ClickHouseAsyncSink.<T>builder()
                .setElementConverter(convertor)
                .setClickHouseClientConfig(config)
                .setClickHouseFormat(ClickHouseFormat.JSONEachRow)
                .setMaxBatchSize(maxBatchSize)
                .setMaxInFlightRequests(maxInFlightRequests)
                .setMaxBufferedRequests(maxBufferedRequests)
                .setMaxBatchSizeInBytes(5_242_880L)
                .setMaxTimeInBufferMS(1_000L)
                .setMaxRecordSizeInBytes(1_048_576L)
                .build();
    }

    private static final class RawBehaviorEventMapper extends DataMapper<UserBehaviorEvent> {
        @Override
        public void toMap(UserBehaviorEvent event, Map<String, Object> out) {
            out.putAll(ClickHouseRowMapper.rawValues(event));
        }

        @Override
        public List<ColumnBinding> bindings() {
            return List.of(
                    ColumnBinding.scalar("event_id", "event_id", ClickHouseDataType.String),
                    ColumnBinding.scalar("user_id", "user_id", ClickHouseDataType.UInt64),
                    ColumnBinding.scalar("item_id", "item_id", ClickHouseDataType.UInt64),
                    ColumnBinding.scalar("category_id", "category_id", ClickHouseDataType.UInt64),
                    ColumnBinding.scalar("behavior_type", "behavior_type", ClickHouseDataType.String),
                    ColumnBinding.dateTime64("event_time", "event_time", 3),
                    ColumnBinding.scalar("replay_run_id", "replay_run_id", ClickHouseDataType.String),
                    ColumnBinding.scalar("source_sequence", "source_sequence", ClickHouseDataType.UInt64),
                    ColumnBinding.scalar("record_version", "record_version", ClickHouseDataType.UInt64));
        }
    }

    private static final class ItemMetricsMapper extends DataMapper<ItemMetrics1m> {
        @Override
        public void toMap(ItemMetrics1m metrics, Map<String, Object> out) {
            out.putAll(ClickHouseRowMapper.itemMetricsValues(metrics));
        }

        @Override
        public List<ColumnBinding> bindings() {
            return List.of(
                    ColumnBinding.dateTime64("window_start", "window_start", 3),
                    ColumnBinding.scalar("item_id", "item_id", ClickHouseDataType.UInt64),
                    ColumnBinding.scalar(
                            "source_category_id",
                            "source_category_id",
                            ClickHouseDataType.UInt64),
                    ColumnBinding.scalar("pv_count", "pv_count", ClickHouseDataType.UInt64),
                    ColumnBinding.scalar("cart_count", "cart_count", ClickHouseDataType.UInt64),
                    ColumnBinding.scalar("fav_count", "fav_count", ClickHouseDataType.UInt64),
                    ColumnBinding.scalar("buy_count", "buy_count", ClickHouseDataType.UInt64),
                    ColumnBinding.scalar("unique_users", "unique_users", ClickHouseDataType.UInt64),
                    ColumnBinding.scalar("replay_run_id", "replay_run_id", ClickHouseDataType.String),
                    ColumnBinding.scalar("record_version", "record_version", ClickHouseDataType.UInt64));
        }
    }

    private static final class QualityEventMapper extends DataMapper<StreamQualityEvent> {
        @Override
        public void toMap(StreamQualityEvent event, Map<String, Object> out) {
            out.putAll(ClickHouseRowMapper.qualityValues(event));
        }

        @Override
        public List<ColumnBinding> bindings() {
            return List.of(
                    ColumnBinding.scalar(
                            "quality_event_id", "quality_event_id", ClickHouseDataType.String),
                    ColumnBinding.scalar(
                            "quality_type", "quality_type", ClickHouseDataType.String),
                    ColumnBinding.scalar("event_id", "event_id", ClickHouseDataType.String),
                    ColumnBinding.scalar("user_id", "user_id", ClickHouseDataType.Int64),
                    ColumnBinding.scalar("item_id", "item_id", ClickHouseDataType.Int64),
                    ColumnBinding.scalar("category_id", "category_id", ClickHouseDataType.Int64),
                    ColumnBinding.scalar(
                            "behavior_type", "behavior_type", ClickHouseDataType.String),
                    ColumnBinding.scalar("event_time", "event_time", ClickHouseDataType.Int64),
                    ColumnBinding.scalar(
                            "replay_run_id", "replay_run_id", ClickHouseDataType.String),
                    ColumnBinding.scalar("source_sequence", "source_sequence", ClickHouseDataType.Int64),
                    ColumnBinding.scalar("reason_code", "reason_code", ClickHouseDataType.String),
                    ColumnBinding.scalar("reason_message", "reason_message", ClickHouseDataType.String),
                    ColumnBinding.dateTime64("observed_at", "observed_at", 3),
                    ColumnBinding.scalar(
                            "record_version", "record_version", ClickHouseDataType.UInt64));
        }
    }

    private static final class ProductCatalogMapper extends DataMapper<ProductCatalogRecord> {
        @Override
        public void toMap(ProductCatalogRecord product, Map<String, Object> out) {
            out.putAll(ClickHouseRowMapper.productCatalogValues(product));
        }

        @Override
        public List<ColumnBinding> bindings() {
            return List.of(
                    ColumnBinding.scalar("product_id", "product_id", ClickHouseDataType.UInt64),
                    ColumnBinding.scalar("category_id", "category_id", ClickHouseDataType.UInt64),
                    ColumnBinding.scalar("product_name", "product_name", ClickHouseDataType.String),
                    ColumnBinding.scalar("price", "price", ClickHouseDataType.Decimal),
                    ColumnBinding.scalar("is_active", "is_active", ClickHouseDataType.Bool),
                    ColumnBinding.dateTime64("updated_at", "updated_at", 3),
                    ColumnBinding.scalar(
                            "catalog_version", "catalog_version", ClickHouseDataType.UInt64));
        }
    }

}
