package com.taobao.behavior;

import com.taobao.behavior.aggregation.ItemMetricsAggregator;
import com.taobao.behavior.aggregation.ItemMetricsWindowFunction;
import com.taobao.behavior.avro.UserBehaviorEvent;
import com.taobao.behavior.model.ItemCategoryKey;
import com.taobao.behavior.model.ItemMetrics1m;
import com.taobao.behavior.model.StreamQualityEvent;
import com.taobao.behavior.processing.ActiveCartProjector;
import com.taobao.behavior.processing.CheckpointPolicy;
import com.taobao.behavior.processing.DeduplicationConfig;
import com.taobao.behavior.processing.EventDeduplicator;
import com.taobao.behavior.processing.EventValidator;
import com.taobao.behavior.processing.ImmediateBoundedOutOfOrdernessGenerator;
import com.taobao.behavior.processing.LateEventRouter;
import com.taobao.behavior.sink.ClickHouseSinkFactory;
import com.taobao.behavior.sink.RedisActiveCartSink;
import java.time.Duration;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

public final class TaobaoStreamJob {
    private TaobaoStreamJob() {}

    public static void main(String[] args) throws Exception {
        RuntimeConfig configuration = RuntimeConfig.fromEnvironment(System.getenv());
        String bootstrapServers = configuration.value("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String topic = configuration.value("KAFKA_TOPIC", "user-behavior-events");
        String consumerGroup = configuration.value("KAFKA_CONSUMER_GROUP", "taobao-stream-job");
        String schemaRegistryUrl = configuration.value(
                "SCHEMA_REGISTRY_URL", "http://localhost:8081/apis/ccompat/v7");
        String clickHouseEndpoint = configuration.value("CLICKHOUSE_ENDPOINT", "https://localhost:8443");
        String clickHouseUser = configuration.value("CLICKHOUSE_USER", "default");
        String clickHousePassword = configuration.value("CLICKHOUSE_PASSWORD", "");
        String clickHouseDatabase = configuration.value("CLICKHOUSE_DATABASE", "taobao_behavior");
        CheckpointPolicy checkpointPolicy =
                CheckpointPolicy.fromValues(
                        configuration.value(
                                "FLINK_CHECKPOINTING_ENABLED",
                                "true"),
                        configuration.value("FLINK_CHECKPOINT_INTERVAL_MS", "60000"),
                        configuration.value("FLINK_CHECKPOINT_DIR", ""),
                        configuration.value("FLINK_RESTART_ATTEMPTS", "3"),
                        configuration.value("FLINK_RESTART_DELAY_MS", "10000"));
        DeduplicationConfig deduplicationConfig =
                DeduplicationConfig.fromHours(
                        configuration.value(
                                "FLINK_DEDUP_RETENTION_HOURS",
                                Long.toString(DeduplicationConfig.DEFAULT_RETENTION_HOURS)));

        long maxOutOfOrdernessMillis =
                configuration.longValue("FLINK_MAX_OUT_OF_ORDERNESS_MS", 5_000L, 0L, 3_600_000L);
        StreamExecutionEnvironment execution =
                StreamExecutionEnvironment.getExecutionEnvironment();
        execution.setParallelism(1);
        if (checkpointPolicy.isEnabled()) {
            execution.enableCheckpointing(
                    checkpointPolicy.getIntervalMs(), CheckpointingMode.EXACTLY_ONCE);
            execution.getCheckpointConfig().setCheckpointStorage(checkpointPolicy.getStoragePath());
            execution.getCheckpointConfig().setExternalizedCheckpointCleanup(
                    ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
            execution.setRestartStrategy(
                    RestartStrategies.fixedDelayRestart(
                            checkpointPolicy.getRestartAttempts(),
                            Duration.ofMillis(checkpointPolicy.getRestartDelayMs())));
        }

        var sourceBuilder = KafkaSource.<UserBehaviorEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(consumerGroup)
                .setProperties(configuration.kafkaProperties())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(
                        ConfluentRegistryAvroDeserializationSchema.forSpecific(
                                UserBehaviorEvent.class,
                                schemaRegistryUrl,
                                configuration.schemaRegistryProperties()));
        if (configuration.booleanValue("KAFKA_SOURCE_BOUNDED", true)) {
            sourceBuilder.setBounded(OffsetsInitializer.latest());
        } else {
            sourceBuilder.setUnbounded(OffsetsInitializer.earliest());
        }
        KafkaSource<UserBehaviorEvent> source = sourceBuilder.build();

        DataStream<UserBehaviorEvent> decoded = execution.fromSource(
                        source, WatermarkStrategy.noWatermarks(), "KafkaUserBehaviorSource")
                .uid("kafka-user-behavior-source");

        SingleOutputStreamOperator<UserBehaviorEvent> valid = decoded
                .process(new EventValidator())
                .name("ValidateBehaviorEvent")
                .uid("validate-behavior-event");
        DataStream<StreamQualityEvent> invalid =
                valid.getSideOutput(EventValidator.INVALID_EVENTS);

        SingleOutputStreamOperator<UserBehaviorEvent> acceptedUnique =
                valid.keyBy(event -> event.getEventId().toString())
                        .process(new EventDeduplicator(deduplicationConfig.retention()))
                        .name("DeduplicateEventId")
                        .uid("deduplicate-event-id");
        DataStream<StreamQualityEvent> duplicate =
                acceptedUnique.getSideOutput(EventDeduplicator.DUPLICATE_EVENTS);

        WatermarkStrategy<UserBehaviorEvent> watermarkStrategy = WatermarkStrategy
                .<UserBehaviorEvent>forGenerator(
                        ignored -> new ImmediateBoundedOutOfOrdernessGenerator(
                                maxOutOfOrdernessMillis))
                .withTimestampAssigner((event, previousTimestamp) -> event.getEventTimeMs())
                .withIdleness(Duration.ofSeconds(30));

        SingleOutputStreamOperator<UserBehaviorEvent> onTime = acceptedUnique
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .name("AssignEventTimeAndWatermarks")
                .uid("assign-event-time-watermarks")
                .process(new LateEventRouter())
                .name("RouteLateEvents")
                .uid("route-late-events");
        DataStream<UserBehaviorEvent> late = onTime.getSideOutput(LateEventRouter.LATE_EVENTS);

        SingleOutputStreamOperator<ItemMetrics1m> metrics = onTime
                .keyBy(event -> new ItemCategoryKey(event.getItemId(), event.getCategoryId()))
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .allowedLateness(Duration.ZERO)
                .aggregate(new ItemMetricsAggregator(), new ItemMetricsWindowFunction())
                .name("ItemMetrics1m")
                .uid("item-metrics-1m");

        DataStream<StreamQualityEvent> lateQuality =
                late.map(
                                event ->
                                        StreamQualityEvent.fromEvent(
                                                event,
                                                StreamQualityEvent.QualityType.LATE,
                                                "LATE_FOR_AGGREGATION",
                                                "event_time is at or behind the current watermark",
                                                System.currentTimeMillis()))
                        .returns(StreamQualityEvent.class)
                        .name("BuildLateQualityEvent")
                        .uid("build-late-quality-event");
        DataStream<StreamQualityEvent> qualityEvents =
                invalid.union(duplicate, lateQuality);


        acceptedUnique.sinkTo(
                        ClickHouseSinkFactory.createRawSink(
                                clickHouseEndpoint,
                                clickHouseUser,
                                clickHousePassword,
                                clickHouseDatabase,
                                "raw_behavior_events"))
                .name("WriteRawBehaviorEvents")
                .uid("write-raw-behavior-events");
        qualityEvents.sinkTo(
                        ClickHouseSinkFactory.createQualitySink(
                                clickHouseEndpoint,
                                clickHouseUser,
                                clickHousePassword,
                                clickHouseDatabase,
                                "stream_quality_events"))
                .name("WriteStreamQualityEvents")
                .uid("write-stream-quality-events");
        metrics.sinkTo(
                        ClickHouseSinkFactory.createItemMetricsSink(
                                clickHouseEndpoint,
                                clickHouseUser,
                                clickHousePassword,
                                clickHouseDatabase,
                                "item_metrics_1m"))
                .name("WriteItemMetrics1m")
                .uid("write-item-metrics-1m");
        acceptedUnique
                .keyBy(UserBehaviorEvent::getUserId)
                .process(new ActiveCartProjector())
                .name("ProjectUserActiveCart")
                .uid("project-user-active-cart")
                .addSink(new RedisActiveCartSink(configuration.redisConfig()))
                .name("WriteUserActiveCart")
                .uid("write-user-active-cart");

        execution.execute("Taobao Real-Time Customer Behavior Platform");
    }

}
