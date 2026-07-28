package com.taobao.behavior.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.taobao.behavior.model.ProductCatalogRecord;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public final class ProductCatalogDebeziumDeserializer
        implements KafkaRecordDeserializationSchema<ProductCatalogRecord> {
    private static final ObjectMapper JSON = new ObjectMapper();

    @Override
    public void deserialize(
            ConsumerRecord<byte[], byte[]> input, Collector<ProductCatalogRecord> output)
            throws IOException {
        if (input.value() == null) {
            return;
        }
        JsonNode envelope = JSON.readTree(new String(input.value(), StandardCharsets.UTF_8));
        String operation = required(envelope, "op").asText();
        if ("d".equals(operation)) {
            return;
        }
        if (!"r".equals(operation) && !"c".equals(operation) && !"u".equals(operation)) {
            throw new IOException("unsupported product catalog CDC operation: " + operation);
        }
        JsonNode after = required(envelope, "after");
        try {
            output.collect(
                    new ProductCatalogRecord(
                            required(after, "product_id").asLong(),
                            required(after, "category_id").asLong(),
                            required(after, "product_name").asText(),
                            new BigDecimal(required(after, "price").asText()),
                            required(after, "is_active").asBoolean(),
                            timestampMillis(required(after, "updated_at")),
                            required(after, "catalog_version").asLong()));
        } catch (IllegalArgumentException exception) {
            throw new IOException("invalid product catalog CDC record", exception);
        }
    }

    @Override
    public TypeInformation<ProductCatalogRecord> getProducedType() {
        return TypeInformation.of(ProductCatalogRecord.class);
    }

    private static JsonNode required(JsonNode object, String field) throws IOException {
        JsonNode value = object.get(field);
        if (value == null || value.isNull()) {
            throw new IOException("product catalog CDC record is missing " + field);
        }
        return value;
    }

    private static long timestampMillis(JsonNode value) {
        if (value.isNumber()) {
            long timestamp = value.asLong();
            if (timestamp >= 100_000_000_000_000L) {
                return timestamp / 1_000L;
            }
            if (timestamp < 100_000_000_000L) {
                return Math.multiplyExact(timestamp, 1_000L);
            }
            return timestamp;
        }
        return Instant.parse(value.asText()).toEpochMilli();
    }
}
