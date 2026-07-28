package com.taobao.behavior;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.taobao.behavior.model.ProductCatalogRecord;
import com.taobao.behavior.source.ProductCatalogDebeziumDeserializer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class ProductCatalogContractTest {
    @Test
    void snapshotUpdatesAndRetryConvergeToTheManualExpectedState() throws Exception {
        Path root = repositoryRoot();
        List<String> messages =
                Files.readAllLines(root.resolve("tests/fixtures/product_catalog_cdc.jsonl"));
        ProductCatalogDebeziumDeserializer deserializer =
                new ProductCatalogDebeziumDeserializer();
        Map<Long, ProductCatalogRecord> current = new HashMap<>();
        Collector<ProductCatalogRecord> collector =
                new Collector<>() {
                    @Override
                    public void collect(ProductCatalogRecord record) {
                        current.merge(
                                record.getProductId(),
                                record,
                                (left, right) ->
                                        right.getCatalogVersion() > left.getCatalogVersion()
                                                ? right
                                                : left);
                    }

                    @Override
                    public void close() {}
                };

        for (int offset = 0; offset < messages.size(); offset++) {
            deserializer.deserialize(
                    new ConsumerRecord<>(
                            "product-catalog-cdc",
                            0,
                            offset,
                            null,
                            messages.get(offset).getBytes(StandardCharsets.UTF_8)),
                    collector);
        }

        List<ProductCatalogRecord> actual = new ArrayList<>(current.values());
        actual.sort(Comparator.comparingLong(ProductCatalogRecord::getProductId));
        assertEquals(
                List.of(
                        new ProductCatalogRecord(
                                100L,
                                10L,
                                "Wireless Headphones Pro",
                                new BigDecimal("44.99"),
                                true,
                                1785287100000L,
                                2L),
                        new ProductCatalogRecord(
                                101L,
                                11L,
                                "Wireless Mouse",
                                new BigDecimal("19.99"),
                                false,
                                1785287160000L,
                                2L),
                        new ProductCatalogRecord(
                                102L,
                                12L,
                                "Mechanical Keyboard",
                                new BigDecimal("79.00"),
                                true,
                                1785286800000L,
                                1L),
                        new ProductCatalogRecord(
                                103L,
                                13L,
                                "USB-C Charger",
                                new BigDecimal("29.50"),
                                true,
                                1785286800000L,
                                1L),
                        new ProductCatalogRecord(
                                104L,
                                14L,
                                "Action Camera",
                                new BigDecimal("129.00"),
                                true,
                                1785286800000L,
                                1L)),
                actual);
    }

    private static Path repositoryRoot() {
        Path current = Path.of("").toAbsolutePath();
        while (current != null) {
            if (Files.isRegularFile(current.resolve("tests/fixtures/product_catalog_cdc.jsonl"))) {
                return current;
            }
            current = current.getParent();
        }
        throw new IllegalStateException("could not locate repository root");
    }
}
