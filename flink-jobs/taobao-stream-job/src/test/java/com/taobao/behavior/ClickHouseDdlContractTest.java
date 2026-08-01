package com.taobao.behavior;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class ClickHouseDdlContractTest {
    @Test
    void ddlUsesReplayIndependentReplacementKeysAndOneQualityTable() throws IOException {
        String ddl = Files.readString(repositoryRoot().resolve("infra/clickhouse/schema.sql"));

        assertTrue(ddl.contains("CREATE TABLE IF NOT EXISTS taobao_behavior.raw_behavior_events"));
        assertTrue(ddl.contains("event_id String"));
        assertTrue(ddl.contains("replay_run_id String"));
        assertTrue(ddl.contains("source_sequence UInt64"));
        assertTrue(ddl.contains("record_version UInt64"));
        assertTrue(ddl.contains("ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)"));
        assertTrue(ddl.contains("ENGINE = ReplacingMergeTree(record_version)"));
        assertTrue(ddl.contains("ORDER BY (toDate(event_time), event_id)"));
        assertTrue(ddl.contains("raw_behavior_events_canonical"));
        assertTrue(ddl.contains("CREATE TABLE IF NOT EXISTS taobao_behavior.item_metrics_1m"));
        assertTrue(ddl.contains("unique_users UInt64"));
        assertTrue(ddl.contains("source_category_id UInt64"));
        assertTrue(ddl.contains("ORDER BY (window_start, item_id, source_category_id)"));
        assertTrue(ddl.contains("item_metrics_1m_canonical"));
        assertTrue(ddl.contains("stream_quality_events"));
        assertTrue(ddl.contains("quality_type LowCardinality(String)"));
        assertTrue(ddl.contains("quality_event_id String"));
        assertFalse(ddl.contains("behavior_alerts"));
        assertTrue(ddl.contains("reason_code LowCardinality(String)"));
        assertTrue(ddl.contains("reason_message String"));
        assertTrue(ddl.contains("FROM taobao_behavior.raw_behavior_events FINAL"));
        assertTrue(ddl.contains("FROM taobao_behavior.item_metrics_1m FINAL"));
        assertTrue(ddl.contains("FROM taobao_behavior.stream_quality_events FINAL"));
        assertFalse(ddl.contains("invalid_behavior_events"));
        assertFalse(ddl.contains("late_behavior_events"));
        assertFalse(ddl.contains("f1_telemetry"));
    }

    private static Path repositoryRoot() {
        Path current = Path.of("").toAbsolutePath();
        while (current != null) {
            if (Files.isRegularFile(current.resolve("infra/clickhouse/schema.sql"))) {
                return current;
            }
            current = current.getParent();
        }
        throw new IllegalStateException("could not locate repository root");
    }
}
