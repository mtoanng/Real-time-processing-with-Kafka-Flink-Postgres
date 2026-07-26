package com.taobao.behavior;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.taobao.behavior.avro.BehaviorType;
import com.taobao.behavior.avro.UserBehaviorEvent;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class ClickHouseDeliveryContractTest {
    @Test
    void replayUsesStableLogicalIdentityAndVerificationUsesDeduplicatedViews()
            throws IOException {
        UserBehaviorEvent first = EventTestSupport.event(
                100L, 500L, 50L, BehaviorType.cart, 1_511_658_000_000L, 7L);
        UserBehaviorEvent retry = EventTestSupport.event(
                100L, 500L, 50L, BehaviorType.cart, 1_511_658_000_000L, 7L);

        assertEquals(first.getEventId(), retry.getEventId());
        assertEquals(first.getSourceSequence(), retry.getSourceSequence());

        String verification = Files.readString(
                repositoryRoot().resolve("infra/clickhouse/verify.sql"));
        assertTrue(verification.contains("raw_behavior_events_canonical"));
        assertTrue(verification.contains("item_metrics_1m_canonical"));
        assertTrue(verification.contains("stream_quality_events_canonical"));
        assertTrue(verification.contains("raw_transport_rows_above_canonical"));
    }

    private static Path repositoryRoot() {
        Path current = Path.of("").toAbsolutePath();
        while (current != null) {
            if (Files.isRegularFile(current.resolve("infra/clickhouse/verify.sql"))) {
                return current;
            }
            current = current.getParent();
        }
        throw new IllegalStateException("could not locate repository root");
    }
}
