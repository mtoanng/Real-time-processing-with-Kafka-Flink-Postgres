package com.taobao.behavior.processing;

import java.io.Serializable;
import java.time.Duration;

/** Bounded event-id deduplication retention. */
public final class DeduplicationConfig implements Serializable {
    public static final long DEFAULT_RETENTION_HOURS = 168L;

    private final Duration retention;

    private DeduplicationConfig(Duration retention) {
        this.retention = retention;
    }

    public static DeduplicationConfig fromHours(String value) {
        final long hours;
        try {
            hours = Long.parseLong(value);
        } catch (NumberFormatException exc) {
            throw new IllegalArgumentException(
                    "FLINK_DEDUP_RETENTION_HOURS must be an integer", exc);
        }
        if (hours < 1L || hours > 24L * 365L) {
            throw new IllegalArgumentException(
                    "FLINK_DEDUP_RETENTION_HOURS must be between 1 and 8760");
        }
        return new DeduplicationConfig(Duration.ofHours(hours));
    }

    public Duration retention() {
        return retention;
    }
}
