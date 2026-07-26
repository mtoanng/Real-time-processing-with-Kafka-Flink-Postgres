package com.taobao.behavior;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.taobao.behavior.processing.DeduplicationConfig;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class DeduplicationConfigTest {
    @Test
    void acceptsConservativeBoundedDemoRetention() {
        assertEquals(
                Duration.ofDays(7),
                DeduplicationConfig.fromHours("168").retention());
    }

    @Test
    void rejectsUnboundedOrMalformedRetention() {
        assertThrows(
                IllegalArgumentException.class,
                () -> DeduplicationConfig.fromHours("0"));
        assertThrows(
                IllegalArgumentException.class,
                () -> DeduplicationConfig.fromHours("8761"));
        assertThrows(
                IllegalArgumentException.class,
                () -> DeduplicationConfig.fromHours("seven"));
    }
}
