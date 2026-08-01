package com.taobao.behavior.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RedisConfigTest {
    @Test
    void acceptsLocalAndManagedConnectionSettings() {
        RedisConfig local = RedisConfig.fromEnvironment(Map.of("REDIS_HOST", "redis"));
        assertEquals("redis", local.host());
        assertEquals(6379, local.port());
        assertFalse(local.tls());
        assertEquals(604_800L, local.cartTtlSeconds());
        assertEquals("taobao:active_cart:{100}", local.keyForUser(100L));

        RedisConfig managed =
                RedisConfig.fromEnvironment(
                        Map.of(
                                "REDIS_HOST", "cache.example.internal",
                                "REDIS_PORT", "6380",
                                "REDIS_TLS", "true",
                                "REDIS_USERNAME", "app",
                                "REDIS_PASSWORD", "secret",
                                "REDIS_CART_TTL_SECONDS", "3600"));
        assertTrue(managed.tls());
        assertEquals("app", managed.username());
        assertEquals("taobao:active_cart:{100}", managed.keyForUser(100L));
        assertEquals(3_600L, managed.cartTtlSeconds());
    }

    @Test
    void rejectsMissingHostPartialAuthAndUnboundedKeys() {
        assertThrows(
                IllegalArgumentException.class,
                () -> RedisConfig.fromEnvironment(Map.of()));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        RedisConfig.fromEnvironment(
                                Map.of("REDIS_HOST", "redis", "REDIS_USERNAME", "user")));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        RedisConfig.fromEnvironment(
                                Map.of(
                                        "REDIS_HOST",
                                        "redis",
                                        "REDIS_CART_TTL_SECONDS",
                                        "0")));
    }

    @Test
    void errorMessagesDoNotContainPasswords() {
        Map<String, String> values = new HashMap<>();
        values.put("REDIS_HOST", "redis");
        values.put("REDIS_USERNAME", "app");
        values.put("REDIS_PASSWORD", "test-secret");
        values.put("REDIS_TLS", "not-a-boolean");

        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> RedisConfig.fromEnvironment(values));
        assertFalse(error.getMessage().contains("test-secret"));
    }
}
