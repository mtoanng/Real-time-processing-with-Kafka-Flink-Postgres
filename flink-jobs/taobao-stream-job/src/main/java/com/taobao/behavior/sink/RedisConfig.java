package com.taobao.behavior.sink;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;

public final class RedisConfig implements Serializable {
    public static final long DEFAULT_CART_TTL_SECONDS = 604_800L;
    private static final String KEY_PREFIX = "taobao:active_cart";

    private final String host;
    private final int port;
    private final boolean tls;
    private final String username;
    private final String password;
    private final long cartTtlSeconds;

    private RedisConfig(
            String host,
            int port,
            boolean tls,
            String username,
            String password,
            long cartTtlSeconds) {
        this.host = host;
        this.port = port;
        this.tls = tls;
        this.username = username;
        this.password = password;
        this.cartTtlSeconds = cartTtlSeconds;
    }

    public static RedisConfig fromEnvironment(Map<String, String> environment) {
        String host = optional(environment, "REDIS_HOST");
        if (host == null) {
            throw new IllegalArgumentException("REDIS_HOST is required");
        }
        int port = (int) number(environment, "REDIS_PORT", 6379L, 1L, 65_535L);
        String tlsValue = environment.getOrDefault("REDIS_TLS", "false");
        if (!"true".equalsIgnoreCase(tlsValue) && !"false".equalsIgnoreCase(tlsValue)) {
            throw new IllegalArgumentException("REDIS_TLS must be true or false");
        }
        String username = optional(environment, "REDIS_USERNAME");
        String password = optional(environment, "REDIS_PASSWORD");
        if (username != null && password == null) {
            throw new IllegalArgumentException(
                    "REDIS_PASSWORD is required when REDIS_USERNAME is provided");
        }
        long ttl = number(
                environment,
                "REDIS_CART_TTL_SECONDS",
                DEFAULT_CART_TTL_SECONDS,
                60L,
                31_536_000L);
        return new RedisConfig(
                host, port, Boolean.parseBoolean(tlsValue), username, password, ttl);
    }

    public String keyForUser(long userId) {
        if (userId <= 0L) {
            throw new IllegalArgumentException("user ID must be positive");
        }
        return KEY_PREFIX + ":{" + userId + "}";
    }

    public String host() { return host; }
    public int port() { return port; }
    public boolean tls() { return tls; }
    public String username() { return username; }
    public String password() { return password; }
    public long cartTtlSeconds() { return cartTtlSeconds; }
    public Duration connectTimeout() { return Duration.ofSeconds(10); }
    public Duration socketTimeout() { return Duration.ofSeconds(5); }

    private static String optional(Map<String, String> environment, String key) {
        String value = environment.get(key);
        return value == null || value.isBlank() ? null : value.trim();
    }

    private static long number(
            Map<String, String> environment,
            String key,
            long defaultValue,
            long minimum,
            long maximum) {
        try {
            long value = Long.parseLong(environment.getOrDefault(key, Long.toString(defaultValue)));
            if (value < minimum || value > maximum) {
                throw new NumberFormatException("outside range");
            }
            return value;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(
                    key + " must be an integer between " + minimum + " and " + maximum,
                    exception);
        }
    }
}
