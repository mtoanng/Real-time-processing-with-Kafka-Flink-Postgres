package com.taobao.behavior.sink;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;
import java.util.regex.Pattern;

/** Profile-scoped connection and bounded active-cart key configuration. */
public final class RedisConfig implements Serializable {
    public static final long DEFAULT_CART_TTL_SECONDS = 604_800L;
    private static final Pattern KEY_PREFIX = Pattern.compile("[a-z][a-z0-9:_-]*");

    private final String host;
    private final int port;
    private final boolean tls;
    private final String username;
    private final String password;
    private final String keyPrefix;
    private final long cartTtlSeconds;
    private final Duration connectTimeout;
    private final Duration socketTimeout;

    private RedisConfig(
            String host,
            int port,
            boolean tls,
            String username,
            String password,
            String keyPrefix,
            long cartTtlSeconds,
            Duration connectTimeout,
            Duration socketTimeout) {
        this.host = host;
        this.port = port;
        this.tls = tls;
        this.username = username;
        this.password = password;
        this.keyPrefix = keyPrefix;
        this.cartTtlSeconds = cartTtlSeconds;
        this.connectTimeout = connectTimeout;
        this.socketTimeout = socketTimeout;
    }

    public static RedisConfig fromEnvironment(Map<String, String> environment) {
        String host = required(environment, "REDIS_HOST");
        int port = integerInRange(environment, "REDIS_PORT", 6379, 1, 65_535);
        boolean tls = strictBoolean(environment.getOrDefault("REDIS_TLS", "false"), "REDIS_TLS");
        String username = optional(environment, "REDIS_USERNAME");
        String password = optional(environment, "REDIS_PASSWORD");
        if (username != null && password == null) {
            throw new IllegalArgumentException(
                    "REDIS_PASSWORD is required when REDIS_USERNAME is provided");
        }
        String keyPrefix = environment.getOrDefault("REDIS_KEY_PREFIX", "taobao:active_cart").trim();
        if (!KEY_PREFIX.matcher(keyPrefix).matches()) {
            throw new IllegalArgumentException(
                    "REDIS_KEY_PREFIX must start with a lowercase letter and contain only "
                            + "lowercase letters, digits, colon, underscore, or hyphen");
        }
        long ttlSeconds =
                longInRange(
                        environment,
                        "REDIS_CART_TTL_SECONDS",
                        DEFAULT_CART_TTL_SECONDS,
                        60L,
                        31_536_000L);
        Duration connectTimeout =
                Duration.ofMillis(
                        longInRange(
                                environment,
                                "REDIS_CONNECT_TIMEOUT_MS",
                                10_000L,
                                1L,
                                300_000L));
        Duration socketTimeout =
                Duration.ofMillis(
                        longInRange(
                                environment,
                                "REDIS_SOCKET_TIMEOUT_MS",
                                5_000L,
                                1L,
                                300_000L));
        return new RedisConfig(
                host,
                port,
                tls,
                username,
                password,
                keyPrefix,
                ttlSeconds,
                connectTimeout,
                socketTimeout);
    }

    public String keyForUser(long userId) {
        if (userId <= 0L) {
            throw new IllegalArgumentException("user ID must be positive");
        }
        return keyPrefix + ":{" + userId + "}";
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    public boolean tls() {
        return tls;
    }

    public String username() {
        return username;
    }

    public String password() {
        return password;
    }

    public String keyPrefix() {
        return keyPrefix;
    }

    public long cartTtlSeconds() {
        return cartTtlSeconds;
    }

    public Duration connectTimeout() {
        return connectTimeout;
    }

    public Duration socketTimeout() {
        return socketTimeout;
    }

    private static String required(Map<String, String> environment, String key) {
        String value = optional(environment, key);
        if (value == null) {
            throw new IllegalArgumentException(key + " is required for RUNTIME_PROFILE=serving");
        }
        return value;
    }

    private static String optional(Map<String, String> environment, String key) {
        String value = environment.get(key);
        return value == null || value.isBlank() ? null : value.trim();
    }

    private static boolean strictBoolean(String raw, String key) {
        if (!"true".equalsIgnoreCase(raw) && !"false".equalsIgnoreCase(raw)) {
            throw new IllegalArgumentException(key + " must be true or false");
        }
        return Boolean.parseBoolean(raw);
    }

    private static int integerInRange(
            Map<String, String> environment, String key, int defaultValue, int minimum, int maximum) {
        return (int) longInRange(environment, key, defaultValue, minimum, maximum);
    }

    private static long longInRange(
            Map<String, String> environment,
            String key,
            long defaultValue,
            long minimum,
            long maximum) {
        String raw = environment.getOrDefault(key, Long.toString(defaultValue));
        try {
            long value = Long.parseLong(raw);
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
