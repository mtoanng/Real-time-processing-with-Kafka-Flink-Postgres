package com.taobao.behavior;

import com.taobao.behavior.sink.RedisConfig;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

final class RuntimeConfig {
    private final Map<String, String> environment;

    private RuntimeConfig(Map<String, String> environment) {
        this.environment = environment;
        kafkaProperties();
        RedisConfig.fromEnvironment(environment);
    }

    static RuntimeConfig fromEnvironment(Map<String, String> environment) {
        return new RuntimeConfig(environment);
    }

    String value(String key, String defaultValue) {
        String value = environment.get(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    long longValue(String key, long defaultValue, long minimum, long maximum) {
        String raw = value(key, Long.toString(defaultValue));
        try {
            long parsed = Long.parseLong(raw);
            if (parsed < minimum || parsed > maximum) {
                throw new NumberFormatException("outside range");
            }
            return parsed;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(
                    key + " must be between " + minimum + " and " + maximum, exception);
        }
    }

    boolean booleanValue(String key, boolean defaultValue) {
        String raw = value(key, Boolean.toString(defaultValue));
        if (!"true".equalsIgnoreCase(raw) && !"false".equalsIgnoreCase(raw)) {
            throw new IllegalArgumentException(key + " must be true or false");
        }
        return Boolean.parseBoolean(raw);
    }

    RedisConfig redisConfig() {
        return RedisConfig.fromEnvironment(environment);
    }

    Properties kafkaProperties() {
        String protocol = value("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").toUpperCase(Locale.ROOT);
        if (!"PLAINTEXT".equals(protocol) && !"SASL_SSL".equals(protocol)) {
            throw new IllegalArgumentException(
                    "KAFKA_SECURITY_PROTOCOL must be PLAINTEXT or SASL_SSL");
        }
        Properties properties = new Properties();
        properties.setProperty("security.protocol", protocol);
        String mechanism = optional("KAFKA_SASL_MECHANISM");
        String username = optional("KAFKA_SASL_USERNAME");
        String password = optional("KAFKA_SASL_PASSWORD");
        String jaas = optional("KAFKA_SASL_JAAS_CONFIG");
        if ("PLAINTEXT".equals(protocol)) {
            if (mechanism != null || username != null || password != null || jaas != null) {
                throw new IllegalArgumentException(
                        "SASL settings require KAFKA_SECURITY_PROTOCOL=SASL_SSL");
            }
            return properties;
        }
        if (mechanism == null) {
            throw new IllegalArgumentException("KAFKA_SASL_MECHANISM is required for SASL_SSL");
        }
        if ((username == null) != (password == null)) {
            throw new IllegalArgumentException(
                    "KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD must be provided together");
        }
        if (jaas == null && username == null) {
            throw new IllegalArgumentException(
                    "SASL_SSL requires KAFKA_SASL_JAAS_CONFIG or username/password");
        }
        properties.setProperty("sasl.mechanism", mechanism);
        properties.setProperty(
                "sasl.jaas.config",
                jaas != null
                        ? jaas
                        : "org.apache.kafka.common.security.plain.PlainLoginModule required "
                                + "username=\""
                                + username
                                + "\" password=\""
                                + password
                                + "\";");
        return properties;
    }

    Map<String, Object> schemaRegistryProperties() {
        Map<String, Object> properties = new HashMap<>();
        String userInfo = optional("SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO");
        if (userInfo != null) {
            properties.put("basic.auth.credentials.source", "USER_INFO");
            properties.put("basic.auth.user.info", userInfo);
        }
        return properties;
    }

    private String optional(String key) {
        String value = environment.get(key);
        return value == null || value.isBlank() ? null : value;
    }
}
