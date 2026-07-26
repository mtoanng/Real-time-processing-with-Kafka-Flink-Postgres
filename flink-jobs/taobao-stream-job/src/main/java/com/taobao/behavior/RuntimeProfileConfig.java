package com.taobao.behavior;

import com.taobao.behavior.sink.CassandraConfig;
import java.util.Locale;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

final class RuntimeProfileConfig {
    private final Map<String, String> environment;
    private final String runtimeProfile;

    private RuntimeProfileConfig(Map<String, String> environment) {
        this.environment = environment;
        runtimeProfile = value("RUNTIME_PROFILE", "core").toLowerCase(Locale.ROOT);
        if (!"checks".equals(runtimeProfile)
                && !"core".equals(runtimeProfile)
                && !"serving".equals(runtimeProfile)
                && !"cdc".equals(runtimeProfile)
                && !"observability".equals(runtimeProfile)) {
            throw new IllegalArgumentException(
                    "RUNTIME_PROFILE must be checks, core, serving, cdc, or observability");
        }
        validateProfile();
        kafkaProperties();
    }

    static RuntimeProfileConfig fromEnvironment(Map<String, String> environment) {
        return new RuntimeProfileConfig(environment);
    }

    String runtimeProfile() {
        return runtimeProfile;
    }

    boolean isChecksProfile() {
        return "checks".equals(runtimeProfile);
    }

    boolean isCassandraEnabled() {
        return "serving".equals(runtimeProfile);
    }

    boolean isCdcEnabled() {
        return "cdc".equals(runtimeProfile);
    }

    boolean isObservabilityEnabled() {
        return "observability".equals(runtimeProfile);
    }

    boolean checkpointingEnabledByDefault() {
        return !isChecksProfile();
    }

    String value(String key, String defaultValue) {
        String value = environment.get(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    CassandraConfig cassandraConfig() {
        if (!isCassandraEnabled()) {
            throw new IllegalStateException("Cassandra is not opened by the checks profile");
        }
        return CassandraConfig.fromEnvironment(environment);
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
        if (jaas != null) {
            properties.setProperty("sasl.jaas.config", jaas);
        } else {
            properties.setProperty(
                    "sasl.jaas.config",
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                            + username + "\" password=\"" + password + "\";");
        }
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

    private void validateProfile() {
        if (isCassandraEnabled()) {
            CassandraConfig.fromEnvironment(environment);
        }
        if (isCdcEnabled()) {
            required("RULES_KAFKA_TOPIC");
        }
    }

    private String required(String key) {
        String value = optional(key);
        if (value == null) {
            throw new IllegalArgumentException(key + " is required for RUNTIME_PROFILE=" + runtimeProfile);
        }
        return value;
    }

    private String optional(String key) {
        String value = environment.get(key);
        return value == null || value.isBlank() ? null : value;
    }
}
