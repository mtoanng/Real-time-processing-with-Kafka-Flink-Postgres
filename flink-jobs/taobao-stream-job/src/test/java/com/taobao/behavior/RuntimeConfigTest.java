package com.taobao.behavior;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class RuntimeConfigTest {
    @Test
    void oneRuntimeRequiresRedisAndUsesPlaintextKafkaByDefault() {
        RuntimeConfig config = RuntimeConfig.fromEnvironment(Map.of("REDIS_HOST", "redis"));
        assertEquals("redis", config.redisConfig().host());
        assertEquals("PLAINTEXT", config.kafkaProperties().getProperty("security.protocol"));
        assertEquals(5_000L, config.longValue("FLINK_MAX_OUT_OF_ORDERNESS_MS", 5_000L, 0L, 60_000L));
    }

    @Test
    void managedKafkaAndSchemaRegistryCredentialsMapDirectly() {
        Map<String, String> environment = new HashMap<>();
        environment.put("REDIS_HOST", "redis");
        environment.put("KAFKA_SECURITY_PROTOCOL", "SASL_SSL");
        environment.put("KAFKA_SASL_MECHANISM", "PLAIN");
        environment.put("KAFKA_SASL_USERNAME", "key");
        environment.put("KAFKA_SASL_PASSWORD", "secret");
        environment.put("SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO", "registry:secret");

        RuntimeConfig config = RuntimeConfig.fromEnvironment(environment);
        Properties kafka = config.kafkaProperties();
        assertEquals("SASL_SSL", kafka.getProperty("security.protocol"));
        assertEquals("PLAIN", kafka.getProperty("sasl.mechanism"));
        assertEquals(
                "registry:secret",
                config.schemaRegistryProperties().get("basic.auth.user.info"));
    }

    @Test
    void rejectsMissingRedisAndPartialKafkaSecurity() {
        assertThrows(IllegalArgumentException.class, () -> RuntimeConfig.fromEnvironment(Map.of()));
        assertThrows(
                IllegalArgumentException.class,
                () -> RuntimeConfig.fromEnvironment(
                        Map.of(
                                "REDIS_HOST", "redis",
                                "KAFKA_SECURITY_PROTOCOL", "SASL_SSL",
                                "KAFKA_SASL_MECHANISM", "PLAIN")));
    }
}
