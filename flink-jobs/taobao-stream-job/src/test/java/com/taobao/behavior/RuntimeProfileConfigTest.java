package com.taobao.behavior;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class RuntimeProfileConfigTest {
    @Test
    void checksProfileRequiresNoExternalServiceConfiguration() {
        RuntimeProfileConfig config = RuntimeProfileConfig.fromEnvironment(
                Map.of("RUNTIME_PROFILE", "checks"));

        assertTrue(config.isChecksProfile());
        assertFalse(config.isCassandraEnabled());
        assertFalse(config.isCdcEnabled());
        assertFalse(config.checkpointingEnabledByDefault());
        assertEquals("PLAINTEXT", config.kafkaProperties().getProperty("security.protocol"));
    }

    @Test
    void coreRequiresNoOptionalExtensionAndDefaultsCheckpointingOn() {
        RuntimeProfileConfig config =
                RuntimeProfileConfig.fromEnvironment(Map.of("RUNTIME_PROFILE", "core"));
        assertFalse(config.isCassandraEnabled());
        assertFalse(config.isCdcEnabled());
        assertFalse(config.isObservabilityEnabled());
        assertTrue(config.checkpointingEnabledByDefault());
    }

    @Test
    void servingAddsCassandraWithoutCdcOrObservability() {
        Map<String, String> environment = localServingEnvironment();
        RuntimeProfileConfig config = RuntimeProfileConfig.fromEnvironment(environment);

        assertTrue(config.isCassandraEnabled());
        assertFalse(config.isCdcEnabled());
        assertFalse(config.isObservabilityEnabled());
        assertEquals("local", config.cassandraConfig().mode());
    }

    @Test
    void cdcAddsOnlyTheExistingRuleControlPlane() {
        Map<String, String> environment = new HashMap<>();
        environment.put("RUNTIME_PROFILE", "cdc");
        environment.put("RULES_KAFKA_TOPIC", "behavior-rules");

        RuntimeProfileConfig config = RuntimeProfileConfig.fromEnvironment(environment);

        assertFalse(config.isCassandraEnabled());
        assertTrue(config.isCdcEnabled());
        assertFalse(config.isObservabilityEnabled());
        assertTrue(config.checkpointingEnabledByDefault());
    }

    @Test
    void cdcFailsFastWithoutRulesTopic() {
        assertThrows(
                IllegalArgumentException.class,
                () -> RuntimeProfileConfig.fromEnvironment(
                        Map.of("RUNTIME_PROFILE", "cdc")));
    }

    @Test
    void observabilityDoesNotChangeTheJobDataPath() {
        RuntimeProfileConfig config =
                RuntimeProfileConfig.fromEnvironment(
                        Map.of("RUNTIME_PROFILE", "observability"));
        assertFalse(config.isCassandraEnabled());
        assertFalse(config.isCdcEnabled());
        assertTrue(config.isObservabilityEnabled());
    }

    @Test
    void unsupportedRuntimeProfileIsRejected() {
        assertThrows(
                IllegalArgumentException.class,
                () -> RuntimeProfileConfig.fromEnvironment(
                        Map.of("RUNTIME_PROFILE", "full")));
    }

    @Test
    void managedSaslSslMapsToKafkaClientProperties() {
        Map<String, String> environment = new HashMap<>();
        environment.put("RUNTIME_PROFILE", "checks");
        environment.put("KAFKA_SECURITY_PROTOCOL", "SASL_SSL");
        environment.put("KAFKA_SASL_MECHANISM", "PLAIN");
        environment.put("KAFKA_SASL_USERNAME", "key");
        environment.put("KAFKA_SASL_PASSWORD", "secret");

        Properties properties = RuntimeProfileConfig.fromEnvironment(environment).kafkaProperties();

        assertEquals("SASL_SSL", properties.getProperty("security.protocol"));
        assertEquals("PLAIN", properties.getProperty("sasl.mechanism"));
        assertTrue(properties.getProperty("sasl.jaas.config").contains("username=\"key\""));
    }

    @Test
    void partialSaslConfigurationIsRejected() {
        assertThrows(
                IllegalArgumentException.class,
                () -> RuntimeProfileConfig.fromEnvironment(Map.of(
                        "RUNTIME_PROFILE", "checks",
                        "KAFKA_SECURITY_PROTOCOL", "SASL_SSL")));
    }

    @Test
    void schemaRegistryBasicAuthenticationIsPassedToTheDeserializer() {
        RuntimeProfileConfig config = RuntimeProfileConfig.fromEnvironment(Map.of(
                "RUNTIME_PROFILE", "checks",
                "SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO", "key:secret"));

        assertEquals("USER_INFO", config.schemaRegistryProperties().get(
                "basic.auth.credentials.source"));
        assertEquals("key:secret", config.schemaRegistryProperties().get(
                "basic.auth.user.info"));
    }

    private static Map<String, String> localServingEnvironment() {
        Map<String, String> environment = new HashMap<>();
        environment.put("RUNTIME_PROFILE", "serving");
        environment.put("CASSANDRA_MODE", "local");
        environment.put("CASSANDRA_HOSTS", "localhost");
        environment.put("CASSANDRA_PORT", "9042");
        environment.put("CASSANDRA_DATACENTER", "datacenter1");
        environment.put("CASSANDRA_KEYSPACE", "taobao_streaming");
        environment.put("CASSANDRA_TABLE", "user_active_cart");
        return environment;
    }
}
