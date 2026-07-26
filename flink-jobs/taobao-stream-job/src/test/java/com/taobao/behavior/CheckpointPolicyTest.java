package com.taobao.behavior;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.taobao.behavior.processing.CheckpointPolicy;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class CheckpointPolicyTest {
    @Test
    void checkpointingCanBeDisabledForCredentialIndependentChecks() {
        CheckpointPolicy policy =
                CheckpointPolicy.fromValues("false", "60000", "", "0", "0");

        assertFalse(policy.isEnabled());
        assertEquals(60_000L, policy.getIntervalMs());
    }

    @Test
    void enabledCheckpointingRequiresDurableStoragePath() {
        CheckpointPolicy policy =
                CheckpointPolicy.fromValues(
                        "true", "10000", "/var/lib/flink/checkpoints", "3", "5000");

        assertTrue(policy.isEnabled());
        assertEquals("/var/lib/flink/checkpoints", policy.getStoragePath());
        assertEquals(3, policy.getRestartAttempts());
        assertEquals(5_000L, policy.getRestartDelayMs());
        assertThrows(
                IllegalArgumentException.class,
                () -> CheckpointPolicy.fromValues("true", "10000", "", "3", "5000"));
    }

    @Test
    void checkpointIntervalMustBeSane() {
        assertThrows(
                IllegalArgumentException.class,
                () -> CheckpointPolicy.fromValues(
                        "sometimes", "60000", "", "3", "5000"));
        assertThrows(
                IllegalArgumentException.class,
                () -> CheckpointPolicy.fromValues(
                        "false", "not-a-number", "", "3", "5000"));
        assertThrows(
                IllegalArgumentException.class,
                () -> CheckpointPolicy.fromValues("false", "999", "", "3", "5000"));
        assertThrows(
                IllegalArgumentException.class,
                () -> CheckpointPolicy.fromValues("true", "10000", "/tmp/cp", "11", "5000"));
        assertThrows(
                IllegalArgumentException.class,
                () -> CheckpointPolicy.fromValues("true", "10000", "/tmp/cp", "3", "-1"));
    }

    @Test
    void jobUsesExactlyOnceStateRecoveryFixedRestartAndStableUids() throws IOException {
        String job = Files.readString(
                repositoryRoot().resolve(
                        "flink-jobs/taobao-stream-job/src/main/java/"
                                + "com/taobao/behavior/TaobaoStreamJob.java"));

        assertTrue(job.contains("CheckpointingMode.EXACTLY_ONCE"));
        assertTrue(job.contains("RestartStrategies.fixedDelayRestart"));
        assertTrue(job.contains(".uid(\"kafka-user-behavior-source\")"));
        assertTrue(job.contains(".uid(\"deduplicate-event-id\")"));
        assertTrue(job.contains(".uid(\"item-metrics-1m\")"));
        assertTrue(job.contains(".uid(\"write-raw-behavior-events\")"));
    }

    private static Path repositoryRoot() {
        Path current = Path.of("").toAbsolutePath();
        while (current != null) {
            if (Files.isRegularFile(current.resolve("pom.xml"))
                    && Files.isDirectory(current.resolve("flink-jobs"))) {
                return current;
            }
            current = current.getParent();
        }
        throw new IllegalStateException("could not locate repository root");
    }
}
