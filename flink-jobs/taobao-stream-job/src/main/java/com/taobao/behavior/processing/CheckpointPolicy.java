package com.taobao.behavior.processing;

import java.io.Serializable;

public final class CheckpointPolicy implements Serializable {
    private final boolean enabled;
    private final long intervalMs;
    private final String storagePath;
    private final int restartAttempts;
    private final long restartDelayMs;

    private CheckpointPolicy(
            boolean enabled,
            long intervalMs,
            String storagePath,
            int restartAttempts,
            long restartDelayMs) {
        this.enabled = enabled;
        this.intervalMs = intervalMs;
        this.storagePath = storagePath;
        this.restartAttempts = restartAttempts;
        this.restartDelayMs = restartDelayMs;
    }

    public static CheckpointPolicy fromValues(
            String enabledValue,
            String intervalValue,
            String storagePath,
            String restartAttemptsValue,
            String restartDelayValue) {
        if (!"true".equalsIgnoreCase(enabledValue) && !"false".equalsIgnoreCase(enabledValue)) {
            throw new IllegalArgumentException("FLINK_CHECKPOINTING_ENABLED must be true or false");
        }
        boolean enabled = Boolean.parseBoolean(enabledValue);
        long intervalMs;
        try {
            intervalMs = Long.parseLong(intervalValue);
        } catch (NumberFormatException exc) {
            throw new IllegalArgumentException("FLINK_CHECKPOINT_INTERVAL_MS must be an integer", exc);
        }
        if (intervalMs < 1_000L) {
            throw new IllegalArgumentException("checkpoint interval must be at least 1000 ms");
        }
        if (enabled && (storagePath == null || storagePath.isBlank())) {
            throw new IllegalArgumentException(
                    "FLINK_CHECKPOINT_DIR is required when checkpointing is enabled");
        }
        final int restartAttempts;
        final long restartDelayMs;
        try {
            restartAttempts = Integer.parseInt(restartAttemptsValue);
            restartDelayMs = Long.parseLong(restartDelayValue);
        } catch (NumberFormatException exc) {
            throw new IllegalArgumentException(
                    "restart attempts and delay must be integers", exc);
        }
        if (restartAttempts < 0 || restartAttempts > 10) {
            throw new IllegalArgumentException("FLINK_RESTART_ATTEMPTS must be between 0 and 10");
        }
        if (restartDelayMs < 0L || restartDelayMs > 300_000L) {
            throw new IllegalArgumentException(
                    "FLINK_RESTART_DELAY_MS must be between 0 and 300000");
        }
        return new CheckpointPolicy(
                enabled,
                intervalMs,
                storagePath == null ? "" : storagePath,
                restartAttempts,
                restartDelayMs);
    }

    public boolean isEnabled() {
        return enabled;
    }

    public long getIntervalMs() {
        return intervalMs;
    }

    public String getStoragePath() {
        return storagePath;
    }

    public int getRestartAttempts() {
        return restartAttempts;
    }

    public long getRestartDelayMs() {
        return restartDelayMs;
    }
}
