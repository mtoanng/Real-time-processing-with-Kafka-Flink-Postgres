package com.taobao.behavior.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.taobao.behavior.model.ActiveCartItem;
import com.taobao.behavior.model.CartMutation;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

class RedisActiveCartSinkTest {
    @Test
    void writesDeterministicHashMutationsRefreshesTtlAndClosesClient() throws Exception {
        RecordingClient client = new RecordingClient();
        RedisActiveCartSink sink =
                new RedisActiveCartSink(
                        config(),
                        new RedisClientFactory() {
                            @Override
                            RedisCommandsClient open(RedisConfig ignored) {
                                return client;
                            }
                        });
        ActiveCartItem item = new ActiveCartItem(100L, 500L, 50L, 1_000L, 2_000L);

        sink.open(new Configuration());
        sink.invoke(new CartMutation(CartMutation.Type.UPSERT_CART_ITEM, item), null);
        assertEquals("50|1000|2000", client.values.get("taobao:active_cart:{100}|500"));
        assertEquals(604_800L, client.lastTtlSeconds);

        sink.invoke(new CartMutation(CartMutation.Type.DELETE_CART_ITEM, item), null);
        assertTrue(client.values.isEmpty());
        assertEquals(2, client.expireCalls);

        sink.close();
        assertTrue(client.closed);
    }

    @Test
    void rejectsMalformedMutationBeforeWriting() {
        RedisActiveCartSink sink = new RedisActiveCartSink(config(), new RedisClientFactory());
        assertThrows(IllegalArgumentException.class, () -> sink.invoke(null, null));
    }

    @Test
    void initializationFailureHasContextWithoutCredentials() {
        RedisActiveCartSink sink =
                new RedisActiveCartSink(
                        config(),
                        new RedisClientFactory() {
                            @Override
                            RedisCommandsClient open(RedisConfig ignored) {
                                throw new IllegalStateException("connection refused");
                            }
                        });

        IllegalStateException error =
                assertThrows(
                        IllegalStateException.class,
                        () -> sink.open(new Configuration()));

        assertTrue(error.getMessage().contains("Redis active-cart sink"));
        assertTrue(!error.getMessage().contains("test-secret"));
    }

    private static RedisConfig config() {
        return RedisConfig.fromEnvironment(
                Map.of(
                        "REDIS_HOST", "redis",
                        "REDIS_PASSWORD", "test-secret"));
    }

    private static final class RecordingClient implements RedisCommandsClient {
        private final Map<String, String> values = new LinkedHashMap<>();
        private long lastTtlSeconds;
        private int expireCalls;
        private boolean closed;

        @Override
        public long hset(String key, String field, String value) {
            values.put(key + "|" + field, value);
            return 1L;
        }

        @Override
        public long hdel(String key, String field) {
            values.remove(key + "|" + field);
            return 1L;
        }

        @Override
        public long expire(String key, long seconds) {
            lastTtlSeconds = seconds;
            expireCalls++;
            return 1L;
        }

        @Override
        public String ping() {
            return "PONG";
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
