package com.taobao.behavior.sink;

import com.taobao.behavior.model.ActiveCartItem;
import com.taobao.behavior.model.CartMutation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * Synchronous, at-least-once active-cart sink.
 *
 * <p>HSET and HDEL are deterministic for one user/item logical key. The Flink projector rejects
 * stale transitions before this sink. A repeated mutation after recovery therefore converges.
 */
public class RedisActiveCartSink extends RichSinkFunction<CartMutation> {
    private final RedisConfig config;
    private final RedisClientFactory clientFactory;
    private transient RedisCommandsClient client;

    public RedisActiveCartSink(RedisConfig config) {
        this(config, new RedisClientFactory());
    }

    RedisActiveCartSink(RedisConfig config, RedisClientFactory clientFactory) {
        this.config = config;
        this.clientFactory = clientFactory;
    }

    @Override
    public void open(Configuration parameters) {
        try {
            client = clientFactory.open(config);
            if (!"PONG".equals(client.ping())) {
                throw new IllegalStateException("unexpected PING response");
            }
        } catch (RuntimeException exception) {
            close();
            throw new IllegalStateException(
                    "failed to initialize Redis active-cart sink for "
                            + config.host()
                            + ":"
                            + config.port(),
                    exception);
        }
    }

    @Override
    public void invoke(CartMutation mutation, Context context) {
        validateMutation(mutation);
        ActiveCartItem item = mutation.getItem();
        String key = config.keyForUser(item.getUserId());
        String field = RedisCartCodec.itemField(item.getItemId());
        try {
            if (mutation.getType() == CartMutation.Type.UPSERT_CART_ITEM) {
                client.hset(key, field, RedisCartCodec.encode(item));
            } else {
                client.hdel(key, field);
            }
            client.expire(key, config.cartTtlSeconds());
        } catch (RuntimeException exception) {
            throw new IllegalStateException(
                    "Redis "
                            + mutation.getType()
                            + " failed for user_id="
                            + item.getUserId()
                            + " item_id="
                            + item.getItemId(),
                    exception);
        }
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    private static void validateMutation(CartMutation mutation) {
        if (mutation == null || mutation.getType() == null || mutation.getItem() == null) {
            throw new IllegalArgumentException("cart mutation and item are required");
        }
        ActiveCartItem item = mutation.getItem();
        if (item.getUserId() <= 0L || item.getItemId() <= 0L) {
            throw new IllegalArgumentException("cart mutation must contain positive user and item IDs");
        }
        RedisCartCodec.encode(item);
    }
}
