package com.taobao.behavior.sink;

import com.taobao.behavior.model.CartMutation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPooled;

public class RedisActiveCartSink extends RichSinkFunction<CartMutation> {
    private final RedisConfig config;
    private transient JedisPooled redis;

    public RedisActiveCartSink(RedisConfig config) {
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) {
        DefaultJedisClientConfig.Builder client = DefaultJedisClientConfig.builder()
                .ssl(config.tls())
                .connectionTimeoutMillis((int) config.connectTimeout().toMillis())
                .socketTimeoutMillis((int) config.socketTimeout().toMillis());
        if (config.username() != null) {
            client.user(config.username());
        }
        if (config.password() != null) {
            client.password(config.password());
        }
        redis = new JedisPooled(new HostAndPort(config.host(), config.port()), client.build());
        if (!"PONG".equals(redis.ping())) {
            close();
            throw new IllegalStateException("unexpected Redis PING response");
        }
    }

    @Override
    public void invoke(CartMutation mutation, Context context) {
        String key = config.keyForUser(mutation.getUserId());
        String field = RedisCartCodec.itemField(mutation.getItemId());
        if (mutation.getType() == CartMutation.Type.UPSERT) {
            redis.hset(key, field, RedisCartCodec.encode(mutation));
        } else {
            redis.hdel(key, field);
        }
        redis.expire(key, config.cartTtlSeconds());
    }

    @Override
    public void close() {
        if (redis != null) {
            redis.close();
            redis = null;
        }
    }
}
