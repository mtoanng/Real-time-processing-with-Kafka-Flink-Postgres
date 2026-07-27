package com.taobao.behavior.sink;

import java.io.Serializable;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.JedisPooled;

public class RedisClientFactory implements Serializable {
    private static final long serialVersionUID = 1L;

    RedisCommandsClient open(RedisConfig config) {
        DefaultJedisClientConfig.Builder builder =
                DefaultJedisClientConfig.builder()
                        .ssl(config.tls())
                        .connectionTimeoutMillis((int) config.connectTimeout().toMillis())
                        .socketTimeoutMillis((int) config.socketTimeout().toMillis());
        if (config.username() != null) {
            builder.user(config.username());
        }
        if (config.password() != null) {
            builder.password(config.password());
        }
        JedisClientConfig clientConfig = builder.build();
        JedisPooled jedis =
                new JedisPooled(new HostAndPort(config.host(), config.port()), clientConfig);
        return new JedisAdapter(jedis);
    }

    private static final class JedisAdapter implements RedisCommandsClient {
        private final JedisPooled jedis;

        private JedisAdapter(JedisPooled jedis) {
            this.jedis = jedis;
        }

        @Override
        public long hset(String key, String field, String value) {
            return jedis.hset(key, field, value);
        }

        @Override
        public long hdel(String key, String field) {
            return jedis.hdel(key, field);
        }

        @Override
        public long expire(String key, long seconds) {
            return jedis.expire(key, seconds);
        }

        @Override
        public String ping() {
            return jedis.ping();
        }

        @Override
        public void close() {
            jedis.close();
        }
    }
}
