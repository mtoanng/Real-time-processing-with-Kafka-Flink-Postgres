package com.taobao.behavior;

import com.taobao.behavior.sink.RedisCartCodec;
import com.taobao.behavior.sink.RedisConfig;
import java.util.Comparator;
import java.util.Map;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPooled;

public final class ActiveCartLookupCli {
    private ActiveCartLookupCli() {}

    public static void main(String[] args) {
        long userId = parseUserId(args);
        RedisConfig config = RedisConfig.fromEnvironment(System.getenv());
        DefaultJedisClientConfig.Builder clientConfig =
                DefaultJedisClientConfig.builder()
                        .ssl(config.tls())
                        .connectionTimeoutMillis((int) config.connectTimeout().toMillis())
                        .socketTimeoutMillis((int) config.socketTimeout().toMillis());
        if (config.username() != null) {
            clientConfig.user(config.username());
        }
        if (config.password() != null) {
            clientConfig.password(config.password());
        }
        try (JedisPooled redis =
                new JedisPooled(
                        new HostAndPort(config.host(), config.port()), clientConfig.build())) {
            Map<String, String> items = redis.hgetAll(config.keyForUser(userId));
            if (items.isEmpty()) {
                System.out.println("NOT FOUND user_id=" + userId);
                return;
            }
            items.entrySet().stream()
                    .sorted(Comparator.comparingLong(entry -> Long.parseLong(entry.getKey())))
                    .forEach(entry -> printItem(userId, entry.getKey(), entry.getValue()));
        }
    }

    static void printItem(long userId, String itemField, String encodedValue) {
        long itemId = Long.parseLong(itemField);
        RedisCartCodec.DecodedCartItem item = RedisCartCodec.decode(encodedValue);
        System.out.printf(
                "user_id=%d item_id=%d category_id=%d added_at_ms=%d last_updated_at_ms=%d%n",
                userId,
                itemId,
                item.categoryId(),
                item.addedAtMs(),
                item.lastUpdatedAtMs());
    }

    static long parseUserId(String[] args) {
        if (args.length != 2 || !"--user-id".equals(args[0])) {
            throw new IllegalArgumentException("usage: lookup-active-cart --user-id <positive-user-id>");
        }
        long userId = Long.parseLong(args[1]);
        if (userId <= 0) {
            throw new IllegalArgumentException("user ID must be positive");
        }
        return userId;
    }
}
