package com.taobao.behavior.sink;

interface RedisCommandsClient extends AutoCloseable {
    long hset(String key, String field, String value);

    long hdel(String key, String field);

    long expire(String key, long seconds);

    String ping();

    @Override
    void close();
}
