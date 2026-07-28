package com.taobao.behavior.sink;

import com.taobao.behavior.model.CartMutation;

public final class RedisCartCodec {
    private RedisCartCodec() {}

    public static String itemField(long itemId) {
        if (itemId <= 0L) {
            throw new IllegalArgumentException("item ID must be positive");
        }
        return Long.toString(itemId);
    }

    public static String encode(CartMutation item) {
        if (item == null
                || item.getCategoryId() <= 0L
                || item.getAddedAtMs() < 0L
                || item.getLastUpdatedAtMs() < 0L) {
            throw new IllegalArgumentException("active-cart item contains invalid values");
        }
        return item.getCategoryId()
                + "|"
                + item.getAddedAtMs()
                + "|"
                + item.getLastUpdatedAtMs();
    }

    public static DecodedCartItem decode(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Redis cart value must not be null");
        }
        String[] fields = value.split("\\|", -1);
        if (fields.length != 3) {
            throw new IllegalArgumentException("Redis cart value must contain three fields");
        }
        try {
            long categoryId = Long.parseLong(fields[0]);
            long addedAtMs = Long.parseLong(fields[1]);
            long lastUpdatedAtMs = Long.parseLong(fields[2]);
            if (categoryId <= 0L || addedAtMs < 0L || lastUpdatedAtMs < 0L) {
                throw new NumberFormatException("invalid value range");
            }
            return new DecodedCartItem(categoryId, addedAtMs, lastUpdatedAtMs);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("Redis cart value contains invalid numbers", exception);
        }
    }

    public static final class DecodedCartItem {
        private final long categoryId;
        private final long addedAtMs;
        private final long lastUpdatedAtMs;

        private DecodedCartItem(long categoryId, long addedAtMs, long lastUpdatedAtMs) {
            this.categoryId = categoryId;
            this.addedAtMs = addedAtMs;
            this.lastUpdatedAtMs = lastUpdatedAtMs;
        }

        public long categoryId() {
            return categoryId;
        }

        public long addedAtMs() {
            return addedAtMs;
        }

        public long lastUpdatedAtMs() {
            return lastUpdatedAtMs;
        }
    }
}
