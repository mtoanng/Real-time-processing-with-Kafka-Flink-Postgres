package com.taobao.behavior.model;

import java.io.Serializable;

public final class CartMutation implements Serializable {
    public enum Type { UPSERT, DELETE }

    private final Type type;
    private final long userId;
    private final long itemId;
    private final long categoryId;
    private final long addedAtMs;
    private final long lastUpdatedAtMs;

    public CartMutation(
            Type type,
            long userId,
            long itemId,
            long categoryId,
            long addedAtMs,
            long lastUpdatedAtMs) {
        this.type = type;
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.addedAtMs = addedAtMs;
        this.lastUpdatedAtMs = lastUpdatedAtMs;
    }

    public Type getType() { return type; }
    public long getUserId() { return userId; }
    public long getItemId() { return itemId; }
    public long getCategoryId() { return categoryId; }
    public long getAddedAtMs() { return addedAtMs; }
    public long getLastUpdatedAtMs() { return lastUpdatedAtMs; }
}
