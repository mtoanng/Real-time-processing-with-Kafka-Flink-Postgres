package com.taobao.behavior.model;

import java.io.Serializable;

public class CartItemState implements Serializable {
    private static final long serialVersionUID = 1L;

    private boolean active;
    private long categoryId;
    private long addedAtMs;
    private long lastUpdatedAtMs;
    private long lastEventTimeMs;
    private long lastSourceSequence;

    public CartItemState() {}

    public CartItemState(boolean active, long categoryId, long addedAtMs, long lastUpdatedAtMs, long lastEventTimeMs, long lastSourceSequence) {
        this.active = active;
        this.categoryId = categoryId;
        this.addedAtMs = addedAtMs;
        this.lastUpdatedAtMs = lastUpdatedAtMs;
        this.lastEventTimeMs = lastEventTimeMs;
        this.lastSourceSequence = lastSourceSequence;
    }

    public boolean isActive() { return active; }
    public long getCategoryId() { return categoryId; }
    public long getAddedAtMs() { return addedAtMs; }
    public long getLastUpdatedAtMs() { return lastUpdatedAtMs; }
    public long getLastEventTimeMs() { return lastEventTimeMs; }
    public long getLastSourceSequence() { return lastSourceSequence; }
}
