package com.taobao.behavior.model;

import java.io.Serializable;

public class ItemMetrics1m implements Serializable {
    private static final long serialVersionUID = 1L;

    private long windowStart;
    private long itemId;
    private long sourceCategoryId;
    private long pvCount;
    private long cartCount;
    private long favCount;
    private long buyCount;
    private long uniqueUsers;
    private String replayRunId;

    public ItemMetrics1m() {}

    public ItemMetrics1m(
            long windowStart,
            long itemId,
            long sourceCategoryId,
            long pvCount,
            long cartCount,
            long favCount,
            long buyCount,
            long uniqueUsers,
            String replayRunId) {
        this.windowStart = windowStart;
        this.itemId = itemId;
        this.sourceCategoryId = sourceCategoryId;
        this.pvCount = pvCount;
        this.cartCount = cartCount;
        this.favCount = favCount;
        this.buyCount = buyCount;
        this.uniqueUsers = uniqueUsers;
        this.replayRunId = replayRunId;
    }

    public long getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(long windowStart) {
        this.windowStart = windowStart;
    }

    public long getItemId() {
        return itemId;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    public long getSourceCategoryId() {
        return sourceCategoryId;
    }

    public void setSourceCategoryId(long sourceCategoryId) {
        this.sourceCategoryId = sourceCategoryId;
    }

    public long getPvCount() {
        return pvCount;
    }

    public void setPvCount(long pvCount) {
        this.pvCount = pvCount;
    }

    public long getCartCount() {
        return cartCount;
    }

    public void setCartCount(long cartCount) {
        this.cartCount = cartCount;
    }

    public long getFavCount() {
        return favCount;
    }

    public void setFavCount(long favCount) {
        this.favCount = favCount;
    }

    public long getBuyCount() {
        return buyCount;
    }

    public void setBuyCount(long buyCount) {
        this.buyCount = buyCount;
    }

    public long getUniqueUsers() {
        return uniqueUsers;
    }

    public void setUniqueUsers(long uniqueUsers) {
        this.uniqueUsers = uniqueUsers;
    }

    public String getReplayRunId() {
        return replayRunId;
    }

    public void setReplayRunId(String replayRunId) {
        this.replayRunId = replayRunId;
    }

    @Override
    public String toString() {
        return "ItemMetrics1m{windowStart=" + windowStart + ", itemId=" + itemId
                + ", sourceCategoryId=" + sourceCategoryId + ", pv=" + pvCount + ", cart="
                + cartCount + ", fav=" + favCount + ", buy=" + buyCount
                + ", uniqueUsers=" + uniqueUsers + ", replayRunId='" + replayRunId + "'}";
    }
}
