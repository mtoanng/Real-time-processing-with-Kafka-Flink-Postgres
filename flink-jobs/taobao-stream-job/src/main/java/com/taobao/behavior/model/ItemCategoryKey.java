package com.taobao.behavior.model;

import java.io.Serializable;
import java.util.Objects;

/** Replay-independent business key using the category carried by the Taobao source event. */
public class ItemCategoryKey implements Serializable {
    private static final long serialVersionUID = 1L;

    private long itemId;
    private long sourceCategoryId;

    public ItemCategoryKey() {}

    public ItemCategoryKey(long itemId, long sourceCategoryId) {
        this.itemId = itemId;
        this.sourceCategoryId = sourceCategoryId;
    }

    public long getItemId() {
        return itemId;
    }

    public long getSourceCategoryId() {
        return sourceCategoryId;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ItemCategoryKey)) {
            return false;
        }
        ItemCategoryKey that = (ItemCategoryKey) other;
        return itemId == that.itemId && sourceCategoryId == that.sourceCategoryId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemId, sourceCategoryId);
    }
}
