package com.taobao.behavior.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Objects;

public final class ProductCatalogRecord implements Serializable {
    private final long productId;
    private final long categoryId;
    private final String productName;
    private final BigDecimal price;
    private final boolean active;
    private final long updatedAtMs;
    private final long catalogVersion;

    public ProductCatalogRecord(
            long productId,
            long categoryId,
            String productName,
            BigDecimal price,
            boolean active,
            long updatedAtMs,
            long catalogVersion) {
        if (productId <= 0L || categoryId <= 0L) {
            throw new IllegalArgumentException("product and category IDs must be positive");
        }
        if (productName == null || productName.isBlank()) {
            throw new IllegalArgumentException("product name must not be blank");
        }
        if (price == null
                || price.signum() < 0
                || price.scale() > 2
                || price.precision() > 12) {
            throw new IllegalArgumentException("price must fit Decimal(12,2) and be non-negative");
        }
        if (updatedAtMs < 0L || catalogVersion <= 0L) {
            throw new IllegalArgumentException(
                    "updated_at must be non-negative and catalog_version must be positive");
        }
        this.productId = productId;
        this.categoryId = categoryId;
        this.productName = productName;
        this.price = price;
        this.active = active;
        this.updatedAtMs = updatedAtMs;
        this.catalogVersion = catalogVersion;
    }

    public long getProductId() {
        return productId;
    }

    public long getCategoryId() {
        return categoryId;
    }

    public String getProductName() {
        return productName;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public boolean isActive() {
        return active;
    }

    public long getUpdatedAtMs() {
        return updatedAtMs;
    }

    public long getCatalogVersion() {
        return catalogVersion;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ProductCatalogRecord)) {
            return false;
        }
        ProductCatalogRecord that = (ProductCatalogRecord) other;
        return productId == that.productId
                && categoryId == that.categoryId
                && active == that.active
                && updatedAtMs == that.updatedAtMs
                && catalogVersion == that.catalogVersion
                && productName.equals(that.productName)
                && price.compareTo(that.price) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                productId,
                categoryId,
                productName,
                price.stripTrailingZeros(),
                active,
                updatedAtMs,
                catalogVersion);
    }
}
