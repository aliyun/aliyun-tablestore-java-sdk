package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;
import com.alicloud.openservices.tablestore.core.utils.OptionalValue;

/**
 * The tiered storage policy for a table, controlling how data is migrated between hot and cold storage tiers.
 */
public class TieredStoragePolicy implements Jsonizable {

    /**
     * Whether to enable tiered storage.
     */
    private boolean enableTieredStorage = false;

    /**
     * The field to use for tiering. Required when enableTieredStorage is true.
     * Valid values: SPT_BY_TIMESTAMP (tier by timestamp), SPT_BY_COLUMN (tier by a custom time column).
     */
    private OptionalValue<StoragePolicyType> type = new OptionalValue<StoragePolicyType>("Type");

    /**
     * The time point at which hot data transitions to cold storage, in seconds.
     * Required when enableTieredStorage is true.
     * For example, a value of 86400 means data older than one day (86400 seconds) from the current time
     * will be migrated to cold storage.
     */
    private OptionalValue<Long> hotRetentionPeriod = new OptionalValue<Long>("HotRetentionPeriod");

    /**
     * The properties of the custom time column. Required when type is SPT_BY_COLUMN.
     */
    private OptionalValue<TieredStorageColumn> column = new OptionalValue<TieredStorageColumn>("Column");

    /**
     * Construct a TieredStoragePolicy instance with default values.
     */
    public TieredStoragePolicy() {
    }

    /**
     * Construct a TieredStoragePolicy instance.
     *
     * @param enableTieredStorage Whether to enable tiered storage.
     */
    public TieredStoragePolicy(boolean enableTieredStorage) {
        this.enableTieredStorage = enableTieredStorage;
    }

    /**
     * Get whether tiered storage is enabled.
     *
     * @return Whether tiered storage is enabled.
     */
    public boolean isEnableTieredStorage() {
        return enableTieredStorage;
    }

    /**
     * Set whether to enable tiered storage.
     *
     * @param enableTieredStorage Whether to enable tiered storage.
     */
    public void setEnableTieredStorage(boolean enableTieredStorage) {
        this.enableTieredStorage = enableTieredStorage;
    }

    /**
     * Get the storage policy type.
     *
     * @return The storage policy type, or null if not set.
     */
    public StoragePolicyType getType() {
        return type.getValue();
    }

    /**
     * Set the storage policy type.
     *
     * @param type The storage policy type.
     */
    public void setType(StoragePolicyType type) {
        this.type.setValue(type);
    }

    /**
     * Get the hot data retention period in seconds.
     *
     * @return The hot data retention period in seconds, or null if not set.
     */
    public Long getHotRetentionPeriod() {
        return hotRetentionPeriod.getValue();
    }

    /**
     * Set the hot data retention period in seconds.
     *
     * @param hotRetentionPeriod The hot data retention period in seconds.
     */
    public void setHotRetentionPeriod(long hotRetentionPeriod) {
        this.hotRetentionPeriod.setValue(hotRetentionPeriod);
    }

    /**
     * Get the custom time column properties.
     *
     * @return The custom time column properties, or null if not set.
     */
    public TieredStorageColumn getColumn() {
        return column.getValue();
    }

    /**
     * Set the custom time column properties.
     *
     * @param column The custom time column properties.
     */
    public void setColumn(TieredStorageColumn column) {
        this.column.setValue(column);
    }

    @Override
    public String jsonize() {
        StringBuilder sb = new StringBuilder();
        jsonize(sb, "\n  ");
        return sb.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        sb.append('{');
        sb.append(newline);
        sb.append("\"EnableTieredStorage\": ");
        sb.append(enableTieredStorage);
        if (type.isValueSet()) {
            sb.append(",");
            sb.append(newline);
            sb.append("\"Type\": \"");
            sb.append(type.getValue().toString());
            sb.append("\"");
        }
        if (hotRetentionPeriod.isValueSet()) {
            sb.append(",");
            sb.append(newline);
            sb.append("\"HotRetentionPeriod\": ");
            sb.append(hotRetentionPeriod.getValue());
        }
        if (column.isValueSet()) {
            sb.append(",");
            sb.append(newline);
            sb.append("\"Column\": ");
            column.getValue().jsonize(sb, newline + "  ");
        }
        sb.append("}");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("EnableTieredStorage: ").append(enableTieredStorage);
        if (type.isValueSet()) {
            sb.append(", Type: ").append(type.getValue().toString());
        }
        if (hotRetentionPeriod.isValueSet()) {
            sb.append(", HotRetentionPeriod: ").append(hotRetentionPeriod.getValue());
        }
        if (column.isValueSet()) {
            sb.append(", Column: ").append(column.getValue().toString());
        }
        return sb.toString();
    }
}
