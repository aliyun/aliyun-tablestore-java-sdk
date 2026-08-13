package com.alicloud.openservices.tablestore.model;

/**
 * Indicates the type of tiered storage policy.
 */
public enum StoragePolicyType {
    /**
     * Tiered by timestamp.
     */
    SPT_BY_TIMESTAMP,
    /**
     * Tiered by a custom time column.
     */
    SPT_BY_COLUMN;
}
