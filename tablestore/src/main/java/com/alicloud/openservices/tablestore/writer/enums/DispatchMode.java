package com.alicloud.openservices.tablestore.writer.enums;


public enum DispatchMode {
    /**
     * Loop through and dispatch bucket distribution
     */
    ROUND_ROBIN,
    /**
     * Hash partition key bucket dispatch
     */
    HASH_PARTITION_KEY,
    /**
     * Hash complete primary key bucket dispatch
     */
    HASH_PRIMARY_KEY,
}
