package com.alicloud.openservices.tablestore.timeserieswriter.enums;


public enum TSDispatchMode {
    /**
     * Loop through the partition buckets and dispatch
     */
    ROUND_ROBIN,

    /**
     * Hash complete primary key bucket dispatch
     */
    HASH_PRIMARY_KEY,
}
