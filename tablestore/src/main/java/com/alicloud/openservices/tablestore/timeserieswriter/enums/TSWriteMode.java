package com.alicloud.openservices.tablestore.timeserieswriter.enums;

public enum TSWriteMode {
    /**
     * Serial write:
     * Concurrency between different buckets, serial requests within the same bucket.
     */
    SEQUENTIAL,
    /**
     * Parallel write
     * Concurrency between different buckets, and parallel requests within the same bucket as well
     */
    PARALLEL
}
