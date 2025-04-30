package com.alicloud.openservices.tablestore.writer.config;

import com.alicloud.openservices.tablestore.writer.enums.WriteMode;


public class BucketConfig {
    private int bucketId;
    private String tableName;
    private WriteMode writeMode;
    private boolean allowDuplicateRowInBatchRequest;

    public BucketConfig(int bucketId, String tableName,WriteMode writeMode, boolean allowDuplicateRowInBatchRequest) {
        this.bucketId = bucketId;
        this.tableName = tableName;
        this.writeMode = writeMode;
        this.allowDuplicateRowInBatchRequest = allowDuplicateRowInBatchRequest;
    }

    public int getBucketId() {
        return bucketId;
    }

    public String getTableName() {
        return tableName;
    }

    public WriteMode getWriteMode() {
        return writeMode;
    }

    public boolean isAllowDuplicateRowInBatchRequest() {
        return allowDuplicateRowInBatchRequest;
    }
}
