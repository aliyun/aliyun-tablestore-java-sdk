package com.alicloud.openservices.tablestore.timeserieswriter.config;

import com.alicloud.openservices.tablestore.timeserieswriter.enums.TSWriteMode;

public class TimeseriesBucketConfig {
    private int bucketId;
    private TSWriteMode writeMode;
    private boolean allowDuplicateRowInBatchRequest;

    public TimeseriesBucketConfig(int bucketId, TSWriteMode writeMode, boolean allowDuplicateRowInBatchRequest) {
        this.bucketId = bucketId;
        this.writeMode = writeMode;
        this.allowDuplicateRowInBatchRequest = allowDuplicateRowInBatchRequest;
    }

    public int getBucketId() {
        return bucketId;
    }

    public TSWriteMode getWriteMode() {
        return writeMode;
    }

    public boolean isAllowDuplicateRowInBatchRequest() {
        return allowDuplicateRowInBatchRequest;
    }

}
