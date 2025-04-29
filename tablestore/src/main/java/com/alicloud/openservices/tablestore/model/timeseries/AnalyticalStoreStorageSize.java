package com.alicloud.openservices.tablestore.model.timeseries;

public class AnalyticalStoreStorageSize {

    private long sizeInBytes;

    private long timestamp;

    public long getSizeInBytes() {
        return sizeInBytes;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setSizeInBytes(long sizeInBytes) {
        this.sizeInBytes = sizeInBytes;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
