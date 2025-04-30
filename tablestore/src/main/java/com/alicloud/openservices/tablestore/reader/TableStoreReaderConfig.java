package com.alicloud.openservices.tablestore.reader;

public class TableStoreReaderConfig {
    private boolean checkTableMeta = true;
    private int bufferSize = 1024;
    private int concurrency = 10;
    private int maxBatchRowsCount = 100;
    private int defaultMaxVersions = 1;
    private int flushInterval = 10000; // milliseconds

    private int logInterval = 10000; // milliseconds
    private int bucketCount = 4;

    public boolean isCheckTableMeta() {
        return checkTableMeta;
    }

    public void setCheckTableMeta(boolean checkTableMeta) {
        this.checkTableMeta = checkTableMeta;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public int getConcurrency() {
        return concurrency;
    }

    public void setConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }

    public int getMaxBatchRowsCount() {
        return maxBatchRowsCount;
    }

    public void setMaxBatchRowsCount(int maxBatchRowsCount) {
        this.maxBatchRowsCount = maxBatchRowsCount;
    }

    public int getDefaultMaxVersions() {
        return defaultMaxVersions;
    }

    public void setDefaultMaxVersions(int defaultMaxVersions) {
        this.defaultMaxVersions = defaultMaxVersions;
    }

    public int getFlushInterval() {
        return flushInterval;
    }

    public void setFlushInterval(int flushInterval) {
        this.flushInterval = flushInterval;
    }

    public int getLogInterval() {
        return logInterval;
    }

    public void setLogInterval(int logInterval) {
        this.logInterval = logInterval;
    }

    public int getBucketCount() {
        return bucketCount;
    }

    public void setBucketCount(int bucketCount) {
        this.bucketCount = bucketCount;
    }
}
