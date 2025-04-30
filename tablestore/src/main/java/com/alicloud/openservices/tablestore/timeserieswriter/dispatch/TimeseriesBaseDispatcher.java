package com.alicloud.openservices.tablestore.timeserieswriter.dispatch;

import java.util.concurrent.atomic.AtomicLong;


public abstract class TimeseriesBaseDispatcher implements TimeseriesDispatcher {
    private final AtomicLong[] bucketDispatchRowCount;

    public TimeseriesBaseDispatcher(int bucketCount) {
        bucketDispatchRowCount = new AtomicLong[bucketCount];
        for (int i = 0; i < bucketCount; i++) {
            bucketDispatchRowCount[i] = new AtomicLong(0);
        }
    }


    protected void addBucketCount(int bucketId) {
        bucketDispatchRowCount[bucketId].incrementAndGet();
    }


    /**
     * Bucket writing statistics
     */
    public AtomicLong[] getBucketDispatchRowCount() {
        return bucketDispatchRowCount;
    }
}
