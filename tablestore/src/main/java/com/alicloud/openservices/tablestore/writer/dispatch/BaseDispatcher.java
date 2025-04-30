package com.alicloud.openservices.tablestore.writer.dispatch;

import java.util.concurrent.atomic.AtomicLong;


public abstract class BaseDispatcher implements Dispatcher {
    private final AtomicLong[] bucketDispatchRowCount;

    public BaseDispatcher(int bucketCount) {
        bucketDispatchRowCount = new AtomicLong[bucketCount];
        for (int i = 0; i < bucketCount; i++) {
            bucketDispatchRowCount[i] = new AtomicLong(0);
        }
    }


    protected void addBucketCount(int bucketId) {
        bucketDispatchRowCount[bucketId].incrementAndGet();
    }


    /**
     * Write statistics for partition buckets
     */
    public AtomicLong[] getBucketDispatchRowCount() {
        return bucketDispatchRowCount;
    }
}
