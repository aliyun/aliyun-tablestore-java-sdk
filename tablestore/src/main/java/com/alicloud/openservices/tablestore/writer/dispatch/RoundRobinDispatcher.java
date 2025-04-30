package com.alicloud.openservices.tablestore.writer.dispatch;


import com.alicloud.openservices.tablestore.model.*;

import java.util.concurrent.atomic.AtomicLong;


public class RoundRobinDispatcher extends BaseDispatcher {
    private AtomicLong counter = new AtomicLong(0);
    private int bucketCount;

    public RoundRobinDispatcher(int bucketCount) {
        super(bucketCount);
        this.bucketCount = bucketCount;
    }
    @Override
    public int getDispatchIndex(RowChange rowChange) {
        int bucketIndex = (int)counter.getAndIncrement() % bucketCount;
        addBucketCount(bucketIndex);

        return bucketIndex;
    }
}
