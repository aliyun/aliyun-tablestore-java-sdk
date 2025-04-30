package com.alicloud.openservices.tablestore.timeserieswriter.dispatch;


import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesRow;

import java.util.concurrent.atomic.AtomicLong;


public class TimeseriesRoundRobinDispatcher extends TimeseriesBaseDispatcher {
    private AtomicLong counter = new AtomicLong(0);
    private int bucketCount;

    public TimeseriesRoundRobinDispatcher(int bucketCount) {
        super(bucketCount);
        this.bucketCount = bucketCount;
    }

    @Override
    public int getDispatchIndex(TimeseriesRow timeseriesRow) {
        int bucketIndex = (int) counter.getAndIncrement() % bucketCount;
        addBucketCount(bucketIndex);

        return bucketIndex;
    }
}
