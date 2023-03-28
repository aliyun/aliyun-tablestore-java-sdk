package com.alicloud.openservices.tablestore.timeserieswriter.unittest;

import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesKey;
import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesRow;
import com.alicloud.openservices.tablestore.timeserieswriter.dispatch.TimeseriesHashPKDispatcher;
import com.alicloud.openservices.tablestore.timeserieswriter.dispatch.TimeseriesRoundRobinDispatcher;
import com.alicloud.openservices.tablestore.writer.dispatch.RoundRobinDispatcher;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class TestDispatcher {

    static int BUCKET_COUNT = (int) Math.pow(2, (int) (Math.random() * 10));
    static final int TOTAL_PRIMARYKEY = 1000000;

    @Test
    public void testTimeseriesHashPKDispatcher() {
        BUCKET_COUNT = 16;

        TimeseriesHashPKDispatcher dispatcher = new TimeseriesHashPKDispatcher(BUCKET_COUNT);
        long start = System.currentTimeMillis();

        for (int i = 0; i < TOTAL_PRIMARYKEY; i++) {
            Map<String, String> tags = new HashMap<String, String>();
            tags.put("region", "hangzhou");
            tags.put("os", "Ubuntu16.04");
            TimeseriesKey timeseriesKey = new TimeseriesKey("cpu" + i, "host_" + i, tags);
            TimeseriesRow row = new TimeseriesRow(timeseriesKey, System.currentTimeMillis() * 1000 + i);
            dispatcher.getDispatchIndex(row);
        }
        System.out.println("TimeseriesHashPKDispatcher Cost: " + (System.currentTimeMillis() - start));


        System.out.println("Bucket Count: " + Arrays.asList(dispatcher.getBucketDispatchRowCount()));
        Assert.assertEquals(dispatcher.getBucketDispatchRowCount().length, BUCKET_COUNT);

        int total = 0;
        for (AtomicLong bucket : dispatcher.getBucketDispatchRowCount()) {
            total += bucket.get();
        }
        Assert.assertEquals(total, TOTAL_PRIMARYKEY);
    }

    @Test
    public void testLoopDispatcher() {

        TimeseriesRoundRobinDispatcher dispatcher = new TimeseriesRoundRobinDispatcher(BUCKET_COUNT);
        long start = System.currentTimeMillis();

        for (int i = 0; i < TOTAL_PRIMARYKEY; i++) {
            Map<String, String> tags = new HashMap<String, String>();
            tags.put("region", "hangzhou");
            tags.put("os", "Ubuntu16.04");
            TimeseriesKey timeseriesKey = new TimeseriesKey("cpu" + i, "host_" + i, tags);
            TimeseriesRow row = new TimeseriesRow(timeseriesKey, System.currentTimeMillis() * 1000 + i);
            dispatcher.getDispatchIndex(row);
        }
        System.out.println("RoundRobinDispatcher Cost: " + (System.currentTimeMillis() - start));


        System.out.println("Bucket Count: " + Arrays.asList(dispatcher.getBucketDispatchRowCount()));
        Assert.assertEquals(dispatcher.getBucketDispatchRowCount().length, BUCKET_COUNT);

        int total = 0;
        for (AtomicLong bucket : dispatcher.getBucketDispatchRowCount()) {
            total += bucket.get();
        }
        Assert.assertEquals(total, TOTAL_PRIMARYKEY);
    }
}
