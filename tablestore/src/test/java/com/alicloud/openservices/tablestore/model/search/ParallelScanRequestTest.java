package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.core.protocol.BaseSearchTest;
import org.junit.Assert;
import org.junit.Test;

public class ParallelScanRequestTest extends BaseSearchTest {
    @Test
    public void testGetTimeoutInMillisecond() {
        ParallelScanRequest req = ParallelScanRequest.newBuilder()
                .timeout(12000)
                .build();
        Assert.assertEquals(12000, req.getTimeoutInMillisecond());
    }

    @Test
    public void testGetTimeoutInMillisecondDefault() {
        ParallelScanRequest req = ParallelScanRequest.newBuilder()
                .build();
        Assert.assertEquals(-1, req.getTimeoutInMillisecond());
    }

    @Test
    public void testSetTimeoutInMillisecond() {
        ParallelScanRequest req = new ParallelScanRequest();
        req.setTimeoutInMillisecond(59000);
        Assert.assertEquals(59000, req.getTimeoutInMillisecond());
    }
}
