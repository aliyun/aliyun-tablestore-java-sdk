package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.core.protocol.BaseSearchTest;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.query.QueryBuilders;
import com.alicloud.openservices.tablestore.model.search.query.RangeQuery;
import org.junit.Assert;
import org.junit.Test;

public class ScanQueryTest extends BaseSearchTest {

    @Test
    public void testGetSetLimit() {
        ScanQuery s1 = new ScanQuery();
        s1.setLimit(99);
        ParallelScanRequest build1 = ParallelScanRequest.newBuilder().scanQuery(s1).build();

        ScanQuery s2 = ScanQuery.newBuilder().limit(99).build();
        ParallelScanRequest build2 = ParallelScanRequest.newBuilder().scanQuery(s2).build();

        Assert.assertEquals(build1.getRequestInfo(true), build2.getRequestInfo(true));
        Assert.assertNotSame(build1.getRequestInfo(true), build2.getRequestInfo(false));
    }

    @Test
    public void testGetSetMaxParallel() {
        ScanQuery s1 = new ScanQuery();
        s1.setMaxParallel(99);
        ParallelScanRequest build1 = ParallelScanRequest.newBuilder().scanQuery(s1).build();

        ScanQuery s2 = ScanQuery.newBuilder().maxParallel(99).build();
        ParallelScanRequest build2 = ParallelScanRequest.newBuilder().scanQuery(s2).build();

        Assert.assertEquals(build1.getRequestInfo(true), build2.getRequestInfo(true));
        Assert.assertNotSame(build1.getRequestInfo(true), build2.getRequestInfo(false));
    }

    @Test
    public void testGetSetCurrentParallelId() {
        ScanQuery s1 = new ScanQuery();
        s1.setCurrentParallelId(99);
        ParallelScanRequest build1 = ParallelScanRequest.newBuilder().scanQuery(s1).build();

        ScanQuery s2 = ScanQuery.newBuilder().currentParallelId(99).build();
        ParallelScanRequest build2 = ParallelScanRequest.newBuilder().scanQuery(s2).build();

        Assert.assertEquals(build1.getRequestInfo(true), build2.getRequestInfo(true));
        Assert.assertNotSame(build1.getRequestInfo(true), build2.getRequestInfo(false));
    }

    @Test
    public void testGetSetAliveTime() {
        ScanQuery s1 = new ScanQuery();
        s1.setAliveTime(99);
        ParallelScanRequest build1 = ParallelScanRequest.newBuilder().scanQuery(s1).build();

        ScanQuery s2 = ScanQuery.newBuilder().aliveTimeInSeconds(91119).aliveTimeInSeconds(99).build();
        ParallelScanRequest build2 = ParallelScanRequest.newBuilder().scanQuery(s2).build();

        Assert.assertEquals(build1.getRequestInfo(true), build2.getRequestInfo(true));
        Assert.assertNotSame(build1.getRequestInfo(true), build2.getRequestInfo(false));
    }

    @Test
    public void testGetAndSetQuery() {
        ScanQuery s1 = new ScanQuery();
        RangeQuery query = new RangeQuery();
        query.setFieldName("f1");
        query.setFrom(ColumnValue.fromLong(123), true);
        s1.setQuery(query);
        ParallelScanRequest build1 = ParallelScanRequest.newBuilder().scanQuery(s1).build();

        ScanQuery s2 = ScanQuery.newBuilder()
            .query(QueryBuilders.range("f1").greaterThanOrEqual(123))
            .build();
        ParallelScanRequest build2 = ParallelScanRequest.newBuilder().scanQuery(s2).build();

        Assert.assertEquals(build1.getRequestInfo(true), build2.getRequestInfo(true));
        Assert.assertNotSame(build1.getRequestInfo(true), build2.getRequestInfo(false));
    }

    @Test
    public void testGetAndSetToken() {
        ScanQuery s1 = new ScanQuery();
        s1.setToken("fsfsf".getBytes());
        ParallelScanRequest build1 = ParallelScanRequest.newBuilder().scanQuery(s1).build();

        ScanQuery s2 = ScanQuery.newBuilder().token("fsfsf".getBytes()).build();
        ParallelScanRequest build2 = ParallelScanRequest.newBuilder().scanQuery(s2).build();

        Assert.assertEquals(build1.getRequestInfo(true), build2.getRequestInfo(true));
        Assert.assertNotSame(build1.getRequestInfo(true), build2.getRequestInfo(false));
    }

    @Test
    public void testGetAndSetNull() {
        ScanQuery s1 = new ScanQuery();
        ParallelScanRequest build1 = ParallelScanRequest.newBuilder().scanQuery(s1).build();

        ScanQuery s2 = ScanQuery.newBuilder().build();
        ParallelScanRequest build2 = ParallelScanRequest.newBuilder().scanQuery(s2).build();

        Assert.assertEquals(build1.getRequestInfo(true), build2.getRequestInfo(true));
        Assert.assertNotSame(build1.getRequestInfo(true), build2.getRequestInfo(false));
    }

    @Test
    public void testSessionId() {
        ScanQuery s1 = new ScanQuery();
        ParallelScanRequest build1 = ParallelScanRequest.newBuilder().scanQuery(s1).sessionId("fsfsf".getBytes()).build();

        ScanQuery s2 = ScanQuery.newBuilder().build();
        ParallelScanRequest build2 = new ParallelScanRequest();
        build2.setScanQuery(s2).setSessionId("fsfsf".getBytes());

        Assert.assertEquals(build1.getRequestInfo(true), build2.getRequestInfo(true));
        Assert.assertNotSame(build1.getRequestInfo(true), build2.getRequestInfo(false));
    }
}
