package com.alicloud.openservices.tablestore.timeserieswriter.unittest;

import com.alicloud.openservices.tablestore.timeserieswriter.config.TimeseriesWriterConfig;
import com.alicloud.openservices.tablestore.timeserieswriter.enums.TSDispatchMode;
import com.alicloud.openservices.tablestore.timeserieswriter.enums.TSWriteMode;
import com.alicloud.openservices.tablestore.writer.enums.WriterRetryStrategy;
import org.junit.Assert;
import org.junit.Test;

public class TimeseriesWriterConfigTest {
    @Test
    public void testParameters() {
        TimeseriesWriterConfig config = new TimeseriesWriterConfig();

        config.setFlushInterval(100000);
        config.setLogInterval(10000);
        config.setCallbackThreadCount(100);
        config.setCallbackThreadPoolQueueSize(4096);
        config.setBucketCount(256);
        config.setBufferSize(1024);
        config.setConcurrency(100);
        config.setMaxBatchSize(200);
        config.setMaxBatchRowsCount(200);
        config.setClientMaxConnections(900);

        config.setWriteMode(TSWriteMode.PARALLEL);
        config.setWriterRetryStrategy(WriterRetryStrategy.CERTAIN_ERROR_CODE_NOT_RETRY);
        config.setDispatchMode(TSDispatchMode.HASH_PRIMARY_KEY);

        config.setAllowDuplicatedRowInBatchRequest(false);


        Assert.assertEquals(100000, config.getFlushInterval());
        Assert.assertEquals(10000, config.getLogInterval());
        Assert.assertEquals(100, config.getCallbackThreadCount());
        Assert.assertEquals(4096, config.getCallbackThreadPoolQueueSize());
        Assert.assertEquals(256, config.getBucketCount());
        Assert.assertEquals(1024, config.getBufferSize());
        Assert.assertEquals(100, config.getConcurrency());
        Assert.assertEquals(200, config.getMaxBatchSize());
        Assert.assertEquals(200, config.getMaxBatchRowsCount());
        Assert.assertEquals(900, config.getClientMaxConnections());

        Assert.assertEquals(TSWriteMode.PARALLEL, config.getWriteMode());
        Assert.assertEquals(WriterRetryStrategy.CERTAIN_ERROR_CODE_NOT_RETRY, config.getWriterRetryStrategy());
        Assert.assertEquals(TSDispatchMode.HASH_PRIMARY_KEY, config.getDispatchMode());
        Assert.assertFalse(config.isAllowDuplicatedRowInBatchRequest());
    }

    @Test
    public void testParameterExceptions() {
        TimeseriesWriterConfig config = new TimeseriesWriterConfig();

        try {
            config.setFlushInterval(0);
            Assert.fail();
        } catch (Exception e) {}

        try {
            config.setLogInterval(0);
            Assert.fail();
        } catch (Exception e) {}

        try {
            config.setCallbackThreadCount(0);
            Assert.fail();
        } catch (Exception e) {}

        try {
            config.setCallbackThreadPoolQueueSize(0);
            Assert.fail();
        } catch (Exception e) {}

        try {
            config.setBucketCount(0);
            Assert.fail();
        } catch (Exception e) {}

        try {
            config.setBufferSize(0);
            Assert.fail();
        } catch (Exception e) {}

        try {
            config.setConcurrency(0);
            Assert.fail();
        } catch (Exception e) {}

        try {
            config.setMaxBatchSize(0);
            Assert.fail();
        } catch (Exception e) {}

        try {
            config.setMaxBatchRowsCount(0);
            Assert.fail();
        } catch (Exception e) {}


    }
}
