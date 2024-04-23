package com.alicloud.openservices.tablestore.writer.unittest;

import com.alicloud.openservices.tablestore.writer.WriterConfig;
import com.alicloud.openservices.tablestore.writer.enums.BatchRequestType;
import com.alicloud.openservices.tablestore.writer.enums.DispatchMode;
import com.alicloud.openservices.tablestore.writer.enums.WriteMode;
import com.alicloud.openservices.tablestore.writer.enums.WriterRetryStrategy;
import org.junit.Assert;
import org.junit.Test;


public class WriterConfigTest {
    @Test
    public void testParameters() {
        WriterConfig config = new WriterConfig();

        config.setFlushInterval(100000);
        config.setLogInterval(10000);
        config.setCallbackThreadCount(100);
        config.setCallbackThreadPoolQueueSize(4096);
        config.setBucketCount(256);
        config.setBufferSize(1024);
        config.setConcurrency(100);
        config.setMaxAttrColumnSize(20);
        config.setMaxBatchSize(200);
        config.setMaxBatchRowsCount(200);
        config.setMaxColumnsCount(20);
        config.setMaxPKColumnSize(3096);
        config.setClientMaxConnections(900);

        config.setWriteMode(WriteMode.PARALLEL);
        config.setWriterRetryStrategy(WriterRetryStrategy.CERTAIN_ERROR_CODE_NOT_RETRY);
        config.setDispatchMode(DispatchMode.HASH_PARTITION_KEY);
        config.setBatchRequestType(BatchRequestType.BULK_IMPORT);

        config.setEnableSchemaCheck(true);
        config.setAllowDuplicatedRowInBatchRequest(false);


        Assert.assertEquals(100000, config.getFlushInterval());
        Assert.assertEquals(10000, config.getLogInterval());
        Assert.assertEquals(100, config.getCallbackThreadCount());
        Assert.assertEquals(4096, config.getCallbackThreadPoolQueueSize());
        Assert.assertEquals(256, config.getBucketCount());
        Assert.assertEquals(1024, config.getBufferSize());
        Assert.assertEquals(100, config.getConcurrency());
        Assert.assertEquals(20, config.getMaxAttrColumnSize());
        Assert.assertEquals(200, config.getMaxBatchSize());
        Assert.assertEquals(200, config.getMaxBatchRowsCount());
        Assert.assertEquals(20, config.getMaxColumnsCount());
        Assert.assertEquals(3096, config.getMaxPKColumnSize());
        Assert.assertEquals(900, config.getClientMaxConnections());

        Assert.assertEquals(WriteMode.PARALLEL, config.getWriteMode());
        Assert.assertEquals(WriterRetryStrategy.CERTAIN_ERROR_CODE_NOT_RETRY, config.getWriterRetryStrategy());
        Assert.assertEquals(DispatchMode.HASH_PARTITION_KEY, config.getDispatchMode());
        Assert.assertEquals(BatchRequestType.BULK_IMPORT, config.getBatchRequestType());


        Assert.assertTrue(config.isEnableSchemaCheck());
        Assert.assertFalse(config.isAllowDuplicatedRowInBatchRequest());

    }

    @Test
    public void testParameterExceptions() {
        WriterConfig config = new WriterConfig();

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
            config.setMaxAttrColumnSize(0);
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

        try {
            config.setMaxColumnsCount(0);
            Assert.fail();
        } catch (Exception e) {}

        try {
            config.setMaxPKColumnSize(0);
            Assert.fail();
        } catch (Exception e) {}
    }
}
