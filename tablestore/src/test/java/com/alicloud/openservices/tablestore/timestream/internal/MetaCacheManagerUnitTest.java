package com.alicloud.openservices.tablestore.timestream.internal;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.model.ConsumedCapacity;
import com.alicloud.openservices.tablestore.model.RowChange;
import com.alicloud.openservices.tablestore.writer.RowWriteResult;
import com.alicloud.openservices.tablestore.writer.WriterConfig;
import com.alicloud.openservices.tablestore.writer.WriterResult;
import com.alicloud.openservices.tablestore.writer.WriterStatistics;
import com.alicloud.openservices.tablestore.timestream.model.TimestreamIdentifier;
import junit.framework.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.Future;

public class MetaCacheManagerUnitTest {

    @Test
    public void testWeigh() {
        // test weigh function is ok, old data will be swap out if cache is full
        long expireTimeInSec = 1000;
        TimestreamIdentifier meta1 = new TimestreamIdentifier.Builder("cpu").addTag("tag1", "0").build();
        TimestreamIdentifier meta2 = new TimestreamIdentifier.Builder("cpu").addTag("tag1", "1").build();
        TimestreamIdentifier meta3 = new TimestreamIdentifier.Builder("cpu").addTag("tag1", "2").build();
        long cacheSize = 2 * (meta1.getDataSize() + 8);
        MetaCacheManager cacheManager = new MetaCacheManager("table", expireTimeInSec, cacheSize, null);
        long stemp = expireTimeInSec * 1000 * 1000;
        long now = System.currentTimeMillis() * 1000;
        cacheManager.updateTimestreamMeta(meta1, now);
        Assert.assertTrue(cacheManager.getTimestreamMetaLastUpdateTime(meta1) == now);
        cacheManager.updateTimestreamMeta(meta2, now);
        Assert.assertTrue(cacheManager.getTimestreamMetaLastUpdateTime(meta1) == now);
        Assert.assertTrue(cacheManager.getTimestreamMetaLastUpdateTime(meta2) == now);
        cacheManager.updateTimestreamMeta(meta3, now + stemp);
        Assert.assertTrue(cacheManager.getTimestreamMetaLastUpdateTime(meta1) == null);
        Assert.assertTrue(cacheManager.getTimestreamMetaLastUpdateTime(meta2) == now);
        Assert.assertTrue(cacheManager.getTimestreamMetaLastUpdateTime(meta3) == now + stemp);
        cacheManager.updateTimestreamMeta(meta2, now + stemp);
        cacheManager.updateTimestreamMeta(meta1, now);
        Assert.assertTrue(cacheManager.getTimestreamMetaLastUpdateTime(meta1) == now);
        Assert.assertTrue(cacheManager.getTimestreamMetaLastUpdateTime(meta2) == now + stemp);
        Assert.assertTrue(cacheManager.getTimestreamMetaLastUpdateTime(meta3) == null);
    }

    @Test
    public void testExpire() throws InterruptedException {
        // test weigh function is ok, old data will be swap out if cache is full
        long expireTimeInSec = 2;
        TimestreamIdentifier meta1 = new TimestreamIdentifier.Builder("cpu").addTag("tag1", "0").build();
        TimestreamIdentifier meta2 = new TimestreamIdentifier.Builder("cpu").addTag("tag1", "1").build();
        TimestreamIdentifier meta3 = new TimestreamIdentifier.Builder("cpu").addTag("tag2", "3").build();
        long cacheSize = 2 * (meta1.getDataSize() + 8);
        MetaCacheManager cacheManager = new MetaCacheManager("table", expireTimeInSec, cacheSize, null);
        long stemp = expireTimeInSec * 1000 * 1000;
        long now = System.currentTimeMillis() * 1000;
        cacheManager.updateTimestreamMeta(meta2, now);
        cacheManager.updateTimestreamMeta(meta1, now);
        Assert.assertTrue(cacheManager.getTimestreamMetaLastUpdateTime(meta1) == now);
        Assert.assertTrue(cacheManager.getTimestreamMetaLastUpdateTime(meta2) == now);
        Thread.sleep(expireTimeInSec * 1000);
        // expired
        Assert.assertTrue(cacheManager.getTimestreamMetaLastUpdateTime(meta1) == null);
        Assert.assertTrue(cacheManager.getTimestreamMetaLastUpdateTime(meta2) == null);
        cacheManager.updateTimestreamMeta(meta1, now + stemp);
        cacheManager.updateTimestreamMeta(meta3, now + stemp);
        Assert.assertTrue(cacheManager.getTimestreamMetaLastUpdateTime(meta1) == now + stemp);
        Assert.assertTrue(cacheManager.getTimestreamMetaLastUpdateTime(meta2) == null);
        Assert.assertTrue(cacheManager.getTimestreamMetaLastUpdateTime(meta3) == now + stemp);
    }

    @Test
    public void testAddMeta() {
        // mock writer: return true when tryAddRowChange
        long expireTimeInSec = 2;
        TimestreamIdentifier meta1 = new TimestreamIdentifier.Builder("cpu").addTag("tag1", "0").build();
        TimestreamIdentifier meta2 = new TimestreamIdentifier.Builder("cpu").addTag("tag1", "1").build();
        long cacheSize = 10 * (meta1.getDataSize() + 8);
        MockWriter writer = new MockWriter();
        MetaCacheManager cacheManager = new MetaCacheManager("table", expireTimeInSec, cacheSize, writer);
        long stemp = expireTimeInSec * 1000 * 1000;
        long now = System.currentTimeMillis() * 1000;

        cacheManager.addTimestreamMeta(meta1, now);
        Assert.assertTrue(cacheManager.getTimestreamMetaLastUpdateTime(meta1) == now);
        Assert.assertEquals(writer.getRowCount(), 1);

        cacheManager.addTimestreamMeta(meta1, now + stemp - 1);
        Assert.assertTrue(cacheManager.getTimestreamMetaLastUpdateTime(meta1) == now);
        Assert.assertEquals(writer.getRowCount(), 1);

        cacheManager.addTimestreamMeta(meta2, now);
        Assert.assertTrue(cacheManager.getTimestreamMetaLastUpdateTime(meta2) == now);
        Assert.assertEquals(writer.getRowCount(), 2);

        cacheManager.addTimestreamMeta(meta1, now + stemp);
        Assert.assertTrue(cacheManager.getTimestreamMetaLastUpdateTime(meta1) == now + stemp);
        Assert.assertEquals(writer.getRowCount(), 3);
    }

    @Test
    public void testUpdateMeta() {
        // mock writer: return true when tryAddRowChange
        long expireTimeInSec = 2;
        TimestreamIdentifier meta1 = new TimestreamIdentifier.Builder("cpu").addTag("tag1", "0").build();
        TimestreamIdentifier meta2 = new TimestreamIdentifier.Builder("cpu").addTag("tag1", "1").build();
        long cacheSize = 10 * (meta1.getDataSize() + 8);
        MockWriter writer = new MockWriter();
        MetaCacheManager cacheManager = new MetaCacheManager("table", expireTimeInSec, cacheSize, writer);
        long stemp = expireTimeInSec * 1000 * 1000;
        long now = System.currentTimeMillis() * 1000;

        cacheManager.updateTimestreamMeta(meta1, now);
        Assert.assertTrue(cacheManager.getTimestreamMetaLastUpdateTime(meta1) == now);
        Assert.assertEquals(writer.getRowCount(), 0);

        cacheManager.updateTimestreamMeta(meta1, now + stemp - 1);
        Assert.assertTrue(cacheManager.getTimestreamMetaLastUpdateTime(meta1) == now + stemp - 1);
        Assert.assertEquals(writer.getRowCount(), 0);

        cacheManager.updateTimestreamMeta(meta2, now);
        Assert.assertTrue(cacheManager.getTimestreamMetaLastUpdateTime(meta2) == now);
        Assert.assertEquals(writer.getRowCount(), 0);

        cacheManager.updateTimestreamMeta(meta1, now + stemp);
        Assert.assertTrue(cacheManager.getTimestreamMetaLastUpdateTime(meta1) == now + stemp);
        Assert.assertEquals(writer.getRowCount(), 0);
    }

    private class MockWriter implements TableStoreWriter {
        private int rowCount = 0;
        private boolean returnTrue = true;

        public int getRowCount() {
            return rowCount;
        }

        public void addRowChange(RowChange var1) {
        }

        @Override
        public Future<WriterResult> addRowChangeWithFuture(RowChange rowChange) throws ClientException {
            return null;
        }

        public void setReturnTrue(boolean res) {
            this.returnTrue = res;
        }

        public boolean tryAddRowChange(RowChange var1) throws ClientException {
            if (returnTrue) {
                rowCount++;
                return true;
            } else {
                return false;
            }
        }

        public void addRowChange(List<RowChange> var1, List<RowChange> var2) throws ClientException{
        }

        @Override
        public Future<WriterResult> addRowChangeWithFuture(List<RowChange> rowChanges) throws ClientException {
            return null;
        }

        /** @deprecated */
        public void setCallback(TableStoreCallback<RowChange, ConsumedCapacity> var1){
        }

        public void setResultCallback(TableStoreCallback<RowChange, RowWriteResult> var1){
        }

        /** @deprecated */
        public TableStoreCallback<RowChange, ConsumedCapacity> getCallback(){
            return new TableStoreCallback<RowChange, ConsumedCapacity>() {
                @Override
                public void onCompleted(RowChange rowChange, ConsumedCapacity consumedCapacity) {

                }

                @Override
                public void onFailed(RowChange rowChange, Exception e) {

                }
            };
        }

        public TableStoreCallback<RowChange, RowWriteResult> getResultCallback(){
            return new TableStoreCallback<RowChange, RowWriteResult>() {
                @Override
                public void onCompleted(RowChange rowChange, RowWriteResult rowWriteResult) {

                }

                @Override
                public void onFailed(RowChange rowChange, Exception e) {

                }
            };
        }

        public WriterConfig getWriterConfig(){
            return new WriterConfig();
        }

        public WriterStatistics getWriterStatistics(){
            return new WriterStatistics() {
                @Override
                public long getTotalRequestCount() {
                    return 0;
                }

                @Override
                public long getTotalRowsCount() {
                    return 0;
                }

                @Override
                public long getTotalSucceedRowsCount() {
                    return 0;
                }

                @Override
                public long getTotalFailedRowsCount() {
                    return 0;
                }

                @Override
                public long getTotalSingleRowRequestCount() {
                    return 0;
                }
            };
        }

        public void flush() throws ClientException{
        }

        public void close(){
        }
    }
}
