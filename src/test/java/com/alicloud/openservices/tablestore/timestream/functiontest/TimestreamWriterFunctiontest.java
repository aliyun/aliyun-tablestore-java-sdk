package com.alicloud.openservices.tablestore.timestream.functiontest;

import com.alicloud.openservices.tablestore.timestream.bench.Conf;
import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.ConsumedCapacity;
import com.alicloud.openservices.tablestore.model.RowChange;
import com.alicloud.openservices.tablestore.timestream.model.filter.Filter;
import com.alicloud.openservices.tablestore.writer.WriterConfig;
import com.alicloud.openservices.tablestore.timestream.*;
import com.alicloud.openservices.tablestore.timestream.model.*;
import com.alicloud.openservices.tablestore.timestream.model.filter.LastUpdateTime;
import com.alicloud.openservices.tablestore.timestream.model.filter.Name;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.alicloud.openservices.tablestore.timestream.model.filter.FilterFactory.*;

public class TimestreamWriterFunctiontest {
    private static Logger logger = LoggerFactory.getLogger(TimestreamMetaFunctiontest.class);
    private static Conf conf;

    @BeforeClass
    public static void init() throws FileNotFoundException {
        logger.debug("load configuration");
        conf = Conf.newInstance("src/test/resources/test_conf.json");
    }

    @AfterClass
    public static void close() {

    }

    @Test
    public void testBasicSave() throws ExecutionException, InterruptedException {
        String metaTableName = "tswrite_ft";
        WriterConfig writerConfig = new WriterConfig();
        writerConfig.setFlushInterval(1);
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        TimestreamDB db = new TimestreamDBClient(
                asyncClient, config, writerConfig, null);
        Helper.safeClearDB(asyncClient);
        db.createMetaTable();
        String tableName = metaTableName + "_datatable_10";
        db.createDataTable(tableName);
        Thread.sleep(5000);
        TimestreamDataTable dataTable = db.dataTable(tableName);
        TimestreamMetaTable metaTable = db.metaTable();

        TimestreamIdentifier identifier = new TimestreamIdentifier.Builder("cpu")
                .addTag("machine", "123.et2")
                .addTag("cluster", "45c")
                .build();

        long now = System.currentTimeMillis() / 1000 * 1000;

        // async write
        Point point1 = new Point.Builder(now, TimeUnit.MILLISECONDS).addField("sys", 30).addField("usr", 50).build();
        dataTable.asyncWrite(identifier, point1);
        Thread.sleep(TimeUnit.SECONDS.toMillis(2));
        Filter filter = Name.equal("cpu");
        {
            Iterator<Point> pointIterator = dataTable.get(identifier).timeRange(TimeRange.range(now, now + 1000, TimeUnit.MILLISECONDS)).fetchAll();
            List<Point> points = new ArrayList<Point>();
            while (pointIterator.hasNext()) {
                Point point = pointIterator.next();
                points.add(point);
            }
            Assert.assertEquals(1, points.size());
            Assert.assertEquals(points.get(0).getTimestamp(), point1.getTimestamp());
            Assert.assertEquals(points.get(0).getField("sys"), point1.getField("sys"));
            Assert.assertEquals(points.get(0).getField("usr"), point1.getField("usr"));
        }

        // sync write
        Point point2 = new Point.Builder(now, TimeUnit.MILLISECONDS).addField("sys", 31).addField("usr", 51).build();
        dataTable.write(identifier, point2);
        {
            Iterator<Point> pointIterator =  dataTable.get(identifier).timeRange(TimeRange.range(now, now + 1000, TimeUnit.MILLISECONDS)).fetchAll();
            List<Point> points = new ArrayList<Point>();
            while (pointIterator.hasNext()) {
                Point point = pointIterator.next();
                points.add(point);
            }
            Assert.assertEquals(1, points.size());
            Assert.assertEquals(points.get(0).getTimestamp(), point2.getTimestamp());
            Assert.assertEquals(points.get(0).getField("sys"), point2.getField("sys"));
            Assert.assertEquals(points.get(0).getField("usr"), point2.getField("usr"));
        }

        Helper.waitSync();
        {
            Iterator<TimestreamMeta> metaIterator = metaTable.filter(filter).fetchAll();
            List<TimestreamIdentifier> metaList = new ArrayList<TimestreamIdentifier>();
            while (metaIterator.hasNext()) {
                metaList.add(metaIterator.next().getIdentifier());
            }
            Assert.assertEquals(metaList.size(), 1);
            Assert.assertTrue(Helper.isContaineIdentifier(metaList, identifier));
        }
    }

    private class MockCallback implements TableStoreCallback<RowChange, ConsumedCapacity> {
        private List<RowChange> failedRows = new ArrayList<RowChange>();
        private List<RowChange> succeedRows = new ArrayList<RowChange>();

        public void onCompleted(RowChange var1, ConsumedCapacity var2) {
            succeedRows.add(var1);
        }

        public void onFailed(RowChange var1, Exception var2) {
            failedRows.add(var1);
        }

        public List<RowChange> getFailedRows() {
            return this.failedRows;
        }

        public List<RowChange> getSucceedRows() {
            return this.succeedRows;
        }
    }

    @Test
    public void testWriterCallback() throws ExecutionException, InterruptedException {
        String metaTableName = "tswrite_ft";
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        WriterConfig writerConfig = new WriterConfig();
        writerConfig.setMaxColumnsCount(2048);
        writerConfig.setFlushInterval(1);
        MockCallback callback = new MockCallback();
        TimestreamDB db = new TimestreamDBClient(asyncClient, config, writerConfig, callback);
        Helper.safeClearDB(asyncClient);
        db.createMetaTable();
        String tableName = metaTableName + "_datatable_10";
        db.createDataTable(tableName);
        Thread.sleep(5000);
        TimestreamDataTable dataTable = db.dataTable(tableName);
        TimestreamMetaTable metaTable = db.metaTable();


        TimestreamIdentifier identifier = new TimestreamIdentifier.Builder("cpu").build();
        int colCount = 1025;
        Point.Builder builder1 = new Point.Builder(123, TimeUnit.SECONDS);
        for (int i = 0; i < colCount; ++i) {
            builder1.addField("key"+i, "a");
        }
        Point point1 = builder1.build();
        Point.Builder builder2 = new Point.Builder(123, TimeUnit.SECONDS);
        for (int i = 0; i < 2; ++i) {
            builder2.addField("key"+i, "a");
        }
        Point point2 = builder2.build();
        // 验证数据异步写入失败时会调用callback
        // 同步写失败
        try {
            dataTable.write(identifier, point1);
            Assert.fail();
        } catch (TableStoreException e) {
            Assert.assertEquals(callback.getFailedRows().size(), 0);
            Assert.assertEquals(callback.getSucceedRows().size(), 0);
        }
        // 异步写失败
        dataTable.asyncWrite(identifier, point1);
        Thread.sleep(2000);
        Assert.assertEquals(callback.getFailedRows().size(), 1);
        Assert.assertEquals(callback.getSucceedRows().size(), 0);
        // 异步写成功
        dataTable.asyncWrite(identifier, point2);
        Thread.sleep(2000);
        Assert.assertEquals(callback.getFailedRows().size(), 1);
        Assert.assertEquals(callback.getSucceedRows().size(), 1);

        // 验证meta写入不会调用callback
        metaTable.delete(identifier);
        TimestreamMeta meta = new TimestreamMeta(identifier).addAttribute("k1", "v1");
        metaTable.put(meta);
        Assert.assertEquals(callback.getFailedRows().size(), 1);
        Assert.assertEquals(callback.getSucceedRows().size(), 1);
    }

    @Test
    public void testFailedWriteData() throws ExecutionException, InterruptedException {
        String metaTableName = "tswrite_ft";
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        TimestreamDB db = new TimestreamDBClient(asyncClient, config);
        Helper.safeClearDB(asyncClient);
        db.createMetaTable();
        String tableName = metaTableName + "_datatable_10";
        db.createDataTable(tableName);
        Thread.sleep(5000);
        TimestreamDataTable dataTable = db.dataTable(tableName);

        TimestreamIdentifier identifier = new TimestreamIdentifier.Builder("cpu").build();
        int colCount = 1025;
        Point.Builder builder = new Point.Builder(123, TimeUnit.SECONDS);
        for (int i = 0; i < colCount; ++i) {
            builder.addField("key"+i, "a");
        }
        try {
            dataTable.write(identifier, builder.build());
            Assert.fail();
        } catch (TableStoreException e) {
            //pass
        }
    }

    @Test
    public void testBasicSaveMeta() throws ExecutionException, InterruptedException {
        String metaTableName = "tswrite_ft";
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        TimestreamDB db = new TimestreamDBClient(asyncClient, config);
        Helper.safeClearDB(asyncClient);
        db.createMetaTable();
        Thread.sleep(10000);
        TimestreamMetaTable metaTable = db.metaTable();

        List<TimestreamMeta> metaList = new ArrayList<TimestreamMeta>();

        TimestreamMeta meta1 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("machine", "123.et2")
                        .addTag("cluster", "45c")
                        .build());
        metaList.add(meta1);
        metaTable.put(meta1);

        TimestreamMeta meta2 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("machine", "123.et2")
                        .addTag("cluster", "45c")
                        .addTag("tag1", "")
                        .build());
        metaList.add(meta2);
        metaTable.put(meta2);
        Thread.sleep(20000);
        Filter filter = Name.equal("cpu");
        {
            Iterator<TimestreamMeta> iterator = metaTable.filter(filter).fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(2, metas.size());
            for (TimestreamMeta meta : metas) {
                Assert.assertTrue(Helper.isContaineMeta(metaList, meta));
            }
        }

        metaTable.delete(meta1.getIdentifier());
        metaTable.delete(meta2.getIdentifier());
        Thread.sleep(10000);
        {
            Iterator<TimestreamMeta> iterator = metaTable.filter(filter).fetchAll();
            List<TimestreamIdentifier> metas = new ArrayList<TimestreamIdentifier>();
            while (iterator.hasNext()) {
                metas.add(iterator.next().getIdentifier());
            }
            Assert.assertEquals(0, metas.size());
        }
    }

    /**
     * 验证数据写入过程中后台会自动更新时间线
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testMetaUpdate() throws ExecutionException, InterruptedException {
        String metaTableName = "tswrite_ft";
        WriterConfig writerConfig = new WriterConfig();
        writerConfig.setFlushInterval(1);
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        config.setIntervalDumpMeta(60, TimeUnit.SECONDS);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        TimestreamDB db = new TimestreamDBClient(asyncClient, config, writerConfig, null);
        Helper.safeClearDB(asyncClient);
        db.createMetaTable();
        String tableName = metaTableName + "_datatable_10";
        db.createDataTable(tableName);
        Thread.sleep(1000);
        TimestreamDataTable dataTable = db.dataTable(tableName);
        TimestreamMetaTable metaTable = db.metaTable();

        /**
         * 1. 写一个数据点，校验数据和meta都有更新
         * 2. 写一个不同时间的点，校验数据有更新，meta不会更新
         * 3. 写一个meta更新后的点，校验数据和meta都有更新
         * 4. 删除meta，校验meta不存在了
         */

        TimestreamMeta meta = new TimestreamMeta(new TimestreamIdentifier.Builder("cpu")
                .addTag("machine", "123.et2")
                .addTag("cluster", "45c")
                .build())
                .addAttribute("TableStore.role#", "")
                .addAttribute("OTS.role#", "");
        meta.getIdentifier();

        long now = System.currentTimeMillis() / 1000 * 1000;
        Point point1 = new Point.Builder(now, TimeUnit.MILLISECONDS).addField("sys", 30).build();
        dataTable.asyncWrite(meta.getIdentifier(), point1);
        // wait for meta flush
        Thread.sleep(TimeUnit.SECONDS.toMillis(30));
        Filter filter1 = and(
                Name.equal("cpu"),
                LastUpdateTime.in(TimeRange.after(now, TimeUnit.MILLISECONDS)));
        long metaTime = 0;
        {
            Iterator<TimestreamMeta> iterator = metaTable.filter(filter1).fetchAll();
            List<TimestreamIdentifier> metas = new ArrayList<TimestreamIdentifier>();
            while (iterator.hasNext()) {
                TimestreamMeta tmpMeta = iterator.next();
                metas.add(tmpMeta.getIdentifier());
                metaTime = tmpMeta.getUpdateTimeInUsec();
            }
            Assert.assertEquals(1, metas.size());
            Assert.assertTrue(Helper.isContaineIdentifier(metas, meta.getIdentifier()));

            Filter filter2 = and(
                    Name.equal("cpu"));
            Iterator<Point> pointIterator =  dataTable.get(meta.getIdentifier()).timeRange(TimeRange.range(now, now + 2000, TimeUnit.MILLISECONDS)).fetchAll();
            List<Point> points = new ArrayList<Point>();
            while (pointIterator.hasNext()) {
                Point point = pointIterator.next();
                points.add(point);
            }
            Assert.assertEquals(1, points.size());
            Assert.assertEquals(points.get(0).getTimestamp(), point1.getTimestamp());
        }

        Point point2 = new Point.Builder(now + 1000, TimeUnit.MILLISECONDS).addField("sys", 40).build();
        dataTable.asyncWrite(meta.getIdentifier(), point2);
        // wait for meta flush
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));
        {
            Iterator<TimestreamMeta> iterator = metaTable.filter(filter1).fetchAll();
            List<TimestreamIdentifier> metas = new ArrayList<TimestreamIdentifier>();
            long newMetaTime = 0;
            while (iterator.hasNext()) {
                TimestreamMeta tmpMeta = iterator.next();
                metas.add(tmpMeta.getIdentifier());
                newMetaTime = tmpMeta.getUpdateTimeInUsec();
            }
            Assert.assertEquals(metaTime, newMetaTime);
            Assert.assertEquals(1, metas.size());
            Assert.assertTrue(Helper.isContaineIdentifier(metas, meta.getIdentifier()));

            Iterator<Point> pointIterator =  dataTable.get(meta.getIdentifier()).timeRange(TimeRange.range(now, now + 2000, TimeUnit.MILLISECONDS)).fetchAll();
            List<Point> points = new ArrayList<Point>();
            while (pointIterator.hasNext()) {
                Point point = pointIterator.next();
                points.add(point);
            }
            Assert.assertEquals(2, points.size());
            Assert.assertEquals(points.get(0).getTimestamp(), point1.getTimestamp());
            Assert.assertEquals(points.get(1).getTimestamp(), point2.getTimestamp());
        }
        meta.addAttribute("attr1", "123");

        Thread.sleep(TimeUnit.SECONDS.toMillis(30));
        dataTable.asyncWrite(meta.getIdentifier(), new Point.Builder(now + 1000000, TimeUnit.MILLISECONDS).addField("sys", 40).build());
        // wait for meta flush
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));
        {
            Filter filter = and(
                    Name.equal("cpu"),
                    LastUpdateTime.in(TimeRange.after(now, TimeUnit.MILLISECONDS)));
            Iterator<TimestreamMeta> iterator = metaTable.filter(filter).fetchAll();
            List<TimestreamIdentifier> metas = new ArrayList<TimestreamIdentifier>();
            long newMetaTime = 0;
            while (iterator.hasNext()) {
                TimestreamMeta tmpMeta = iterator.next();
                metas.add(tmpMeta.getIdentifier());
                newMetaTime = tmpMeta.getUpdateTimeInUsec();
            }
            Assert.assertTrue(newMetaTime > metaTime);
            Assert.assertEquals(1, metas.size());
            Assert.assertTrue(Helper.isContaineIdentifier(metas, meta.getIdentifier()));

            Iterator<Point> pointIterator =  dataTable.get(meta.getIdentifier()).timeRange(TimeRange.range(now, now + 1000000, TimeUnit.MILLISECONDS)).fetchAll();
            List<Point> points = new ArrayList<Point>();
            while (pointIterator.hasNext()) {
                Point point = pointIterator.next();
                points.add(point);
            }
            Assert.assertEquals(2, points.size());
            Assert.assertEquals(points.get(0).getTimestamp(), point1.getTimestamp());
            Assert.assertEquals(points.get(1).getTimestamp(), point2.getTimestamp());
        }

        metaTable.delete(meta.getIdentifier());
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));
        {
            Iterator<TimestreamMeta> iterator = metaTable.filter(filter1).fetchAll();
            List<TimestreamIdentifier> metas = new ArrayList<TimestreamIdentifier>();
            while (iterator.hasNext()) {
                metas.add(iterator.next().getIdentifier());
            }
            Assert.assertEquals(0, metas.size());
        }
        db.deleteMetaTable();
    }

    /**
     * 关闭数据写入过程中后台会自动更新时间线
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testMetaNotUpdateWithMetaTable() throws ExecutionException, InterruptedException {
        String metaTableName = "tswrite_ft";
        WriterConfig writerConfig = new WriterConfig();
        writerConfig.setFlushInterval(1);
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        config.setDumpMeta(false);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        TimestreamDB db = new TimestreamDBClient(asyncClient, config, writerConfig, null);
        Helper.safeClearDB(asyncClient);
        db.createMetaTable();
        String tableName = metaTableName + "_datatable_10";
        db.createDataTable(tableName);
        Thread.sleep(1000);
        TimestreamDataTable dataTable = db.dataTable(tableName);
        TimestreamMetaTable metaTable = db.metaTable();

        /**
         * 1. 写一个数据点，校验数据和meta都有更新
         * 2. 写一个不同时间的点，校验数据有更新，meta不会更新
         * 3. 写一个meta更新后的点，校验数据和meta都有更新
         * 4. 删除meta，校验meta不存在了
         */

        TimestreamMeta meta = new TimestreamMeta(new TimestreamIdentifier.Builder("cpu")
                .addTag("machine", "123.et2")
                .addTag("cluster", "45c")
                .build())
                .addAttribute("TableStore.role#", "")
                .addAttribute("OTS.role#", "");
        meta.getIdentifier();

        long now = System.currentTimeMillis() / 1000 * 1000;
        Point point1 = new Point.Builder(now, TimeUnit.MILLISECONDS).addField("sys", 30).build();
        dataTable.asyncWrite(meta.getIdentifier(), point1);
        // wait for meta flush
        Thread.sleep(TimeUnit.SECONDS.toMillis(30));
        Filter filter1 = and(
                Name.equal("cpu"),
                LastUpdateTime.in(TimeRange.after(now, TimeUnit.MILLISECONDS)));
        {
            Iterator<TimestreamMeta> iterator = metaTable.filter(filter1).fetchAll();
            List<TimestreamIdentifier> metas = new ArrayList<TimestreamIdentifier>();
            while (iterator.hasNext()) {
                metas.add(iterator.next().getIdentifier());
            }
            Assert.assertEquals(0, metas.size());

            Filter filter2 = and(
                    Name.equal("cpu"));
            Iterator<Point> pointIterator = dataTable.get(meta.getIdentifier()).timeRange(TimeRange.range(now, now + 2000, TimeUnit.MILLISECONDS)).fetchAll();
            List<Point> points = new ArrayList<Point>();
            while (pointIterator.hasNext()) {
                Point point = pointIterator.next();
                points.add(point);
            }
            Assert.assertEquals(1, points.size());
            Assert.assertEquals(points.get(0).getTimestamp(), point1.getTimestamp());
        }
    }

    /**
     * 关闭数据写入过程中后台会自动更新时间线，且没有meta表
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testMetaNotUpdateWithoutMetaTable() throws ExecutionException, InterruptedException {
        String metaTableName = "tswrite_ft";
        WriterConfig writerConfig = new WriterConfig();
        writerConfig.setFlushInterval(1);
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        config.setDumpMeta(false);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        TimestreamDB db = new TimestreamDBClient(asyncClient, config, writerConfig, null);
        Helper.safeClearDB(asyncClient);
        String tableName = metaTableName + "_datatable_10";
        db.createDataTable(tableName);
        Thread.sleep(1000);
        TimestreamDataTable dataTable = db.dataTable(tableName);
        TimestreamMetaTable metaTable = db.metaTable();

        /**
         * 1. 写一个数据点，校验数据和meta都有更新
         * 2. 写一个不同时间的点，校验数据有更新，meta不会更新
         * 3. 写一个meta更新后的点，校验数据和meta都有更新
         * 4. 删除meta，校验meta不存在了
         */

        TimestreamMeta meta = new TimestreamMeta(new TimestreamIdentifier.Builder("cpu")
                .addTag("machine", "123.et2")
                .addTag("cluster", "45c")
                .build())
                .addAttribute("TableStore.role#", "")
                .addAttribute("OTS.role#", "");
        meta.getIdentifier();

        long now = System.currentTimeMillis() / 1000 * 1000;
        Point point1 = new Point.Builder(now, TimeUnit.MILLISECONDS).addField("sys", 30).build();
        dataTable.asyncWrite(meta.getIdentifier(), point1);
        // wait for meta flush
        Thread.sleep(TimeUnit.SECONDS.toMillis(30));
        Filter filter1 = and(
                Name.equal("cpu"),
                LastUpdateTime.in(TimeRange.after(now, TimeUnit.MILLISECONDS)));
        {
            Filter filter2 = and(
                    Name.equal("cpu"));
            Iterator<Point> pointIterator = dataTable.get(meta.getIdentifier()).timeRange(TimeRange.range(now, now + 2000, TimeUnit.MILLISECONDS)).fetchAll();
            List<Point> points = new ArrayList<Point>();
            while (pointIterator.hasNext()) {
                Point point = pointIterator.next();
                points.add(point);
            }
            Assert.assertEquals(1, points.size());
            Assert.assertEquals(points.get(0).getTimestamp(), point1.getTimestamp());
        }
    }

    /**
     * 验证tag/attribute name中不能包含=, "字符
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testInvalidMeta() throws ExecutionException, InterruptedException {
        String metaTableName = "tswrite_ft";
        WriterConfig writerConfig = new WriterConfig();
        writerConfig.setFlushInterval(1);
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        TimestreamDB db = new TimestreamDBClient(asyncClient, config);
        Helper.safeClearDB(asyncClient);
        db.createMetaTable();
        String tableName = metaTableName + "_datatable_10";
        db.createDataTable(tableName);
        Thread.sleep(5000);

        // invalid tag name with "="
        try {
            new TimestreamIdentifier.Builder("cpu").addTag("tag=", "");
        } catch (ClientException e) {
            // pass
        }

        // invalid tag name with "\""
        try {
            new TimestreamIdentifier.Builder("cpu").addTag("tag\"", "");
        } catch (ClientException e) {
            // pass
        }

        // invalid attribute name with "="
        try {
            new TimestreamMeta(new TimestreamIdentifier.Builder("cpu").build()).addAttribute("attr=", "");
        } catch (ClientException e) {
            // pass
        }

        // invalid attribute name with "\""
        try {
            new TimestreamMeta(new TimestreamIdentifier.Builder("cpu").build()).addAttribute("attr\"", "");
        } catch (ClientException e) {
            // pass
        }

        new TimestreamMeta(new TimestreamIdentifier.Builder("cpu").build()).addAttribute("attr1", "=").addAttribute("attr2", "\"");
        new TimestreamIdentifier.Builder("cpu").addTag("tag1", "=").addTag("tag2", "\"");
    }
}
