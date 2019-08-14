package com.alicloud.openservices.tablestore.timestream.functiontest;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.filter.Filter;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;
import com.alicloud.openservices.tablestore.timestream.*;
import com.alicloud.openservices.tablestore.timestream.internal.TableMetaGenerator;
import com.alicloud.openservices.tablestore.timestream.internal.Utils;
import com.alicloud.openservices.tablestore.timestream.model.*;
import com.alicloud.openservices.tablestore.timestream.bench.Conf;
import com.alicloud.openservices.tablestore.timestream.bench.wrapper.TableStoreWrapper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class SingleTimestreamFunctiontest {
    private static Logger logger = LoggerFactory.getLogger(SingleTimestreamFunctiontest.class);
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
    public void testBasic() throws ExecutionException, InterruptedException {
        String table = "singlets_10";
        logger.debug("Table:" + table);

        AsyncClient client = new AsyncClient(conf.getEndpoint(), conf.getAccessId(), conf.getAccessKey(), conf.getInstance());

        String metaTableName = "singlets";
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        TimestreamDB db = new TimestreamDBClient(asyncClient, config);
        Helper.safeClearDB(asyncClient);
        db.createMetaTable();
        db.createDataTable(table);
        Thread.sleep(5000);
        TimestreamDataTable dataTable = db.dataTable(table);

        TableStoreWrapper wrapper = TableStoreWrapper.instance(client, table);
        wrapper.createTableAfter();

        TreeMap<String, String> tags = new TreeMap<String, String>();
        tags.put("Cluster", "AY45W");
        tags.put("Role", "OTSServer#");
        String tagString = Utils.serializeTags(tags);
        for (long i = 0; i < 10000; i += 10) {
            wrapper.putRow()
                    .addPrimaryKey(TableMetaGenerator.CN_PK0, Utils.getHashCode("cpu", tagString))
                    .addPrimaryKey(TableMetaGenerator.CN_PK1, "cpu")
                    .addPrimaryKey(TableMetaGenerator.CN_PK2, tagString)
                    .addPrimaryKey(TableMetaGenerator.CN_TAMESTAMP_NAME, TimeUnit.SECONDS.toMicros(i))
                    .addColumn("load1", i)
                    .addColumn("load5", i)
                    .addColumn("load15", i)
                    .commit();
        }
        TimestreamIdentifier meta = new TimestreamIdentifier.Builder("cpu").setTags(tags).build();
        Iterator<Point> pointIterator =  dataTable.get(meta).timeRange(TimeRange.range(0, 10000, TimeUnit.SECONDS)).fetchAll();
        List<Point> points = new ArrayList<Point>(1000);
        while(pointIterator.hasNext()) {
            points.add(pointIterator.next());
        }

        Assert.assertEquals(1000, points.size());
        for (int i = 0; i < 1000; i++) {
            Point point = points.get(i);
            logger.debug(point.getTimestamp() + " : " + point.toString());
            Assert.assertEquals(3, point.getFields().size());
            Assert.assertEquals(TimeUnit.SECONDS.toMicros(i * 10), point.getTimestamp());
        }
        long timestamp = points.get(0).getTimestamp(TimeUnit.MICROSECONDS);
        Iterator<Point> pointIterator1 =  dataTable.get(meta).timestamp(timestamp, TimeUnit.MICROSECONDS).fetchAll();
        Assert.assertTrue(pointIterator1.hasNext());
        Point point = pointIterator1.next();
        Assert.assertTrue(!pointIterator1.hasNext());
        Assert.assertEquals(point.getTimestamp(), timestamp);
        Assert.assertEquals(point.getField("load1").asLong(), timestamp);
        Assert.assertEquals(point.getField("load5").asLong(), timestamp);
        Assert.assertEquals(point.getField("load15").asLong(), timestamp);
    }

    @Test
    public void testWithAddTag() throws Exception {
        String metaTableName = "singlets";
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
        TimestreamMetaTable metaWriter = db.metaTable();

        TimestreamMeta meta = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("Cluster", "AY45W")
                        .addTag("Role", "OTSServer#")
                        .build()
        );

        metaWriter.put(meta);

        for (long i = 0; i < 500; i++) {
            dataTable.write(meta.getIdentifier(), new Point.Builder(i * 10, TimeUnit.SECONDS)
                    .addField("temperature", i)
                    .addField("power", i)
                    .addField("broken", 0 == i % 2).build());
        }

        Helper.waitSync();

        Iterator<Point> pointIterator =  dataTable
                .get(meta.getIdentifier())
                .fetchAll();

        List<Point> points = new ArrayList<Point>(500);
        long j = 0;
        while(pointIterator.hasNext()) {
            points.add(pointIterator.next());
        }

        Assert.assertEquals(500, points.size());

        for (int i = 0; i < 500; i++) {
            Point point = points.get(i);
            logger.debug(point.getTimestamp() + " : " + point.toString());
            Assert.assertEquals(3, point.getFields().size());
            Assert.assertEquals(TimeUnit.SECONDS.toMicros(i * 10), point.getTimestamp());
            Assert.assertEquals(i, point.getField("temperature").asLong());
            Assert.assertEquals(i, point.getField("power").asLong());
            Assert.assertEquals(0 == i % 2, point.getField("broken").asBoolean());
        }
    }

    @Test
    public void testWithSelect() throws Exception {
        String metaTableName = "singlets";
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
        TimestreamMetaTable metaWriter = db.metaTable();

        TimestreamMeta meta = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("Cluster", "AY45W")
                        .addTag("Role", "OTSServer#")
                        .build()
        );

        metaWriter.put(meta);

        for (long i = 0; i < 20; i++) {
            dataTable.write(meta.getIdentifier(), new Point.Builder(i * 10, TimeUnit.SECONDS)
                    .addField("temperature", i)
                    .addField("power", i)
                    .addField("broken", 0 == i % 2).build());
        }

        Helper.waitSync();

        Iterator<Point> pointIterator =  dataTable
                .get(meta.getIdentifier())
                .select("temperature")
                .fetchAll();

        List<Point> points = new ArrayList<Point>(20);
        while(pointIterator.hasNext()) {
            points.add(pointIterator.next());
        }

        Assert.assertEquals(20, points.size());

        for (int i = 0; i < 20; i++) {
            Point point = points.get(i);
            logger.debug(point.getTimestamp() + " : " + point.toString());
            Assert.assertEquals(1, point.getFields().size());
            Assert.assertEquals(TimeUnit.SECONDS.toMicros(i * 10), point.getTimestamp());
            Assert.assertEquals(i, point.getField("temperature").asLong());
        }
    }

    @Test
    public void testWithSelectNotExist() throws Exception {
        String metaTableName = "singlets";
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
        TimestreamMetaTable metaWriter = db.metaTable();

        TimestreamMeta meta = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("Cluster", "AY45W")
                        .addTag("Role", "OTSServer#")
                        .build()
        );

        metaWriter.put(meta);

        for (long i = 0; i < 20; i++) {
            dataTable.write(meta.getIdentifier(), new Point.Builder(i * 10, TimeUnit.SECONDS)
                    .addField("temperature", i)
                    .addField("power", i)
                    .addField("broken", 0 == i % 2).build());
        }

        Helper.waitSync();

        Iterator<Point> pointIterator =  dataTable
                .get(meta.getIdentifier())
                .select("height")
                .fetchAll();

        List<Point> points = new ArrayList<Point>(0);
        while(pointIterator.hasNext()) {
            points.add(pointIterator.next());
        }

        Assert.assertEquals(0, points.size());
    }

    @Test
    public void testWithSelectExistAndNotExist() throws Exception {
        String metaTableName = "singlets";
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
        TimestreamMetaTable metaWriter = db.metaTable();

        TimestreamMeta meta = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("Cluster", "AY45W")
                        .addTag("Role", "OTSServer#")
                        .build()
        );

        metaWriter.put(meta);

        for (long i = 0; i < 20; i++) {
            dataTable.write(meta.getIdentifier(), new Point.Builder(i * 10, TimeUnit.SECONDS)
                    .addField("temperature", i)
                    .addField("power", i)
                    .addField("broken", 0 == i % 2).build());
        }

        Helper.waitSync();

        Iterator<Point> pointIterator =  dataTable
                .get(meta.getIdentifier())
                .select("temperature", "height")
                .fetchAll();

        List<Point> points = new ArrayList<Point>(20);
        while(pointIterator.hasNext()) {
            points.add(pointIterator.next());
        }

        Assert.assertEquals(20, points.size());

        for (int i = 0; i < 20; i++) {
            Point point = points.get(i);
            logger.debug(point.getTimestamp() + " : " + point.toString());
            Assert.assertEquals(1, point.getFields().size());
            Assert.assertEquals(TimeUnit.SECONDS.toMicros(i * 10), point.getTimestamp());
            Assert.assertEquals(i, point.getField("temperature").asLong());
        }
    }

    @Test
    public void testWithTimeRange() throws Exception {
        String metaTableName = "singlets";
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
        TimestreamMetaTable metaWriter = db.metaTable();

        TimestreamMeta meta = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("Cluster", "AY45W")
                        .addTag("Role", "OTSServer#")
                        .build()
        );

        metaWriter.put(meta);

        for (long i = 0; i < 20; i++) {
            dataTable.write(meta.getIdentifier(), new Point.Builder(i, TimeUnit.SECONDS)
                    .addField("temperature", i)
                    .addField("power", i)
                    .addField("broken", 0 == i % 2).build());
        }

        for (long i = 20; i < 40; i++) {
            dataTable.write(meta.getIdentifier(), new Point.Builder(i, TimeUnit.SECONDS)
                    .addField("temperature", i)
                    .addField("power", i)
                    .addField("broken", 0 == i % 2).build());
        }

        for (long i = 40; i < 60; i++) {
            dataTable.write(meta.getIdentifier(), new Point.Builder(i, TimeUnit.SECONDS)
                    .addField("temperature", i)
                    .addField("power", i)
                    .addField("broken", 0 == i % 2).build());
        }

        Helper.waitSync();

        Iterator<Point> pointIterator =  dataTable
                .get(meta.getIdentifier())
                .timeRange(TimeRange.range(20, 40, TimeUnit.SECONDS))
                .fetchAll();

        List<Point> points = new ArrayList<Point>(20);
        while(pointIterator.hasNext()) {
            points.add(pointIterator.next());
        }

        Assert.assertEquals(20, points.size());

        for (int i = 20; i < 40; i++) {
            Point point = points.get(i - 20);
            logger.debug(point.getTimestamp() + " : " + point.toString());
            Assert.assertEquals(TimeUnit.SECONDS.toMicros(i), point.getTimestamp());
            Assert.assertEquals(3, point.getFields().size());
            Assert.assertEquals(i, point.getField("temperature").asLong());
            Assert.assertEquals(i, point.getField("power").asLong());
            Assert.assertEquals(0 == i % 2, point.getField("broken").asBoolean());
        }
    }

    @Test
    public void testWithBeginTimeRange() throws Exception{
        String metaTableName = "singlets";
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
        TimestreamMetaTable metaWriter = db.metaTable();

        TimestreamMeta meta = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("Cluster", "AY45W")
                        .addTag("Role", "OTSServer#")
                        .build()
        );

        metaWriter.put(meta);

        for (long i = 0; i < 20; i++) {
            dataTable.write(meta.getIdentifier(), new Point.Builder(i, TimeUnit.SECONDS)
                    .addField("temperature", i)
                    .addField("power", i)
                    .addField("broken", 0 == i % 2).build());
        }

        for (long i = 20; i < 40; i++) {
            dataTable.write(meta.getIdentifier(), new Point.Builder(i, TimeUnit.SECONDS)
                    .addField("temperature", i)
                    .addField("power", i)
                    .addField("broken", 0 == i % 2).build());
        }

        for (long i = 40; i < 60; i++) {
            dataTable.write(meta.getIdentifier(), new Point.Builder(i, TimeUnit.SECONDS)
                    .addField("temperature", i)
                    .addField("power", i)
                    .addField("broken", 0 == i % 2).build());
        }

        Helper.waitSync();

        Iterator<Point> pointIterator =  dataTable
                .get(meta.getIdentifier())
                .timeRange(TimeRange.after(20, TimeUnit.SECONDS))
                .fetchAll();

        List<Point> points = new ArrayList<Point>(40);
        while(pointIterator.hasNext()) {
            points.add(pointIterator.next());
        }

        Assert.assertEquals(40, points.size());

        for (int i = 20; i < 60; i++) {
            Point point = points.get(i - 20);
            logger.debug(point.getTimestamp() + " : " + point.toString());
            Assert.assertEquals(TimeUnit.SECONDS.toMicros(i), point.getTimestamp());
            Assert.assertEquals(3, point.getFields().size());
            Assert.assertEquals(i, point.getField("temperature").asLong());
            Assert.assertEquals(i, point.getField("power").asLong());
            Assert.assertEquals(0 == i % 2, point.getField("broken").asBoolean());
        }
    }

    @Test
    public void testWithEndTimeRange() throws Exception {
        String metaTableName = "singlets";
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
        TimestreamMetaTable metaWriter = db.metaTable();

        TimestreamMeta meta = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("Cluster", "AY45W")
                        .addTag("Role", "OTSServer#")
                        .build()
        );

        metaWriter.put(meta);

        for (long i = 20; i < 40; i++) {
            dataTable.write(meta.getIdentifier(), new Point.Builder(i, TimeUnit.SECONDS)
                    .addField("temperature", i)
                    .addField("power", i)
                    .addField("broken", 0 == i % 2).build());
        }

        for (long i = 40; i < 60; i++) {
            dataTable.write(meta.getIdentifier(), new Point.Builder(i, TimeUnit.SECONDS)
                    .addField("temperature", i)
                    .addField("power", i)
                    .addField("broken", 0 == i % 2).build());
        }

        for (long i = 60; i < 80; i++) {
            dataTable.write(meta.getIdentifier(), new Point.Builder(i, TimeUnit.SECONDS)
                    .addField("temperature", i)
                    .addField("power", i)
                    .addField("broken", 0 == i % 2).build());
        }

        Helper.waitSync();

        Iterator<Point> pointIterator =  dataTable
                .get(meta.getIdentifier())
                .timeRange(TimeRange.range(0, 40, TimeUnit.SECONDS))
                .fetchAll();

        List<Point> points = new ArrayList<Point>(20);
        while(pointIterator.hasNext()) {
            points.add(pointIterator.next());
        }

        Assert.assertEquals(20, points.size());

        for (int i = 20; i < 40; i++) {
            Point point = points.get(i - 20);
            logger.debug(point.getTimestamp() + " : " + point.toString());
            Assert.assertEquals(TimeUnit.SECONDS.toMicros(i), point.getTimestamp());
            Assert.assertEquals(3, point.getFields().size());
            Assert.assertEquals(i, point.getField("temperature").asLong());
            Assert.assertEquals(i, point.getField("power").asLong());
            Assert.assertEquals(0 == i % 2, point.getField("broken").asBoolean());
        }
    }

    @Test
    public void testWithTimeRangeNoData() throws Exception {
        String metaTableName = "singlets";
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
        TimestreamMetaTable metaWriter = db.metaTable();

        TimestreamMeta meta = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("Cluster", "AY45W")
                        .addTag("Role", "OTSServer#")
                        .build()
        );

        metaWriter.put(meta);

        for (long i = 20; i < 40; i++) {
            dataTable.write(meta.getIdentifier(), new Point.Builder(i, TimeUnit.SECONDS)
                    .addField("temperature", i)
                    .addField("power", i)
                    .addField("broken", 0 == i % 2).build());
        }

        for (long i = 60; i < 80; i++) {
            dataTable.write(meta.getIdentifier(), new Point.Builder(i, TimeUnit.SECONDS)
                    .addField("temperature", i)
                    .addField("power", i)
                    .addField("broken", 0 == i % 2).build());
        }

        Helper.waitSync();

        Iterator<Point> pointIterator =  dataTable
                .get(meta.getIdentifier())
                .timeRange(TimeRange.range(40, 60, TimeUnit.SECONDS))
                .fetchAll();

        List<Point> points = new ArrayList<Point>(0);
        while(pointIterator.hasNext()) {
            points.add(pointIterator.next());
        }

        Assert.assertEquals(0, points.size());
    }

    @Test
    public void testErrorTimeRange() throws Exception {
        String metaTableName = "singlets";
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
        TimestreamMetaTable metaWriter = db.metaTable();

        TimestreamMeta meta = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("Cluster", "AY45W")
                        .addTag("Role", "OTSServer#")
                        .build()
        );

        {
            try {
                dataTable.get(meta.getIdentifier())
                        .timeRange(TimeRange.range(0, 0, TimeUnit.SECONDS))
                        .fetchAll();
                Assert.fail();
            } catch (ClientException e) {
                // pass
            }
        }
        {
            {
                try {
                    dataTable.get(meta.getIdentifier())
                            .timeRange(TimeRange.range(0, -1, TimeUnit.SECONDS))
                            .fetchAll();
                    Assert.fail();
                } catch (ClientException e) {
                    // pass
                }
            }
        }
    }

    @Test
    public void testErrorNoDataTable() throws Exception {
        String metaTableName = "singlets";
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


        try {
            TimestreamDataTable dataTable = db.dataTable(tableName);
            Assert.fail();
        } catch (ClientException e) {
            Assert.assertTrue(e.getMessage().contains("Requested table does not exist.")); // OTS 自身有点问题，表不存在的时候会报 "Internal server error." 不符合预期
        }
    }

    @Test
    public void test10000Data() throws Exception {
        String metaTableName = "singlets";
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
        TimestreamMetaTable metaWriter = db.metaTable();

        TimestreamMeta meta = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("Cluster", "AY45W")
                        .addTag("Role", "OTSServer#")
                        .build()
        );


        metaWriter.put(meta);

        for (long i = 0; i < 10000; i++) {
            dataTable.write(meta.getIdentifier(), new Point.Builder(i * 10, TimeUnit.SECONDS)
                    .addField("temperature", i)
                    .addField("power", i)
                    .addField("broken", 0 == i % 2).build());
        }

        Helper.waitSync();

        Iterator<Point> pointIterator =  dataTable.get(meta.getIdentifier())
                .select("temperature")
                .fetchAll();

        List<Point> points = new ArrayList<Point>(10000);
        while(pointIterator.hasNext()) {
            points.add(pointIterator.next());
        }

        Assert.assertEquals(10000, points.size());

        for (int i = 0; i < 10000; i++) {
            Point point = points.get(i);
            logger.debug(point.getTimestamp() + " : " + point.toString());
            Assert.assertEquals(1, point.getFields().size());
            Assert.assertEquals(TimeUnit.SECONDS.toMicros(i * 10), point.getTimestamp());
            Assert.assertEquals(i, point.getField("temperature").asLong());
        }
    }

    @Test
    public void testFilter() throws Exception {
        String table = "singlets_filter";
        logger.debug("Table:" + table);

        AsyncClient client = new AsyncClient(conf.getEndpoint(), conf.getAccessId(), conf.getAccessKey(), conf.getInstance());

        String metaTableName = "singlets";
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        TimestreamDB db = new TimestreamDBClient(asyncClient, config);
        Helper.safeClearDB(asyncClient);
        db.createMetaTable();
        db.createDataTable(table);
        Thread.sleep(5000);
        TimestreamDataTable dataTable = db.dataTable(table);

        TableStoreWrapper wrapper = TableStoreWrapper.instance(client, table);
        wrapper.createTableAfter();

        TreeMap<String, String> tags = new TreeMap<String, String>();
        tags.put("Cluster", "AY45W");
        tags.put("Role", "OTSServer#");
        String tagString = Utils.serializeTags(tags);
        for (long i = 0; i < 10000; i += 10) {
            wrapper.putRow()
                    .addPrimaryKey(TableMetaGenerator.CN_PK0, Utils.getHashCode("cpu", tagString))
                    .addPrimaryKey(TableMetaGenerator.CN_PK1, "cpu")
                    .addPrimaryKey(TableMetaGenerator.CN_PK2, tagString)
                    .addPrimaryKey(TableMetaGenerator.CN_TAMESTAMP_NAME, TimeUnit.SECONDS.toMicros(i))
                    .addColumn("load1", i)
                    .addColumn("load5", i)
                    .addColumn("load15", i)
                    .commit();
        }
        TimestreamIdentifier meta = new TimestreamIdentifier.Builder("cpu").setTags(tags).build();
        Filter filter = new SingleColumnValueFilter("load1", SingleColumnValueFilter.CompareOperator.LESS_THAN, ColumnValue.fromLong(100));
        Iterator<Point> pointIterator =  dataTable.get(meta)
                .timeRange(TimeRange.range(0, 10000, TimeUnit.SECONDS))
                .filter(filter)
                .fetchAll();
        List<Point> points = new ArrayList<Point>(1000);
        while(pointIterator.hasNext()) {
            points.add(pointIterator.next());
        }

        Assert.assertEquals(10, points.size());
        for (int i = 0; i < 10; i++) {
            Point point = points.get(i);
            logger.debug(point.getTimestamp() + " : " + point.toString());
            Assert.assertEquals(3, point.getFields().size());
            Assert.assertEquals(TimeUnit.SECONDS.toMicros(i * 10), point.getTimestamp());
            Assert.assertEquals(point.getField("load1").asLong(), i * 10);
        }
    }

    @Test
    public void testOrderBy() throws Exception {
        String table = "singlets_orderby";
        logger.debug("Table:" + table);

        AsyncClient client = new AsyncClient(conf.getEndpoint(), conf.getAccessId(), conf.getAccessKey(), conf.getInstance());

        String metaTableName = "singlets";
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        TimestreamDB db = new TimestreamDBClient(asyncClient, config);
        Helper.safeClearDB(asyncClient);
        db.createMetaTable();
        db.createDataTable(table);
        Thread.sleep(5000);
        TimestreamDataTable dataTable = db.dataTable(table);

        TableStoreWrapper wrapper = TableStoreWrapper.instance(client, table);
        wrapper.createTableAfter();

        TreeMap<String, String> tags = new TreeMap<String, String>();
        tags.put("Cluster", "AY45W");
        tags.put("Role", "OTSServer#");
        String tagString = Utils.serializeTags(tags);
        for (long i = 0; i < 10000; i += 10) {
            wrapper.putRow()
                    .addPrimaryKey(TableMetaGenerator.CN_PK0, Utils.getHashCode("cpu", tagString))
                    .addPrimaryKey(TableMetaGenerator.CN_PK1, "cpu")
                    .addPrimaryKey(TableMetaGenerator.CN_PK2, tagString)
                    .addPrimaryKey(TableMetaGenerator.CN_TAMESTAMP_NAME, TimeUnit.SECONDS.toMicros(i))
                    .addColumn("load1", i)
                    .addColumn("load5", i)
                    .addColumn("load15", i)
                    .commit();
        }
        TimestreamIdentifier meta = new TimestreamIdentifier.Builder("cpu").setTags(tags).build();
        Filter filter = new SingleColumnValueFilter("load1", SingleColumnValueFilter.CompareOperator.LESS_THAN, ColumnValue.fromLong(100));
        Iterator<Point> pointIterator =  dataTable.get(meta)
                .timeRange(TimeRange.range(0, 10000, TimeUnit.SECONDS))
                .filter(filter)
                .descTimestamp()
                .fetchAll();
        List<Point> points = new ArrayList<Point>(1000);
        while(pointIterator.hasNext()) {
            points.add(pointIterator.next());
        }

        Assert.assertEquals(9, points.size());
        for (int i = 0; i < 9; i++) {
            int j = 10 - i - 1;
            Point point = points.get(i);
            logger.debug(point.getTimestamp() + " : " + point.toString());
            Assert.assertEquals(3, point.getFields().size());
            Assert.assertEquals(TimeUnit.SECONDS.toMicros(j * 10), point.getTimestamp());
            Assert.assertEquals(point.getField("load1").asLong(), j * 10);
        }
    }
}
