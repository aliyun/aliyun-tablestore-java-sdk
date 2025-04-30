package com.alicloud.openservices.tablestore.timestream.functiontest;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.timestream.*;
import com.alicloud.openservices.tablestore.timestream.internal.TableMetaGenerator;
import com.alicloud.openservices.tablestore.timestream.internal.Utils;
import com.alicloud.openservices.tablestore.timestream.model.*;
import com.alicloud.openservices.tablestore.timestream.bench.Conf;
import com.alicloud.openservices.tablestore.timestream.bench.wrapper.TableStoreWrapper;
import com.alicloud.openservices.tablestore.timestream.model.filter.Attribute;
import com.alicloud.openservices.tablestore.timestream.model.filter.Filter;
import com.alicloud.openservices.tablestore.timestream.model.filter.Name;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.alicloud.openservices.tablestore.timestream.model.filter.FilterFactory.*;

public class MultiTimestreamFunctiontest {
    private static Logger logger = LoggerFactory.getLogger(MultiTimestreamFunctiontest.class);
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
        String table = "multits_10";
        logger.debug("Table:" + table);

        AsyncClient client = new AsyncClient(conf.getEndpoint(), conf.getAccessId(), conf.getAccessKey(), conf.getInstance());

        long[] dataInterval = {TimeUnit.SECONDS.toMicros(10)};
        long metaInterval = TimeUnit.SECONDS.toMicros(10);
        String metaTableName = "multits";
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

        TableStoreWrapper wrapper = TableStoreWrapper.instance(client, table);
        wrapper.createTableAfter();

        TimestreamDataTable dataWriter = db.dataTable(table);
        TimestreamMetaTable metaWriter = db.metaTable();

        TimestreamIdentifier identifier = new TimestreamIdentifier.Builder("cpu")
                .addTag("Cluster", "AY45W")
                .addTag("Role", "OTSServer#")
                .addTag("Machine", "eu13.rt19032").build();
        TimestreamMeta meta1 = new TimestreamMeta(identifier);

        TimestreamIdentifier identifier2 = new TimestreamIdentifier.Builder("cpu")
                .addTag("Cluster", "AY45W")
                .addTag("Role", "OTSServer#")
                .addTag("Machine", "eu13.rt238987").build();
        TimestreamMeta meta2 = new TimestreamMeta(identifier2);

        metaWriter.put(meta1);
        metaWriter.put(meta2);
        {

            String tagString = Utils.serializeTags(meta1.getIdentifier().getTags());
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

            Iterator<Point> pointIterator =  dataWriter
                    .get(meta1.getIdentifier())
                    .timeRange(TimeRange.range(0, 10000, TimeUnit.SECONDS))
                    .fetchAll();
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
        }

        {
            String tagString = Utils.serializeTags(meta2.getIdentifier().getTags());
            for (long i = 0; i < 10000; i += 10) {
                wrapper.putRow()
                        .addPrimaryKey(TableMetaGenerator.CN_PK0, Utils.getHashCode("cpu", tagString))
                        .addPrimaryKey(TableMetaGenerator.CN_PK1, "cpu")
                        .addPrimaryKey(TableMetaGenerator.CN_PK2, tagString)
                        .addPrimaryKey(TableMetaGenerator.CN_TAMESTAMP_NAME, TimeUnit.SECONDS.toMicros(i))
                        .addColumn("load1", i + 1)
                        .addColumn("load15", i + 1)
                        .commit();
            }
            Iterator<Point> pointIterator =  dataWriter
                    .get(meta2.getIdentifier())
                    .timeRange(TimeRange.range(0, 10000, TimeUnit.SECONDS))
                    .fetchAll();
            List<Point> points = new ArrayList<Point>(1000);
            while(pointIterator.hasNext()) {
                points.add(pointIterator.next());
            }

            Assert.assertEquals(1000, points.size());
            for (int i = 0; i < 1000; i++) {
                Point point = points.get(i);
                logger.debug(point.getTimestamp() + " : " + point.toString());
                Assert.assertEquals(2, point.getFields().size());
                Assert.assertEquals(TimeUnit.SECONDS.toMicros(i * 10), point.getTimestamp());
            }
        }

        Helper.waitSync();

        // Get multiple timelines
        {
            Filter filter = Name.equal("cpu");
            Iterator<TimestreamMeta> metaIterator = metaWriter.filter(filter)
                    .fetchAll();
            List<TimestreamMeta> metaList = new ArrayList<TimestreamMeta>();
            while (metaIterator.hasNext()) {
                metaList.add(metaIterator.next());
            }
            Assert.assertEquals(2, metaList.size());

            Assert.assertTrue(Helper.isContaineMeta(metaList, meta1));
            Assert.assertTrue(Helper.isContaineMeta(metaList, meta2));
        }
    }

    @Test
    public void testMix() throws InterruptedException {
        String metaTableName = "mix";
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        TimestreamDB db = new TimestreamDBClient(asyncClient, config);

        Helper.safeClearDB(asyncClient);
        List<AttributeIndexSchema> indexSchemas = new ArrayList<AttributeIndexSchema>();
        indexSchemas.add(new AttributeIndexSchema("location", AttributeIndexSchema.Type.GEO_POINT));
        db.createMetaTable(indexSchemas);
        TimestreamMetaTable metaTable = db.metaTable();

        TimestreamIdentifier identifier1 = new TimestreamIdentifier.Builder("status")
                .addTag("name", "alibaba1")
                .addTag("floor", "1-6").build();// Fly Heaven Park
        TimestreamMeta meta1 = new TimestreamMeta(identifier1)
                .addAttribute("location", "30.130370, 120.083263");

        TimestreamIdentifier identifier2 = new TimestreamIdentifier.Builder("status")
                .addTag("name", "alibaba2")
                .addTag("floor", "2-1").build();// Qianjiang Block
        TimestreamMeta meta2 = new TimestreamMeta(identifier2)
                .addAttribute("location", "30.129237, 120.088380");

        TimestreamIdentifier identifier3 = new TimestreamIdentifier.Builder("status")
                .addTag("name", "alibaba3")
                .addTag("floor", "8-1").build();
        TimestreamMeta meta3 = new TimestreamMeta(identifier3)
                .addAttribute("location", "30.130188, 120.082112");// Medium or Large

        TimestreamIdentifier identifier4 = new TimestreamIdentifier.Builder("status")
                .addTag("name", "alibaba4")
                .addTag("floor", "1-1").build();
        TimestreamMeta meta4 = new TimestreamMeta(identifier4)
                .addAttribute("location", "31.207134, 121.343689");// Shanghai Hongqiao

        TimestreamIdentifier identifier5 = new TimestreamIdentifier.Builder("status")
                .addTag("name", "aliyun1")
                .addTag("floor", "1-5").build();
        TimestreamMeta meta5 = new TimestreamMeta(identifier5)
                .addAttribute("location", "30.280175, 120.024933"); // Future Tech City

        {
            metaTable.put(meta1);
            metaTable.put(meta2);
            metaTable.put(meta3);
            metaTable.put(meta4);
            metaTable.put(meta5);
        }

        Helper.waitSync();

        {
            List<String> points = Arrays.asList(
                    "30.130645, 120.079297",
                    "30.132399, 120.083171",
                    "30.128854, 120.083825",
                    "30.127931, 120.081095"
            );
            Filter filter = and(
                    Name.in(new String[]{"status"}),
                    Attribute.inGeoPolygon("location", points)
            );

            Iterator<TimestreamMeta> iterator = metaTable
                    .filter(filter)
                    .fetchAll();
            List<TimestreamIdentifier> metas = new ArrayList<TimestreamIdentifier>();
            while(iterator.hasNext()) {
                metas.add(iterator.next().getIdentifier());
            }
            Assert.assertEquals(2, metas.size());
            Assert.assertEquals(true, Helper.isContaineIdentifier(metas, identifier1));
            Assert.assertEquals(false, Helper.isContaineIdentifier(metas, identifier2));
            Assert.assertEquals(true, Helper.isContaineIdentifier(metas, identifier3));
            Assert.assertEquals(false, Helper.isContaineIdentifier(metas, identifier4));
            Assert.assertEquals(false, Helper.isContaineIdentifier(metas, identifier5));
        }
    }
}
