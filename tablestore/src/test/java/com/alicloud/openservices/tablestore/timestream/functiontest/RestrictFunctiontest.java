package com.alicloud.openservices.tablestore.timestream.functiontest;

import com.alicloud.openservices.tablestore.timestream.bench.Conf;
import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.timestream.TimestreamDB;
import com.alicloud.openservices.tablestore.timestream.TimestreamDBClient;
import com.alicloud.openservices.tablestore.timestream.TimestreamDBConfiguration;
import com.alicloud.openservices.tablestore.timestream.TimestreamMetaTable;
import com.alicloud.openservices.tablestore.timestream.internal.Utils;
import com.alicloud.openservices.tablestore.timestream.model.AttributeIndexSchema;
import com.alicloud.openservices.tablestore.timestream.model.TimestreamIdentifier;
import com.alicloud.openservices.tablestore.timestream.model.TimestreamMeta;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class RestrictFunctiontest {
    private static Logger logger = LoggerFactory.getLogger(RestrictFunctiontest.class);
    private static Conf conf;

    @BeforeClass
    public static void init() throws FileNotFoundException {
        logger.debug("load configuration");
        conf = Conf.newInstance("src/test/resources/test_conf.json");
    }

    @AfterClass
    public static void close() {

    }

    @Before
    public void caseBefore() {

    }

    @After
    public void caseAfter() {

    }

    @Test
    public void testMaxDataTable() throws InterruptedException {
        long[] dataInterval = {
                TimeUnit.SECONDS.toMicros(1),
                TimeUnit.SECONDS.toMicros(2),
                TimeUnit.SECONDS.toMicros(4),
                TimeUnit.SECONDS.toMicros(8),
                TimeUnit.SECONDS.toMicros(16),
                TimeUnit.SECONDS.toMicros(32),
        };

        String metaTable = "restrict";
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTable);
        config.setMaxDataTableNumForWrite(5);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        Helper.safeClearDB(asyncClient);
        TimestreamDB db = new TimestreamDBClient(asyncClient, config);
        db.createMetaTable();

        for (Long l : dataInterval) {
            db.createDataTable("datatable_" + l);
        }
        for (int i = 0; i < dataInterval.length - 1 ; ++i) {
            db.dataTable("datatable_" + dataInterval[i]);
        }
        try {
            db.dataTable("datatable_" + dataInterval[5]);
            Assert.fail();
        } catch (ClientException e) {
            // pass
        }
        db.metaTable();
    }

    @Test
    public void testTag() throws InterruptedException {
        String tableName = "restrict";
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(tableName);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        TimestreamDB db = new TimestreamDBClient(asyncClient, config);
        Helper.safeClearDB(asyncClient);
        db.createMetaTable();
        TimestreamMetaTable metaTable = db.metaTable();
        Helper.waitSync();
        {
            TimestreamIdentifier identifier = new TimestreamIdentifier.Builder("cpu")
                    .addTag("tag0", "tv0")
                    .addTag("tag1", "tv1")
                    .addTag("tag2", "tv2")
                    .addTag("tag3", "tv3")
                    .addTag("tag4", "tv4")
                    .addTag("tag5", "tv5")
                    .addTag("tag6", "tv6")
                    .addTag("tag7", "tv7")
                    .addTag("tag8", "tv8")
                    .addTag("tag9", "tv9")
                    .addTag("tag10", "tv10")
                    .addTag("tag11", "tv11").build();
            TimestreamMeta meta = new TimestreamMeta(identifier);
            metaTable.put(meta);
        }
        {
            try {
                TimestreamIdentifier identifier = new TimestreamIdentifier.Builder("cpu")
                        .addTag("tag0", "tv0")
                        .addTag("tag1", "tv1")
                        .addTag("tag2", "tv2")
                        .addTag("tag3", "tv3")
                        .addTag("tag4", "tv4")
                        .addTag("tag5", "tv5")
                        .addTag("tag6", "tv6")
                        .addTag("tag7", "tv7")
                        .addTag("tag8", "tv8")
                        .addTag("tag9", "tv9")
                        .addTag("tag10", "tv10")
                        .addTag("tag11", "tv11")
                        .addTag("tag12", "tv12").build();
                TimestreamMeta meta = new TimestreamMeta(identifier);
                metaTable.put(meta);
                Assert.fail();
            } catch (ClientException e) {
                Assert.assertEquals("The input tag count(13) more than 12 .", e.getMessage());
            }
        }
    }
}
