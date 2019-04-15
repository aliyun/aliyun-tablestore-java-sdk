package com.alicloud.openservices.tablestore.timestream.functiontest;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.timestream.*;
import com.alicloud.openservices.tablestore.timestream.bench.Conf;
import com.alicloud.openservices.tablestore.timestream.model.*;
import com.alicloud.openservices.tablestore.timestream.model.filter.*;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.alicloud.openservices.tablestore.timestream.model.filter.FilterFactory.*;

public class TimestreamMetaFunctiontest {
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

    @Before
    public void caseBefore() {

    }

    @After
    public void caseAfter() {

    }

    @Test
    public void testBasic() throws InterruptedException {
        String metaTableName = "tsmeta";
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        TimestreamDB db = new TimestreamDBClient(asyncClient, config);
        Helper.safeClearDB(asyncClient);
        db.createMetaTable();
        Thread.sleep(5000);

        TimestreamMetaTable metaTable = db.metaTable();

        long s1 = System.currentTimeMillis();
        TimestreamMeta meta1 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("Cluster", "AY45W")
                        .addTag("Role", "OTSServer#\"")
                        .addTag("Machine", "eu13.rt19032")
                        .build());
        metaTable.put(meta1);

        Thread.sleep(TimeUnit.SECONDS.toMillis(2));

        long s2 = System.currentTimeMillis();

        Thread.sleep(TimeUnit.SECONDS.toMillis(2));

        TimestreamMeta meta2 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("Cluster", "AY45W")
                        .addTag("Role", "=OTSServer#\"")
                        .addTag("Machine", "eu13.rt238987")
                        .build());
        metaTable.put(meta2);

        Helper.waitSync();

        long s3 = System.currentTimeMillis();

        {
            Filter filter = Name.equal("cpu");
            TimestreamMetaIterator iterator = metaTable.filter(filter).fetchAll();
            Assert.assertEquals(2, iterator.getTotalCount());
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta1));
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta2));
        }
        {
            Filter filter = and(
                    Name.equal("cpu"),
                    LastUpdateTime.in(TimeRange.range(s1, s2, TimeUnit.MILLISECONDS)));
            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(1, metas.size());
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta1));
        }
        {
            Filter filter = and(
                    Name.equal("cpu"),
                    LastUpdateTime.in(TimeRange.range(s2, s3, TimeUnit.MILLISECONDS)));
            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(1, metas.size());
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta2));
        }
        {
            Filter filter = and(
                    Name.equal("cpu"),
                    LastUpdateTime.in(TimeRange.range(s1, s3, TimeUnit.MILLISECONDS)));
            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(2, metas.size());
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta1));
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta2));
        }
    }

    @Test
    public void testSelectAttributes() throws ExecutionException, InterruptedException {
        String metaTableName = "tsmeta";
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        TimestreamDB db = new TimestreamDBClient(asyncClient, config);
        Helper.safeClearDB(asyncClient);
        db.createMetaTable();
        Thread.sleep(5000);

        TimestreamMetaTable metaTable = db.metaTable();

        long s1 = System.currentTimeMillis();
        TimestreamMeta meta1 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("Cluster", "AY45W")
                        .addTag("Machine", "eu13.rt19032")
                        .build())
                .addAttribute("Role", "OTSServer#")
                .addAttribute("description", "test");
        metaTable.put(meta1);
        Helper.waitSync();
        // get with attr
        {
            TimestreamMeta meta = metaTable.get(meta1.getIdentifier()).returnAll().fetch();
            Assert.assertTrue(Helper.compareMeta(meta, meta1));
        }
        // get with identifier only
        {
            TimestreamMeta meta = metaTable.get(meta1.getIdentifier()).fetch();
            Assert.assertTrue(meta.getIdentifier().equals(meta1.getIdentifier()));
            Assert.assertEquals(meta.getUpdateTimeInUsec(), meta1.getUpdateTimeInUsec());
            Assert.assertEquals(meta.getAttributes().size(), 0);
        }
        // get with some attributes
        {
            TimestreamMeta meta = metaTable.get(meta1.getIdentifier()).selectAttributes("Role").fetch();
            Assert.assertTrue(meta.getIdentifier().equals(meta1.getIdentifier()));
            Assert.assertEquals(meta.getUpdateTimeInUsec(), meta1.getUpdateTimeInUsec());
            Assert.assertEquals(meta.getAttributes().size(), 1);
        }
        Filter filter = Name.equal("cpu");
        // filter with attr
        {
            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .returnAll()
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while (iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(metas.size(), 1);
            Assert.assertTrue(Helper.isContaineMeta(metas, meta1));
        }
        // filter with identifier only
        {
            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .fetchAll();
            List<TimestreamIdentifier> metas = new ArrayList<TimestreamIdentifier>();
            while (iterator.hasNext()) {
                TimestreamMeta meta = iterator.next();
                Assert.assertEquals(meta.getAttributes().size(), 0);
                Assert.assertEquals(meta.getUpdateTimeInUsec(), meta1.getUpdateTimeInUsec());
                metas.add(meta.getIdentifier());
            }
            Assert.assertEquals(metas.size(), 1);
            Assert.assertTrue(Helper.isContaineIdentifier(metas, meta1.getIdentifier()));
        }
        // filter with some attributes
        {
            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .selectAttributes("Role")
                    .fetchAll();
            List<TimestreamIdentifier> metas = new ArrayList<TimestreamIdentifier>();
            while (iterator.hasNext()) {
                TimestreamMeta meta = iterator.next();
                Assert.assertEquals(meta.getUpdateTimeInUsec(), meta1.getUpdateTimeInUsec());
                Assert.assertEquals(meta.getAttributes().size(), 1);
                metas.add(meta.getIdentifier());
            }
            Assert.assertEquals(metas.size(), 1);
            Assert.assertTrue(Helper.isContaineIdentifier(metas, meta1.getIdentifier()));
        }
    }

    @Test
    public void testWithTag() throws ExecutionException, InterruptedException {
        String metaTableName = "tsmeta";
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        TimestreamDB db = new TimestreamDBClient(asyncClient, config);
        Helper.safeClearDB(asyncClient);

        db.createMetaTable();
        Thread.sleep(5000);

        TimestreamMetaTable metaTable = db.metaTable();

        long s1 = System.currentTimeMillis();
        TimestreamMeta meta1 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("Cluster", "AY45W")
                        .addTag("Role", "OTSServer#")
                        .addTag("Machine", "eu13.rt19032")
                        .build());
        metaTable.put(meta1);

        Thread.sleep(TimeUnit.SECONDS.toMillis(2));

        long s2 = System.currentTimeMillis();

        Thread.sleep(TimeUnit.SECONDS.toMillis(2));

        TimestreamMeta meta2 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("Cluster", "AY45W")
                        .addTag("Role", "OTSServer#")
                        .addTag("Machine", "eu13.rt238987")
                        .build());
        metaTable.put(meta2);

        Helper.waitSync();

        {
            // equal
            Filter filter = and(
                    Name.equal("cpu"),
                    Tag.equal("Machine", "eu13.rt238987")
            );

            Iterator<TimestreamMeta> iterator = metaTable
                    .filter(filter)
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(1, metas.size());
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta2));
        }
        {
            // not equal
            Filter filter = and(
                    Name.equal("cpu"),
                    Tag.notEqual("Machine", "eu13.rt238987")
            );

            Iterator<TimestreamMeta> iterator = metaTable
                    .filter(filter)
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(1, metas.size());
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta1));
        }
        {
            // in
            Filter filter = and(
                    Name.equal("cpu"),
                    Tag.in("Machine", new String[]{"eu13.rt238987", "eu13.rt19032"})
            );

            Iterator<TimestreamMeta> iterator = metaTable
                    .filter(filter)
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(2, metas.size());
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta1));
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta2));
        }
        {
            // not in
            Filter filter = and(
                    Name.equal("cpu"),
                    Tag.notIn("Machine", new String[]{"eu13.rt238987"})
            );

            Iterator<TimestreamMeta> iterator = metaTable
                    .filter(filter)
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(1, metas.size());
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta1));
        }
        {
            // prefix
            Filter filter = and(
                    Name.equal("cpu"),
                    Tag.prefix("Machine", "eu13")
            );

            Iterator<TimestreamMeta> iterator = metaTable
                    .filter(filter)
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(2, metas.size());
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta1));
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta2));
        }
    }

    @Test
    public void testWithTagAndCompare() throws ExecutionException, InterruptedException {
        String metaTableName = "tsmeta";
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        TimestreamDB db = new TimestreamDBClient(asyncClient, config);
        Helper.safeClearDB(asyncClient);

        db.createMetaTable();
        Thread.sleep(5000);

        TimestreamMetaTable metaTable = db.metaTable();

        long s1 = System.currentTimeMillis();
        TimestreamMeta meta1 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("Cluster", "AY45W")
                        .addTag("Role", "OTSServer#")
                        .addTag("Machine", "eu13.rt19032")
                        .build());
        metaTable.put(meta1);

        Thread.sleep(TimeUnit.SECONDS.toMillis(2));

        long s2 = System.currentTimeMillis();

        Thread.sleep(TimeUnit.SECONDS.toMillis(2));

        TimestreamMeta meta2 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("Cluster", "AY45W")
                        .addTag("Role", "OTSServer#")
                        .addTag("Machine", "eu13.rt238987")
                        .build());
        metaTable.put(meta2);

        Helper.waitSync();

        {
            Filter filter = and(
                    Name.equal("cpu"),
                    Tag.equal("Cluster", "AY45W"),
                    Tag.equal("Machine", "eu13.rt238987")
            );

            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(1, metas.size());
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta2));
        }
    }

    @Test
    public void testWithTagOrCompare() throws ExecutionException, InterruptedException {
        long[] dataInterval = {TimeUnit.SECONDS.toMicros(10)};
        long metaInterval = TimeUnit.SECONDS.toMicros(1);

        String metaTableName = "tsmeta";
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        TimestreamDB db = new TimestreamDBClient(asyncClient, config);
        Helper.safeClearDB(asyncClient);

        db.createMetaTable();
        Thread.sleep(5000);

        TimestreamMetaTable metaTable = db.metaTable();

        long s1 = System.currentTimeMillis();
        TimestreamMeta meta1 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("Cluster", "AY45W")
                        .addTag("Role", "OTSServer#")
                        .addTag("Machine", "eu13.rt19032")
                        .build());
        metaTable.put(meta1);

        Thread.sleep(TimeUnit.SECONDS.toMillis(2));

        long s2 = System.currentTimeMillis();

        Thread.sleep(TimeUnit.SECONDS.toMillis(2));

        TimestreamMeta meta2 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("Cluster", "AY45W")
                        .addTag("Role", "OTSServer#")
                        .addTag("Machine", "eu13.rt238987")
                        .build());
        metaTable.put(meta2);

        Helper.waitSync();

        {
            Filter filter = and(
                    Name.equal("cpu"),
                    or(
                            Tag.equal("Machine", "eu13.rt19032"),
                            Tag.equal("Machine", "eu13.rt238987")
                    )
            );

            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(2, metas.size());
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta1));
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta2));
        }
    }

    @Test
    public void testWithTagMixCompare() throws ExecutionException, InterruptedException {
        String metaTableName = "tsmeta";
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        TimestreamDB db = new TimestreamDBClient(asyncClient, config);
        Helper.safeClearDB(asyncClient);

        db.createMetaTable();
        Thread.sleep(5000);

        TimestreamMetaTable metaTable = db.metaTable();

        long s1 = System.currentTimeMillis();
        TimestreamMeta meta1 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("Cluster", "AY45W")
                        .addTag("Role", "OTSServer#")
                        .addTag("Machine", "eu13.rt19032")
                        .build());
        metaTable.put(meta1);

        Thread.sleep(TimeUnit.SECONDS.toMillis(2));

        long s2 = System.currentTimeMillis();

        Thread.sleep(TimeUnit.SECONDS.toMillis(2));

        TimestreamMeta meta2 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("Cluster", "AY45W")
                        .addTag("Role", "OTSServer#")
                        .addTag("Machine", "eu13.rt238987")
                        .build());
        metaTable.put(meta2);

        TimestreamMeta meta3 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("Cluster", "AY45W-HA")
                        .addTag("Role", "OTSServer#")
                        .addTag("Machine", "eu13.rt238987")
                        .build());
        metaTable.put(meta3);

        Helper.waitSync();

        {
            Filter filter = and (
                    Name.equal("cpu"),
                    Tag.equal("Cluster", "AY45W-HA"),
                    or(
                            Tag.equal("Machine", "eu13.rt19032"),
                            Tag.equal("Machine", "eu13.rt238987")
                    )
            );

            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(1, metas.size());
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta3));
        }
    }

    @Test
    public void testWithAttribute() throws InterruptedException {
        String metaTableName = "tsmeta";
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        TimestreamDB db = new TimestreamDBClient(asyncClient, config);
        Helper.safeClearDB(asyncClient);
        List<AttributeIndexSchema> indexSchemas = new ArrayList<AttributeIndexSchema>();
        indexSchemas.add(new AttributeIndexSchema("owner", AttributeIndexSchema.Type.KEYWORD));
        indexSchemas.add(new AttributeIndexSchema("number", AttributeIndexSchema.Type.LONG));
        indexSchemas.add(new AttributeIndexSchema("score", AttributeIndexSchema.Type.DOUBLE));
        indexSchemas.add(new AttributeIndexSchema("succ", AttributeIndexSchema.Type.BOOLEAN));
        db.createMetaTable(indexSchemas);
        Thread.sleep(5000);
        TimestreamMetaTable metaTable = db.metaTable();

        long s1 = System.currentTimeMillis();
        TimestreamMeta meta1 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("Cluster", "AY45W")
                        .addTag("Role", "OTSServer#")
                        .addTag("Machine", "eu13.rt19032")
                        .build())
                .addAttribute("owner", "pre-redchen")
                .addAttribute("number", 10)
                .addAttribute("score", 10.0)
                .addAttribute("succ", true);
        metaTable.put(meta1);

        Thread.sleep(TimeUnit.SECONDS.toMillis(2));

        long s2 = System.currentTimeMillis();

        Thread.sleep(TimeUnit.SECONDS.toMillis(2));

        TimestreamMeta meta2 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("Cluster", "AY45W")
                        .addTag("Role", "OTSServer#")
                        .addTag("Machine", "eu13.rt238987")
                        .build())
                .addAttribute("owner", "pre-wanhong")
                .addAttribute("number", 20)
                .addAttribute("score", 20.0)
                .addAttribute("succ", false);
        metaTable.put(meta2);

        Helper.waitSync();

        {
            // equal
            Filter filter = and(
                    Name.equal("cpu"),
                    Attribute.equal("owner", "pre-wanhong"),
                    Attribute.equal("number", 20),
                    Attribute.equal("score", 20.0),
                    Attribute.equal("succ", false)
            );

            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .returnAll()
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(1, metas.size());
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta2));
        }
        {
            // not equal
            Filter filter = and(
                    Name.equal("cpu"),
                    Attribute.notEqual("owner", "pre-wanhong"),
                    Attribute.notEqual("number", 20),
                    Attribute.notEqual("score", 20.0),
                    Attribute.notEqual("succ", false)
            );

            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .returnAll()
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(1, metas.size());
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta1));
        }
        {
            // in
            Filter filter = and(
                    Name.equal("cpu"),
                    Attribute.in("owner", new String[]{"pre-wanhong", "pre-redchen"}),
                    Attribute.in("number", new long[]{10, 20}),
                    Attribute.in("score", new double[]{10.0, 20.0}),
                    Attribute.in("succ", new boolean[]{true, false})
            );

            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .returnAll()
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(2, metas.size());
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta1));
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta2));
        }
        {
            // not in
            Filter filter = and(
                    Name.equal("cpu"),
                    Attribute.notIn("owner", new String[]{"pre-wanhong"}),
                    Attribute.notIn("number", new long[]{20}),
                    Attribute.notIn("score", new double[]{20.0}),
                    Attribute.notIn("succ", new boolean[]{false})
            );

            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .returnAll()
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(1, metas.size());
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta1));
        }
        {
            // range
            Filter filter = and(
                    Name.equal("cpu"),
                    Attribute.inRange("owner", "pre-r", "pre-w"),
                    Attribute.inRange("number", 10, 20),
                    Attribute.inRange("score", 10.0, 20.0)
            );

            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .returnAll()
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(1, metas.size());
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta1));
        }
        {
            // prefix
            Filter filter = and(
                    Name.equal("cpu"),
                    Attribute.prefix("owner", "pre")
            );

            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .returnAll()
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(2, metas.size());
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta1));
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta2));
        }
        {
            // wildcard
            Filter filter = and(
                    Name.equal("cpu"),
                    Attribute.wildcard("owner", "p*")
            );

            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .returnAll()
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(2, metas.size());
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta1));
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta2));
        }
    }

    @Test
    public void testWithAttributeInArray() throws InterruptedException {
        String metaTableName = "tsmeta";
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        TimestreamDB db = new TimestreamDBClient(asyncClient, config);
        Helper.safeClearDB(asyncClient);
        List<AttributeIndexSchema> indexSchemas = new ArrayList<AttributeIndexSchema>();
        indexSchemas.add(new AttributeIndexSchema("owner", AttributeIndexSchema.Type.KEYWORD).setIsArray(true));
        indexSchemas.add(new AttributeIndexSchema("number", AttributeIndexSchema.Type.LONG).setIsArray(true));
        indexSchemas.add(new AttributeIndexSchema("score", AttributeIndexSchema.Type.DOUBLE).setIsArray(true));
        indexSchemas.add(new AttributeIndexSchema("succ", AttributeIndexSchema.Type.BOOLEAN).setIsArray(true));
        db.createMetaTable(indexSchemas);
        Thread.sleep(5000);
        TimestreamMetaTable metaTable = db.metaTable();

        long s1 = System.currentTimeMillis();
        TimestreamMeta meta1 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("Cluster", "AY45W")
                        .addTag("Role", "OTSServer#")
                        .addTag("Machine", "eu13.rt19032")
                        .build())
                .addAttribute("owner", "[\"pre-redchen1\", \"pre-redchen2\"]")
                .addAttribute("number", "[10, 11]")
                .addAttribute("score", "[10.0, 11.0]")
                .addAttribute("succ", "[true]");
        metaTable.put(meta1);

        Thread.sleep(TimeUnit.SECONDS.toMillis(2));

        long s2 = System.currentTimeMillis();

        Thread.sleep(TimeUnit.SECONDS.toMillis(2));

        TimestreamMeta meta2 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("Cluster", "AY45W")
                        .addTag("Role", "OTSServer#")
                        .addTag("Machine", "eu13.rt238987")
                        .build())
                .addAttribute("owner", "[\"pre-wanhong1\", \"pre-wanhong2\"]")
                .addAttribute("number", "[20, 21]")
                .addAttribute("score", "[20.0, 21.0]")
                .addAttribute("succ", "[false]");
        metaTable.put(meta2);

        Helper.waitSync();

        {
            // equal
            Filter filter = and(
                    Name.equal("cpu"),
                    Attribute.equal("owner", "pre-wanhong1"),
                    Attribute.equal("number", 20),
                    Attribute.equal("score", 20.0),
                    Attribute.equal("succ", false)
            );

            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .returnAll()
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(1, metas.size());
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta2));
        }
        {
            // not equal
            Filter filter = and(
                    Name.equal("cpu"),
                    Attribute.notEqual("owner", "pre-wanhong1"),
                    Attribute.notEqual("number", 20),
                    Attribute.notEqual("score", 20.0),
                    Attribute.notEqual("succ", false)
            );

            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .returnAll()
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(1, metas.size());
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta1));
        }
        {
            // in
            Filter filter = and(
                    Name.equal("cpu"),
                    Attribute.in("owner", new String[]{"pre-wanhong1", "pre-redchen1"}),
                    Attribute.in("number", new long[]{10, 20}),
                    Attribute.in("score", new double[]{10.0, 20.0}),
                    Attribute.in("succ", new boolean[]{true, false})
            );

            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .returnAll()
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(2, metas.size());
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta1));
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta2));
        }
        {
            // not in
            Filter filter = and(
                    Name.equal("cpu"),
                    Attribute.notIn("owner", new String[]{"pre-wanhong1"}),
                    Attribute.notIn("number", new long[]{20}),
                    Attribute.notIn("score", new double[]{20.0}),
                    Attribute.notIn("succ", new boolean[]{false})
            );

            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .returnAll()
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(1, metas.size());
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta1));
        }
        {
            // range
            Filter filter = and(
                    Name.equal("cpu"),
                    Attribute.inRange("owner", "pre-r", "pre-w"),
                    Attribute.inRange("number", 10, 20),
                    Attribute.inRange("score", 10.0, 20.0)
            );

            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .returnAll()
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(1, metas.size());
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta1));
        }
        {
            // prefix
            Filter filter = and(
                    Name.equal("cpu"),
                    Attribute.prefix("owner", "pre")
            );

            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .returnAll()
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(2, metas.size());
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta1));
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta2));
        }
        {
            // wildcard
            Filter filter = and(
                    Name.equal("cpu"),
                    Attribute.wildcard("owner", "p*")
            );

            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .returnAll()
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(2, metas.size());
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta1));
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta2));
        }
    }

    @Test
    public void testUpdateTimeFilter() throws InterruptedException {
        String metaTableName = "tsmeta";
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        TimestreamDB db = new TimestreamDBClient(asyncClient, config);
        Helper.safeClearDB(asyncClient);
        db.createMetaTable();
        Thread.sleep(5000);
        TimestreamMetaTable metaTable = db.metaTable();

        TimestreamMeta meta1 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("Cluster", "AY45W")
                        .addTag("Role", "OTSServer#")
                        .addTag("Machine", "eu13.rt19032")
                        .build())
                .addAttribute("owner", "redchen");
        metaTable.put(meta1);
        long ts1 = meta1.getUpdateTimeInUsec();

        TimestreamMeta meta2 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("Cluster", "AY45W-HA")
                        .addTag("Role", "OTSServer#")
                        .addTag("Machine", "eu13.rt19032")
                        .build())
                .addAttribute("owner", "redchen");
        long ts2 = ts1 + 80000000;
        meta2.setUpdateTime(ts2, TimeUnit.MICROSECONDS);
        metaTable.put(meta2);

        Helper.waitSync(); // wait 80s

        {
            // range
            Filter filter = and(
                    Name.equal("cpu"),
                    LastUpdateTime.in(TimeRange.range(ts1, ts2, TimeUnit.MICROSECONDS))
            );

            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .returnAll()
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(1, metas.size());
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta1));
        }
        {
            // after
            Filter filter = and(
                    Name.equal("cpu"),
                    LastUpdateTime.in(TimeRange.after(ts1, TimeUnit.MICROSECONDS))
            );

            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .returnAll()
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(2, metas.size());
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta1));
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta2));
        }
        {
            // before
            Filter filter = and(
                    Name.equal("cpu"),
                    LastUpdateTime.in(TimeRange.before(ts2, TimeUnit.MICROSECONDS))
            );

            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .returnAll()
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(1, metas.size());
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta1));
        }
        {
            // latest
            Filter filter = and(
                    Name.equal("cpu"),
                    LastUpdateTime.in(TimeRange.latest(10, TimeUnit.SECONDS))
            );

            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .returnAll()
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(1, metas.size());
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta2));
        }
    }

    @Test
    public void testNoFilter() throws InterruptedException {
        String metaTableName = "tsmeta";
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        TimestreamDB db = new TimestreamDBClient(asyncClient, config);
        Helper.safeClearDB(asyncClient);
        db.createMetaTable();
        Thread.sleep(5000);
        TimestreamMetaTable metaTable = db.metaTable();

        long s1 = System.currentTimeMillis();
        TimestreamMeta meta1 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("Cluster", "AY45W")
                        .addTag("Role", "OTSServer#")
                        .addTag("Machine", "eu13.rt19032")
                        .build())
                .addAttribute("owner", "redchen");
        metaTable.put(meta1);

        Thread.sleep(TimeUnit.SECONDS.toMillis(2));

        long s2 = System.currentTimeMillis();

        Thread.sleep(TimeUnit.SECONDS.toMillis(2));

        TimestreamMeta meta2 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("Cluster", "AY45W")
                        .addTag("Role", "OTSServer#")
                        .addTag("Machine", "eu13.rt238987")
                        .build())
                .addAttribute("owner", "wanhong");
        metaTable.put(meta2);

        Helper.waitSync();

        {
            Filter filter = Name.equal("cpu");

            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .returnAll()
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(2, metas.size());
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta1));
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta2));
        }
    }

    @Test
    public void testNoMetaTable() throws ExecutionException, InterruptedException {
        String metaTableName = "tsmeta";
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());

        Helper.safeClearDB(asyncClient);
        TimestreamDB db = new TimestreamDBClient(asyncClient, config);

        {
            try {
                db.metaTable();
                Assert.fail();
            } catch (TableStoreException e) {
                e.getMessage().contains("Requested table does not exist.");
            }
        }
    }

    // -----------------------》 with location 《-----------------------

    @Test
    public void testLocationCircle() throws Exception {
        String metaTableName = "tsmeta";
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        TimestreamDB db = new TimestreamDBClient(asyncClient, config);

        Helper.safeClearDB(asyncClient);
        List<AttributeIndexSchema> indexSchemas = new ArrayList<AttributeIndexSchema>();
        indexSchemas.add(new AttributeIndexSchema("loc", AttributeIndexSchema.Type.GEO_POINT));
        db.createMetaTable(indexSchemas);
        Thread.sleep(5000);
        TimestreamMetaTable metaTable = db.metaTable();

        TimestreamMeta meta1 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("company")
                        .addTag("name", "alibaba1")
                        .build())
                .addAttribute("loc", "30.130370, 120.083263"); // 飞天园区

        TimestreamMeta meta2 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("company")
                        .addTag("name", "alibaba2")
                        .build())
                .addAttribute("loc", "30.129237, 120.088380"); // 钱江Block

        TimestreamMeta meta3 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("company")
                        .addTag("name", "alibaba3")
                        .build())
                .addAttribute("loc", "30.130188, 120.082112"); // 中大

        TimestreamMeta meta4 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("company")
                        .addTag("name", "alibaba4")
                        .build())
                .addAttribute("loc", "31.207134, 121.343689"); // 上海虹桥

        TimestreamMeta meta5 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("company")
                        .addTag("name", "aliyun1")
                        .build())
                .addAttribute("loc", "30.280175, 120.024933"); // 未来科技城

        metaTable.put(meta1);
        metaTable.put(meta2);
        metaTable.put(meta3);
        metaTable.put(meta4);
        metaTable.put(meta5);

        Helper.waitSync();


        // 简单的圆查找
        {
            Filter filter = and(
                    Name.equal("company"),
                    Attribute.inGeoDistance("loc", "30.130370, 120.083263", 50 * 1000)
            );

            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .returnAll()
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(4, metas.size());
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta1));
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta2));
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta3));
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta5));
        }

        // 简单的圆查找 未命中
        {
            Filter filter = and(
                    Name.equal("company"),
                    Attribute.inGeoDistance("loc", "40.262348,120.092127", 0.1)
            );

            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .returnAll()
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(0, metas.size());
        }
    }

    @Test
    public void testLocationRectangle() throws Exception{
        String metaTableName = "tsmeta";
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        TimestreamDB db = new TimestreamDBClient(asyncClient, config);

        Helper.safeClearDB(asyncClient);
        List<AttributeIndexSchema> indexSchemas = new ArrayList<AttributeIndexSchema>();
        indexSchemas.add(new AttributeIndexSchema("loc", AttributeIndexSchema.Type.GEO_POINT));
        db.createMetaTable(indexSchemas);
        Thread.sleep(5000);
        TimestreamMetaTable metaTable = db.metaTable();

        TimestreamMeta meta1 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("company")
                        .addTag("name", "alibaba1")
                        .build())
                .addAttribute("loc", "30.130370, 120.083263"); // 飞天园区

        TimestreamMeta meta2 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("company")
                        .addTag("name", "alibaba2")
                        .build())
                .addAttribute("loc", "30.129237, 120.088380"); // 钱江Block

        TimestreamMeta meta3 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("company")
                        .addTag("name", "alibaba3")
                        .build())
                .addAttribute("loc", "30.130188, 120.082112"); // 中大

        TimestreamMeta meta4 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("company")
                        .addTag("name", "alibaba4")
                        .build())
                .addAttribute("loc", "31.207134, 121.343689"); // 上海虹桥

        TimestreamMeta meta5 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("company")
                        .addTag("name", "aliyun1")
                        .build())
                .addAttribute("loc", "30.280175, 120.024933"); // 未来科技城

        metaTable.put(meta1);
        metaTable.put(meta2);
        metaTable.put(meta3);
        metaTable.put(meta4);
        metaTable.put(meta5);

        Helper.waitSync();

        {
            Filter filter = and(
                    Name.equal("company"),
                    Attribute.inGeoBoundingBox("loc", "30.141097, 120.072030", "30.122962, 120.097807")
            );

            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .returnAll()
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(3, metas.size());
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta1));
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta2));
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta3));
        }

        {
            Filter filter = and(
                    Name.equal("company"),
                    Attribute.inGeoBoundingBox("loc", "40.141097, 120.072030", "40.122962, 120.097807")
            );

            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .returnAll()
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(0, metas.size());
        }
    }

    @Test
    public void testLocationPolygon() throws Exception {
        String metaTableName = "tsmeta";
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        TimestreamDB db = new TimestreamDBClient(asyncClient, config);

        Helper.safeClearDB(asyncClient);
        List<AttributeIndexSchema> indexSchemas = new ArrayList<AttributeIndexSchema>();
        indexSchemas.add(new AttributeIndexSchema("loc", AttributeIndexSchema.Type.GEO_POINT));
        db.createMetaTable(indexSchemas);
        Thread.sleep(5000);
        TimestreamMetaTable metaTable = db.metaTable();

        TimestreamMeta meta1 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("company")
                        .addTag("name", "alibaba1")
                        .build())
                .addAttribute("loc", "30.130370, 120.083263"); // 飞天园区

        TimestreamMeta meta2 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("company")
                        .addTag("name", "alibaba2")
                        .build())
                .addAttribute("loc", "30.129237, 120.088380"); // 钱江Block

        TimestreamMeta meta3 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("company")
                        .addTag("name", "alibaba3")
                        .build())
                .addAttribute("loc", "30.130188, 120.082112"); // 中大

        TimestreamMeta meta4 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("company")
                        .addTag("name", "alibaba4")
                        .build())
                .addAttribute("loc", "31.207134, 121.343689"); // 上海虹桥

        TimestreamMeta meta5 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("company")
                        .addTag("name", "aliyun1")
                        .build())
                .addAttribute("loc", "30.280175, 120.024933"); // 未来科技城

        metaTable.put(meta1);
        metaTable.put(meta2);
        metaTable.put(meta3);
        metaTable.put(meta4);
        metaTable.put(meta5);

        Helper.waitSync();

        {
            List<String> points = Arrays.asList(
                    "30.130645, 120.079297",
                    "30.132399, 120.083171",
                    "30.128854, 120.083825",
                    "30.127931, 120.081095"
            );
            Filter filter = and(
                    Name.equal("company"),
                    Attribute.inGeoPolygon("loc", points)
            );

            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .returnAll()
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(2, metas.size());
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta1));
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta3));
        }

        {
            List<String> points = Arrays.asList(
                    "31.130645, 120.079297",
                    "31.132399, 120.083171",
                    "31.128854, 120.083825",
                    "31.127931, 120.081095"
            );
            Filter filter = and(
                    Name.equal("company"),
                    Attribute.inGeoPolygon("loc", points)
            );

            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .returnAll()
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(0, metas.size());
        }
    }

    @Test
    public void test10000Meta() throws Exception {
        String metaTableName = "tsmeta";
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        TimestreamDB db = new TimestreamDBClient(asyncClient, config);

        Helper.safeClearDB(asyncClient);
        db.createMetaTable();
        Thread.sleep(5000);
        TimestreamMetaTable metaTable = db.metaTable();

        for (int i = 0; i < 10000; i++) {
            TimestreamIdentifier identifier = new TimestreamIdentifier.Builder("company")
                    .addTag("name", String.format("alibaba%04d", i)).build();
            TimestreamMeta meta = new TimestreamMeta(identifier)
                    .addAttribute("loc", "30.130370, 120.083263");
            metaTable.put(meta);
        }

        Helper.waitSync();

        Filter filter = Name.equal("company");
        TimestreamMetaIterator iterator = (TimestreamMetaIterator)metaTable
                .filter(filter)
                .returnAll()
                .fetchAll();

        List<TimestreamMeta> out = new ArrayList<TimestreamMeta>();
        while (iterator.hasNext()) {
            out.add(iterator.next());
        }
        Assert.assertEquals(10000, out.size());
    }

    @Test
    public void testMaxLen() throws Exception {
        TimestreamIdentifier identifier = null;
        {
            TimestreamIdentifier.Builder builder = new TimestreamIdentifier.Builder("cpu");
            String key1 = "a";
            String key2 = "b";
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < (TimestreamRestrict.TAG_LEN_BYTE - 2) / 2; ++i) {
                sb.append("a");
            }
            String value = sb.toString();
            builder.addTag(key1, value);
            builder.addTag(key2, value);
            identifier = builder.build();
        }
        TimestreamMeta meta = new TimestreamMeta(identifier);
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < TimestreamRestrict.ATTR_LEN_BYTE * 2 / TimestreamRestrict.ATTR_COUNT - 1; ++i) {
                sb.append("a");
            }
            String value = sb.toString();
            for (int i = 0; i < TimestreamRestrict.ATTR_COUNT / 2; ++i) {
                meta.addAttribute(String.format("%s", i), value);
            }
        }


        String metaTableName = "tsmeta";
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        TimestreamDB db = new TimestreamDBClient(asyncClient, config);

        Helper.safeClearDB(asyncClient);
        db.createMetaTable();
        Thread.sleep(5000);
        TimestreamMetaTable metaTable = db.metaTable();
        metaTable.put(meta);

        Helper.waitSync();

        Filter filter = Name.equal("cpu");
        Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                .returnAll()
                .fetchAll();
        List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
        while(iterator.hasNext()) {
            metas.add(iterator.next());
        }
        Assert.assertEquals(1, metas.size());
        Assert.assertTrue(Helper.isContaineMeta(metas, meta));
    }

    @Test
    public void testLimitOffset() throws ExecutionException, InterruptedException {
        String metaTableName = "tsmeta";
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        TimestreamDB db = new TimestreamDBClient(asyncClient, config);
        Helper.safeClearDB(asyncClient);
        db.createMetaTable();
        Thread.sleep(5000);
        TimestreamMetaTable metaTable = db.metaTable();

        long s1 = System.currentTimeMillis();
        TimestreamMeta meta1 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("Cluster", "AY45W")
                        .addTag("Role", "OTSServer#")
                        .addTag("Machine", "eu13.rt19032")
                        .build())
                .addAttribute("owner", "redchen");
        metaTable.put(meta1);

        TimestreamMeta meta2 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("Cluster", "AY45W")
                        .addTag("Role", "OTSServer#")
                        .addTag("Machine", "eu13.rt238987")
                        .build())
                .addAttribute("owner", "wanhong");
        metaTable.put(meta2);

        TimestreamMeta meta3 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("Cluster", "AY45W")
                        .addTag("Role", "OTSServer#")
                        .addTag("Machine", "eu13.rt37068")
                        .build())
                .addAttribute("owner", "wanhong");
        metaTable.put(meta3);


        Helper.waitSync();

        {
            Filter filter = Name.equal("cpu");

            TimestreamMetaIterator iterator = metaTable.filter(filter)
                    .returnAll()
                    .limit(1)
                    .fetchAll();
            Assert.assertEquals(3, iterator.getTotalCount());
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta1));
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta2));
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta3));
        }

        {
            Filter filter = Name.equal("cpu");

            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .returnAll()
                    .limit(1)
                    .offset(1)
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(2, metas.size());
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta1));
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta3));
        }

        {
            Filter filter = Name.equal("cpu");

            Iterator<TimestreamMeta> iterator = metaTable.filter(filter)
                    .returnAll()
                    .limit(1)
                    .offset(2)
                    .fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(1, metas.size());
            Assert.assertEquals(true, Helper.isContaineMeta(metas, meta3));
        }
    }

    @Test
    public void testEmpty() throws ExecutionException, InterruptedException {
        String metaTableName = "tsmeta";
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        TimestreamDB db = new TimestreamDBClient(asyncClient, config);
        Helper.safeClearDB(asyncClient);
        db.createMetaTable();
        Thread.sleep(5000);
        TimestreamMetaTable metaTable = db.metaTable();

        long s1 = System.currentTimeMillis();
        TimestreamMeta meta1 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("Cluster", "AY45W")
                        .addTag("Role", "OTSServer#")
                        .addTag("Machine", "eu13.rt19032")
                        .build())
                .addAttribute("owner", "redchen");
        {
            TimestreamMeta meta = metaTable.get(meta1.getIdentifier()).returnAll().fetch();
            Assert.assertTrue(meta == null);
        }
        {
            Iterator<TimestreamMeta> iterator = metaTable.filter().returnAll().fetchAll();
            Assert.assertTrue(!iterator.hasNext());
        }
    }

    @Test
    public void testAll() throws ExecutionException, InterruptedException {
        String metaTableName = "tsmeta";
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        TimestreamDB db = new TimestreamDBClient(asyncClient, config);
        Helper.safeClearDB(asyncClient);
        db.createMetaTable();
        Thread.sleep(5000);
        TimestreamMetaTable metaTable = db.metaTable();

        long s1 = System.currentTimeMillis();
        TimestreamMeta meta1 = new TimestreamMeta(
                new TimestreamIdentifier.Builder("cpu")
                        .addTag("Cluster", "AY45W")
                        .addTag("Role", "OTSServer#")
                        .addTag("Machine", "eu13.rt19032")
                        .build())
                .addAttribute("owner", "redchen");
        metaTable.put(meta1);

        Helper.waitSync();

        {
            Iterator<TimestreamMeta> iterator = metaTable.filter().returnAll().fetchAll();
            List<TimestreamMeta> metas = new ArrayList<TimestreamMeta>();
            while(iterator.hasNext()) {
                metas.add(iterator.next());
            }
            Assert.assertEquals(1, metas.size());
            Assert.assertTrue(Helper.isContaineMeta(metas, meta1));
        }
    }
}
