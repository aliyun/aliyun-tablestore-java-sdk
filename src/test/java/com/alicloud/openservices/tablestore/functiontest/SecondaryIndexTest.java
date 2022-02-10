package com.alicloud.openservices.tablestore.functiontest;

import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.common.BaseFT;
import com.alicloud.openservices.tablestore.common.OTSHelper;
import com.alicloud.openservices.tablestore.common.Utils;
import com.alicloud.openservices.tablestore.model.*;
import com.google.gson.JsonSyntaxException;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SecondaryIndexTest extends BaseFT {

    private static String tableName = "SecondaryIndexFunctiontest";
    private static SyncClientInterface ots;

    private static final Logger LOG = LoggerFactory.getLogger(SecondaryIndexTest.class);

    @BeforeClass
    public static void classBefore() throws JsonSyntaxException, IOException {
        ots = Utils.getOTSInstance();
    }

    @AfterClass
    public static void classAfter() {
        ots.shutdown();
    }

    @Before
    public void setup() throws Exception {
        OTSHelper.deleteAllTable(ots);
    }

    @After
    public void teardown() {

    }

    @Test
    public void testCase1() {
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("uid", PrimaryKeyType.STRING);
        TableOptions tableOptions = new TableOptions();
        tableOptions.setTimeToLive(-1);
        tableOptions.setMaxVersions(1);

        CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, tableOptions);
        ots.createTable(createTableRequest);
    }

    @Test
    public void testCase2_1() throws Exception {
        int limit = 5;
        {
            TableMeta tableMeta = new TableMeta(tableName);
            tableMeta.addPrimaryKeyColumn("uid", PrimaryKeyType.STRING);

            tableMeta.addDefinedColumn("d0", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d1", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d2", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d3", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d4", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d5", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d6", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d7", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d8", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d9", DefinedColumnType.STRING);

            TableOptions tableOptions = new TableOptions();
            tableOptions.setTimeToLive(-1);
            tableOptions.setMaxVersions(1);

            CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, tableOptions);

            for (int i = 0; i < limit + 1; i++) {
                IndexMeta indexMeta = new IndexMeta("i" + i);
                indexMeta.addPrimaryKeyColumn("d" + i);
                indexMeta.addDefinedColumn("d9");
                createTableRequest.addIndex(indexMeta);
            }
            try {
                ots.createTable(createTableRequest);
                Assert.fail();
            } catch (TableStoreException e) {
                Assert.assertEquals("OTSParameterInvalid", e.getErrorCode());
                Assert.assertEquals("Number of index per table exceeds the quota:"+ limit +".", e.getMessage());
            }
        }
        {
            TableMeta tableMeta = new TableMeta(tableName);
            tableMeta.addPrimaryKeyColumn("uid", PrimaryKeyType.STRING);

            tableMeta.addDefinedColumn("d0", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d1", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d2", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d3", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d4", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d5", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d6", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d7", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d8", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d9", DefinedColumnType.STRING);

            TableOptions tableOptions = new TableOptions();
            tableOptions.setTimeToLive(-1);
            tableOptions.setMaxVersions(1);

            CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, tableOptions);

            for (int i = 0; i < limit; i++) {
                IndexMeta indexMeta = new IndexMeta("i" + i);
                indexMeta.addPrimaryKeyColumn("d" + i);
                indexMeta.addDefinedColumn("d9");
                createTableRequest.addIndex(indexMeta);
            }
            ots.createTable(createTableRequest);
        }
    }

    @Test
    public void testCase2_2() throws Exception {
        int limit = 5;

        {
            TableMeta tableMeta = new TableMeta(tableName);
            tableMeta.addPrimaryKeyColumn("uid", PrimaryKeyType.STRING);

            tableMeta.addDefinedColumn("d0", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d1", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d2", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d3", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d4", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d5", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d6", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d7", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d8", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d9", DefinedColumnType.STRING);

            TableOptions tableOptions = new TableOptions();
            tableOptions.setTimeToLive(-1);
            tableOptions.setMaxVersions(1);

            CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, tableOptions);
            ots.createTable(createTableRequest);
        }

        Utils.waitForPartitionLoad(tableName);

        for (int i = 0; i < limit; i++) {
            IndexMeta indexMeta = new IndexMeta("i" + i);
            indexMeta.addPrimaryKeyColumn("d" + i);
            indexMeta.addDefinedColumn("d9");
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(tableName, indexMeta, false);

            ots.createIndex(createIndexRequest);
        }

        Utils.waitForPartitionLoad(tableName);

        {
            IndexMeta indexMeta = new IndexMeta("i" + limit);
            indexMeta.addPrimaryKeyColumn("d" + limit);
            indexMeta.addDefinedColumn("d9");
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(tableName, indexMeta, false);

            try {
                ots.createIndex(createIndexRequest);
                Assert.fail();
            } catch (TableStoreException e) {
                Assert.assertEquals("OTSParameterInvalid", e.getErrorCode());
                Assert.assertEquals("Number of index per table exceeds the quota:"+ limit +".", e.getMessage());
            }
        }
    }

    @Test
    public void testCase4() {
        int limit = 32;
        {
            TableMeta tableMeta = new TableMeta(tableName);
            tableMeta.addPrimaryKeyColumn("uid", PrimaryKeyType.STRING);

            for (int i = 0; i < limit + 1; i++) {
                tableMeta.addDefinedColumn("d" + i, DefinedColumnType.STRING);
            }

            TableOptions tableOptions = new TableOptions();
            tableOptions.setTimeToLive(-1);
            tableOptions.setMaxVersions(1);

            CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, tableOptions);
            try {
                ots.createTable(createTableRequest);
                Assert.fail();
            } catch (TableStoreException e) {
                Assert.assertEquals("OTSParameterInvalid", e.getErrorCode());
                Assert.assertEquals("The number of defined columns must be in range: [0, 32].", e.getMessage());
            }
        }
        {
            TableMeta tableMeta = new TableMeta(tableName);
            tableMeta.addPrimaryKeyColumn("uid", PrimaryKeyType.STRING);

            for (int i = 0; i < limit; i++) {
                tableMeta.addDefinedColumn("d" + i, DefinedColumnType.STRING);
            }

            TableOptions tableOptions = new TableOptions();
            tableOptions.setTimeToLive(-1);
            tableOptions.setMaxVersions(1);

            CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, tableOptions);
            ots.createTable(createTableRequest);
        }
    }

    @Test
    public void testCase3() throws Exception {
        int limit = 4;

        // by create table
        {
            TableMeta tableMeta = new TableMeta(tableName);
            tableMeta.addPrimaryKeyColumn("uid", PrimaryKeyType.STRING);

            tableMeta.addDefinedColumn("d0", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d1", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d2", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d3", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d4", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d5", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d6", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d7", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d8", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d9", DefinedColumnType.STRING);

            TableOptions tableOptions = new TableOptions();
            tableOptions.setTimeToLive(-1);
            tableOptions.setMaxVersions(1);

            CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, tableOptions);
            IndexMeta indexMeta = new IndexMeta("i");
            for (int i = 0; i < limit + 1 ; i++) {
                indexMeta.addPrimaryKeyColumn("d" + i);
            }
            indexMeta.addDefinedColumn("d9");
            createTableRequest.addIndex(indexMeta);
            try {
                ots.createTable(createTableRequest);
                Assert.fail();
            } catch (TableStoreException e) {
                Assert.assertEquals("OTSParameterInvalid", e.getErrorCode());
                Assert.assertEquals("The number of primary key columns must be in range: [1, 4] in index.", e.getMessage());
            }
        }

        // by create index
        {
            IndexMeta indexMeta = new IndexMeta("i");
            for (int i = 0; i < limit + 1; i++) {
                indexMeta.addPrimaryKeyColumn("d" + i);
            }
            indexMeta.addDefinedColumn("d9");
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(tableName, indexMeta, false);

            try {
                ots.createIndex(createIndexRequest);
                Assert.fail();
            } catch (TableStoreException e) {
                Assert.assertEquals("OTSParameterInvalid", e.getErrorCode());
                Assert.assertEquals("The number of primary key columns must be in range: [1, 4] in index.", e.getMessage());
            }
        }
    }

    @Test
    public void testCase5() {
        int limit = 32;
        {
            TableMeta tableMeta = new TableMeta(tableName);
            tableMeta.addPrimaryKeyColumn("uid", PrimaryKeyType.STRING);
            tableMeta.addPrimaryKeyColumn("gid", PrimaryKeyType.STRING);

            for (int i = 0; i < limit; i++) {
                tableMeta.addDefinedColumn("d" + i, DefinedColumnType.STRING);
            }

            TableOptions tableOptions = new TableOptions();
            tableOptions.setTimeToLive(-1);
            tableOptions.setMaxVersions(1);

            CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, tableOptions);
            IndexMeta indexMeta = new IndexMeta("i");
            indexMeta.addPrimaryKeyColumn("gid");
            for (int i = 0; i < limit + 1; i++) {
                indexMeta.addDefinedColumn("d" + i);
            }
            createTableRequest.addIndex(indexMeta);
            try {
                ots.createTable(createTableRequest);
                Assert.fail();
            } catch (TableStoreException e) {
                Assert.assertEquals("OTSParameterInvalid", e.getErrorCode());
                Assert.assertEquals("The number of attribute columns must be in range: [0, 32] in index.", e.getMessage());
            }
        }
        {

            TableMeta tableMeta = new TableMeta(tableName);
            tableMeta.addPrimaryKeyColumn("uid", PrimaryKeyType.STRING);
            tableMeta.addPrimaryKeyColumn("gid", PrimaryKeyType.STRING);

            for (int i = 0; i < limit; i++) {
                tableMeta.addDefinedColumn("d" + i, DefinedColumnType.STRING);
            }

            TableOptions tableOptions = new TableOptions();
            tableOptions.setTimeToLive(-1);
            tableOptions.setMaxVersions(1);

            CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, tableOptions);
            IndexMeta indexMeta = new IndexMeta("i");
            indexMeta.addPrimaryKeyColumn("gid");
            for (int i = 0; i < limit; i++) {
                indexMeta.addDefinedColumn("d" + i);
            }
            createTableRequest.addIndex(indexMeta);
            ots.createTable(createTableRequest);
        }
    }

    @Test
    public void testCase6() {
        // by create table
        {
            TableMeta tableMeta = new TableMeta(tableName);
            tableMeta.addPrimaryKeyColumn("uid", PrimaryKeyType.STRING);

            tableMeta.addDefinedColumn("d0", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d1", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d2", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d3", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d4", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d5", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d6", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d7", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d8", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d9", DefinedColumnType.STRING);

            TableOptions tableOptions = new TableOptions();
            tableOptions.setTimeToLive(-1);
            tableOptions.setMaxVersions(1);

            CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, tableOptions);
            IndexMeta indexMeta = new IndexMeta("i");
            // indexMeta.addPrimaryKeyColumn("d"); // no pk
            indexMeta.addDefinedColumn("d9");
            createTableRequest.addIndex(indexMeta);
            try {
                ots.createTable(createTableRequest);
                Assert.fail();
            } catch (TableStoreException e) {
                Assert.assertEquals("OTSParameterInvalid", e.getErrorCode());
                Assert.assertEquals("The number of primary key columns must be in range: [1, 4] in index.", e.getMessage());
            }
        }
        {
            TableMeta tableMeta = new TableMeta(tableName);
            tableMeta.addPrimaryKeyColumn("uid", PrimaryKeyType.STRING);

            tableMeta.addDefinedColumn("d0", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d1", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d2", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d3", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d4", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d5", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d6", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d7", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d8", DefinedColumnType.STRING);
            tableMeta.addDefinedColumn("d9", DefinedColumnType.STRING);

            TableOptions tableOptions = new TableOptions();
            tableOptions.setTimeToLive(-1);
            tableOptions.setMaxVersions(1);

            CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, tableOptions);
            IndexMeta indexMeta = new IndexMeta("i");
            indexMeta.addPrimaryKeyColumn("d0");
            // indexMeta.addDefinedColumn("d9");// no attr
            createTableRequest.addIndex(indexMeta);
            ots.createTable(createTableRequest);
        }
        {

            IndexMeta indexMeta = new IndexMeta("i1");
            //indexMeta.addPrimaryKeyColumn("d0");
            indexMeta.addDefinedColumn("d9");
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(tableName, indexMeta, false);

            try {
                ots.createIndex(createIndexRequest);
                Assert.fail();
            } catch (TableStoreException e) {
                Assert.assertEquals("OTSParameterInvalid", e.getErrorCode());
                Assert.assertEquals("The number of primary key columns must be in range: [1, 4] in index.", e.getMessage());
            }
        }
        {

            IndexMeta indexMeta = new IndexMeta("i2");
            indexMeta.addPrimaryKeyColumn("d0");
            //indexMeta.addDefinedColumn("d9");
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(tableName, indexMeta, false);
            ots.createIndex(createIndexRequest);
        }
    }
}
