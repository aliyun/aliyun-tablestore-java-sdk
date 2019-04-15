package com.alicloud.openservices.tablestore.timestream.functiontest;

import com.alicloud.openservices.tablestore.timestream.bench.Conf;
import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.core.ErrorCode;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.search.*;
import com.alicloud.openservices.tablestore.timestream.TimestreamDB;
import com.alicloud.openservices.tablestore.timestream.TimestreamDBClient;
import com.alicloud.openservices.tablestore.timestream.TimestreamDBConfiguration;
import com.alicloud.openservices.tablestore.timestream.internal.TableMetaGenerator;
import com.alicloud.openservices.tablestore.timestream.internal.Utils;
import com.alicloud.openservices.tablestore.timestream.model.AttributeIndexSchema;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class TableOperationFunctiontest {
    private static Logger logger = LoggerFactory.getLogger(TableOperationFunctiontest.class);
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
    public void testMetaTable() throws ExecutionException, InterruptedException {
        String metaTableName = "metaTable";
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        Helper.safeClearDB(asyncClient);
        {
            ListTableResponse ltResp = Utils.waitForFuture(asyncClient.listTable(null));
            Assert.assertEquals(ltResp.getTableNames().size(), 0);
        }

        TimestreamDB db = new TimestreamDBClient(asyncClient, config);
        db.createMetaTable();
        {
            ListTableResponse ltResp = Utils.waitForFuture(asyncClient.listTable(null));
            Assert.assertEquals(ltResp.getTableNames().size(), 1);
            Assert.assertTrue(ltResp.getTableNames().get(0).equals(metaTableName));

            ListSearchIndexRequest request = new ListSearchIndexRequest();
            request.setTableName(metaTableName);
            ListSearchIndexResponse lsResp = Utils.waitForFuture(asyncClient.listSearchIndex(request, null));
            Assert.assertEquals(lsResp.getIndexInfos().size(), 1);
            Assert.assertTrue(lsResp.getIndexInfos().get(0).getTableName().equals(metaTableName));
            Assert.assertTrue(lsResp.getIndexInfos().get(0).getIndexName().equals(metaTableName + "_INDEX"));
        }

        try {
            db.createMetaTable();
            Assert.fail();
        } catch (TableStoreException e) {
            Assert.assertTrue(e.getErrorCode().equals(ErrorCode.OBJECT_ALREADY_EXIST));
        }

        db.deleteMetaTable();
        {
            ListTableResponse ltResp = Utils.waitForFuture(asyncClient.listTable(null));
            Assert.assertEquals(ltResp.getTableNames().size(), 0);
        }

        try {
            db.deleteMetaTable();
            Assert.fail();
        } catch (TableStoreException e) {
            Assert.assertTrue(e.getErrorCode().equals(ErrorCode.INVALID_PARAMETER));
        }
    }

    @Test
    public void testMetaTableWithInvalidAttribute() throws ExecutionException, InterruptedException {
        String metaTableName = "metaTable";
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        Helper.safeClearDB(asyncClient);
        {
            ListTableResponse ltResp = Utils.waitForFuture(asyncClient.listTable(null));
            Assert.assertEquals(ltResp.getTableNames().size(), 0);
        }

        TimestreamDB db = new TimestreamDBClient(asyncClient, config);
        {
            List<AttributeIndexSchema> attrIndex = new ArrayList<AttributeIndexSchema>();
            attrIndex.add(new AttributeIndexSchema(TableMetaGenerator.CN_PK0, AttributeIndexSchema.Type.BOOLEAN));
            try {
                db.createMetaTable(attrIndex);
                Assert.assertTrue(false);
            } catch (ClientException e) {
                Assert.assertEquals("Name of attribute for indexes cannot be " +
                        TableMetaGenerator.CN_PK0 + "/" +
                        TableMetaGenerator.CN_PK1 + "/" +
                        TableMetaGenerator.CN_PK2 + "/" +
                        TableMetaGenerator.CN_TAMESTAMP_NAME + ".", e.getMessage());
            }
        }
        {
            List<AttributeIndexSchema> attrIndex = new ArrayList<AttributeIndexSchema>();
            attrIndex.add(new AttributeIndexSchema(TableMetaGenerator.CN_PK1, AttributeIndexSchema.Type.BOOLEAN));
            try {
                db.createMetaTable(attrIndex);
                Assert.assertTrue(false);
            } catch (ClientException e) {
                Assert.assertEquals("Name of attribute for indexes cannot be " +
                        TableMetaGenerator.CN_PK0 + "/" +
                        TableMetaGenerator.CN_PK1 + "/" +
                        TableMetaGenerator.CN_PK2 + "/" +
                        TableMetaGenerator.CN_TAMESTAMP_NAME + ".", e.getMessage());
            }
        }
        {
            List<AttributeIndexSchema> attrIndex = new ArrayList<AttributeIndexSchema>();
            attrIndex.add(new AttributeIndexSchema(TableMetaGenerator.CN_PK2, AttributeIndexSchema.Type.BOOLEAN));
            try {
                db.createMetaTable(attrIndex);
                Assert.assertTrue(false);
            } catch (ClientException e) {
                Assert.assertEquals("Name of attribute for indexes cannot be " +
                        TableMetaGenerator.CN_PK0 + "/" +
                        TableMetaGenerator.CN_PK1 + "/" +
                        TableMetaGenerator.CN_PK2 + "/" +
                        TableMetaGenerator.CN_TAMESTAMP_NAME + ".", e.getMessage());
            }
        }
        {
            List<AttributeIndexSchema> attrIndex = new ArrayList<AttributeIndexSchema>();
            attrIndex.add(new AttributeIndexSchema(TableMetaGenerator.CN_TAMESTAMP_NAME, AttributeIndexSchema.Type.BOOLEAN));
            try {
                db.createMetaTable(attrIndex);
                Assert.assertTrue(false);
            } catch (ClientException e) {
                Assert.assertEquals("Name of attribute for indexes cannot be " +
                        TableMetaGenerator.CN_PK0 + "/" +
                        TableMetaGenerator.CN_PK1 + "/" +
                        TableMetaGenerator.CN_PK2 + "/" +
                        TableMetaGenerator.CN_TAMESTAMP_NAME + ".", e.getMessage());
            }
        }
    }

    @Test
    public void testDataTable() throws ExecutionException, InterruptedException {
        String metaTableName = "metaTable";
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        Helper.safeClearDB(asyncClient);
        {
            ListTableResponse ltResp = Utils.waitForFuture(asyncClient.listTable(null));
            Assert.assertEquals(ltResp.getTableNames().size(), 0);
        }

        TimestreamDB db = new TimestreamDBClient(asyncClient, config);
        String dataTableName = "dataTable";
        db.createDataTable(dataTableName);
        {
            ListTableResponse ltResp = Utils.waitForFuture(asyncClient.listTable(null));
            Assert.assertEquals(ltResp.getTableNames().size(), 1);
            Assert.assertTrue(ltResp.getTableNames().get(0).equals(dataTableName));

            ListSearchIndexRequest request = new ListSearchIndexRequest();
            request.setTableName(dataTableName);
            ListSearchIndexResponse lsResp = Utils.waitForFuture(asyncClient.listSearchIndex(request, null));
            Assert.assertEquals(lsResp.getIndexInfos().size(), 0);
        }

        try {
            db.createDataTable(dataTableName);
            Assert.fail();
        } catch (TableStoreException e) {
            Assert.assertTrue(e.getErrorCode().equals(ErrorCode.OBJECT_ALREADY_EXIST));
        }

        db.deleteDataTable(dataTableName);
        {
            ListTableResponse ltResp = Utils.waitForFuture(asyncClient.listTable(null));
            Assert.assertEquals(ltResp.getTableNames().size(), 0);
        }

        try {
            db.deleteDataTable(dataTableName);
            Assert.fail();
        } catch (TableStoreException e) {
            Assert.assertTrue(e.getErrorCode().equals(ErrorCode.OBJECT_NOT_EXIST));
        }
    }

    @Test
    public void testIllegalMetaTable() throws ExecutionException, InterruptedException {
        /**
         * 同名表存在，但schema不一致，预期报错
         */
        String metaTableName = "metaTable";
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        Helper.safeClearDB(asyncClient);

        TimestreamDB db = new TimestreamDBClient(asyncClient, config);
        {
            // pk数量不一致
            TableMeta tableMeta = new TableMeta(metaTableName);
            tableMeta.addPrimaryKeyColumn(TableMetaGenerator.CN_PK0, PrimaryKeyType.STRING);

            TableOptions tableOptions = new TableOptions();
            CreateTableRequest request = new CreateTableRequest(
                    tableMeta, tableOptions);
            tableOptions.setMaxVersions(1);
            tableOptions.setTimeToLive(-1);
            Future<CreateTableResponse> res = asyncClient.createTable(request, null);
            Utils.waitForFuture(res);

            Thread.sleep(1000);

            try {
                db.metaTable();
                Assert.fail();
            } catch (ClientException e) {
                Assert.assertEquals(e.getMessage(), "Same table with different meta exist: " + metaTableName);
            }
        }

        Helper.safeClearDB(asyncClient);
        {
            // pk不一致
            TableMeta tableMeta = new TableMeta(metaTableName);
            tableMeta.addPrimaryKeyColumn("ha", PrimaryKeyType.STRING);
            tableMeta.addPrimaryKeyColumn(TableMetaGenerator.CN_PK1, PrimaryKeyType.STRING);
            tableMeta.addPrimaryKeyColumn(TableMetaGenerator.CN_PK2, PrimaryKeyType.STRING);

            TableOptions tableOptions = new TableOptions();
            CreateTableRequest request = new CreateTableRequest(
                    tableMeta, tableOptions);
            tableOptions.setMaxVersions(1);
            tableOptions.setTimeToLive(-1);
            Future<CreateTableResponse> res = asyncClient.createTable(request, null);
            Utils.waitForFuture(res);

            Thread.sleep(1000);

            try {
                db.metaTable();
                Assert.fail();
            } catch (ClientException e) {
                Assert.assertEquals(e.getMessage(), "Same table with different meta exist: " + metaTableName);
            }
        }
    }

    @Test
    public void testIllegalMetaIndex() throws ExecutionException, InterruptedException {
        /**
         * 同名表存在，但schema不一致，预期报错
         */
        String metaTableName = "metaTable";
        TimestreamDBConfiguration config = new TimestreamDBConfiguration(metaTableName);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        Helper.safeClearDB(asyncClient);

        TimestreamDB db = new TimestreamDBClient(asyncClient, config);

        {
            // 建表
            TableMeta tableMeta = TableMetaGenerator.getMetaTableMeta(metaTableName);
            TableOptions tableOptions = new TableOptions();
            CreateTableRequest request = new CreateTableRequest(
                    tableMeta, tableOptions);
            tableOptions.setMaxVersions(1);
            tableOptions.setTimeToLive(-1);
            Future<CreateTableResponse> res = asyncClient.createTable(request, null);
            Utils.waitForFuture(res);
        }

        Thread.sleep(1000);
        {
            // index为空
            try {
                db.metaTable();
                Assert.fail();
            } catch (ClientException e) {
                Assert.assertEquals(e.getMessage(), String.format("Index for meta(%s) not exist: %s", metaTableName, metaTableName + "_INDEX"));
            }
        }
        {
            // 有index，但没有对应的名字
            IndexSchema indexSchema = new IndexSchema();
            indexSchema.setFieldSchemas(Arrays.asList(
                    new FieldSchema(TableMetaGenerator.CN_PK0, FieldType.KEYWORD)));
            CreateSearchIndexRequest request = new CreateSearchIndexRequest(
                    metaTableName, // 设置表名
                    "index1"); // 设置索引名
            // set name as index routing key
            IndexSetting indexSetting = new IndexSetting();
            indexSetting.setRoutingFields(Arrays.asList(TableMetaGenerator.CN_PK1));
            indexSchema.setIndexSetting(indexSetting);
            request.setIndexSchema(indexSchema);

            Future<CreateSearchIndexResponse> future = asyncClient.createSearchIndex(request, null);
            Utils.waitForFuture(future);

            try {
                db.metaTable();
                Assert.fail();
            } catch (ClientException e) {
                Assert.assertEquals(e.getMessage(), String.format("Index for meta(%s) not exist: %s", metaTableName, metaTableName + "_INDEX"));
            }
        }
    }
}
