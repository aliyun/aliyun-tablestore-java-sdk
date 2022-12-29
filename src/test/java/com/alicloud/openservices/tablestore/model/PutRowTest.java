package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.common.BaseFT;
import com.alicloud.openservices.tablestore.common.OTSHelper;
import com.alicloud.openservices.tablestore.common.Utils;
import com.alicloud.openservices.tablestore.core.ErrorCode;
import com.google.gson.JsonSyntaxException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class PutRowTest extends BaseFT {
    private static final int MILLISECONDS_UNTIL_TABLE_READY = 10 * 1000;

    private static final String tableName = "PutRowTest";
    private static SyncClientInterface client;
    private static Logger LOG = Logger.getLogger(BatchWriteTest.class.getName());
    
    @BeforeClass
    public static void classBefore() throws JsonSyntaxException, IOException {
        client = Utils.getOTSInstance();
    }
    
    @AfterClass
    public static void classAfter() {
    	client.shutdown();
    }

    @Before
    public void setup() throws Exception {
        ListTableResponse r = client.listTable();
        for (String table: r.getTableNames()) {
            DeleteTableRequest deleteTableRequest = new DeleteTableRequest(table);
            client.deleteTable(deleteTableRequest);
            LOG.info("Delete table: " + table);

            Thread.sleep(1000);
        }
    }   

    private void CreateTable(SyncClientInterface ots, String tableName, Map<String, PrimaryKeyType> pk) throws Exception {
        OTSHelper.createTable(ots, tableName, pk);
        LOG.info("Create table: " + tableName);
        Thread.sleep(MILLISECONDS_UNTIL_TABLE_READY);
    }
    
    /**
     * internal写入的行pk无序
     * @throws Exception
     */
    @Test
    public void testWriteDisorderedPKsForInternal() throws Exception {
    	Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("PK1", PrimaryKeyType.STRING);
        pks.put("PK2", PrimaryKeyType.INTEGER);
        
    	CreateTable(client, tableName, pks);
    	
        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(1234))
                .addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("haha"))
                .build();
        
        Map<String, ColumnValue> columns = new TreeMap<String, ColumnValue>();
        columns.put("attr", ColumnValue.fromString("hello world"));
        try {
            OTSHelper.putRow(client, tableName, pk, columns);
        	assertTrue(false);
        } catch (TableStoreException e) {
        	LOG.info(e.toString());
        	assertTableStoreException(ErrorCode.INVALID_PK, "Validate PK name fail. Input: PK2, Meta: PK1.", 400, e);
        }
    }
    
    /**
     * 插入不存在的行，期望行存在性：IGNORE
     * @throws Exception
     */
    @Test
    public void testPutNonExistRowWithIGnoreForInternal() throws Exception {
    	Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("PK1", PrimaryKeyType.STRING);
        pks.put("PK2", PrimaryKeyType.INTEGER);
        
    	CreateTable(client, tableName, pks);
    	
        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("haha"))
                .addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(1234))
                .build();
        Map<String, ColumnValue> columns = new TreeMap<String, ColumnValue>();
        columns.put("attr", ColumnValue.fromString("hello world"));
        OTSHelper.putRow(client, tableName, pk, columns, RowExistenceExpectation.IGNORE);
        
        SingleRowQueryCriteria query = new SingleRowQueryCriteria(tableName, pk);
        query.setMaxVersions(1);
        GetRowResponse response = OTSHelper.getRow(client, query);
        Row row = response.getRow();
        assertEquals(row.getColumns().length, 1);
        assertEquals(row.getColumn("attr").get(0).getValue().asString(), "hello world");
    }
    
    /**
     * 插入不存在的行，期望行存在性：存在，抛错
     * @throws Exception
     */
    @Test
    public void testPutNonExistRowWithExpectExistForInternal() throws Exception {
    	Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("PK1", PrimaryKeyType.STRING);
        pks.put("PK2", PrimaryKeyType.INTEGER);
        
    	CreateTable(client, tableName, pks);
    	
        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("haha"))
                .addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(1234))
                .build();
        Map<String, ColumnValue> columns = new TreeMap<String, ColumnValue>();
        columns.put("attr", ColumnValue.fromString("hello world"));
        try {
            OTSHelper.putRow(client, tableName, pk, columns, RowExistenceExpectation.EXPECT_EXIST);
        	assertTrue(false);
        } catch (TableStoreException e) {
        	LOG.info(e.toString());
        	assertEquals(ErrorCode.CONDITION_CHECK_FAIL, e.getErrorCode());
        }
    }
    
    /**
     * 插入不存在的行，期望行存在性：不存在
     * @throws Exception
     */
    @Test
    public void testPutExistRowWithExpectNonExistForInternal() throws Exception {
    	Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("PK1", PrimaryKeyType.STRING);
        pks.put("PK2", PrimaryKeyType.INTEGER);
        
    	CreateTable(client, tableName, pks);
    	
        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("haha"))
                .addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(1234))
                .build();
        Map<String, ColumnValue> columns = new TreeMap<String, ColumnValue>();
        columns.put("attr", ColumnValue.fromString("hello world"));
        OTSHelper.putRow(client, tableName, pk, columns, RowExistenceExpectation.EXPECT_NOT_EXIST);
        
        SingleRowQueryCriteria query = new SingleRowQueryCriteria(tableName, pk);
        query.setMaxVersions(1);
        GetRowResponse response = OTSHelper.getRow(client, query);
        Row row = response.getRow();
        assertEquals(row.getColumns().length, 1);
        assertEquals(row.getColumn("attr").get(0).getValue().asString(), "hello world");
    }
    
    /**
     * 插入存在的行，期望行存在性：IGNORE
     * @throws Exception
     */
    @Test
    public void testPutExistRowWithIGnoreForInternal() throws Exception {
    	Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("PK1", PrimaryKeyType.STRING);
        pks.put("PK2", PrimaryKeyType.INTEGER);
        
    	CreateTable(client, tableName, pks);
    	
        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("haha"))
                .addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(1234))
                .build();
        Map<String, ColumnValue> columns = new TreeMap<String, ColumnValue>();
        columns.put("attr", ColumnValue.fromString("hello world"));
        OTSHelper.putRow(client, tableName, pk, columns, RowExistenceExpectation.IGNORE);
        
        // 写入相同行
        columns.put("attr1", ColumnValue.fromString("hello world"));
        OTSHelper.putRow(client, tableName, pk, columns, RowExistenceExpectation.IGNORE);
        
        SingleRowQueryCriteria query = new SingleRowQueryCriteria(tableName, pk);
        query.setMaxVersions(1);
        GetRowResponse response = OTSHelper.getRow(client, query);
        Row row = response.getRow();
        assertEquals(row.getColumns().length, 2);
        assertEquals(row.getColumn("attr").get(0).getValue().asString(), "hello world");
        assertEquals(row.getColumn("attr1").get(0).getValue().asString(), "hello world");
    }
    
    /**
     * 插入存在的行，期望行存在性：存在
     * @throws Exception
     */
    @Test
    public void testPutExistRowWithExpectExistForInternal() throws Exception {
    	Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("PK1", PrimaryKeyType.STRING);
        pks.put("PK2", PrimaryKeyType.INTEGER);
        
    	CreateTable(client, tableName, pks);
    	
        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("haha"))
                .addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(1234))
                .build();
        Map<String, ColumnValue> columns = new TreeMap<String, ColumnValue>();
        columns.put("attr", ColumnValue.fromString("hello world"));
        OTSHelper.putRow(client, tableName, pk, columns, RowExistenceExpectation.IGNORE);
        
        // 写入相同行
        columns.put("attr1", ColumnValue.fromString("hello world"));
        OTSHelper.putRow(client, tableName, pk, columns, RowExistenceExpectation.EXPECT_EXIST);
        
        SingleRowQueryCriteria query = new SingleRowQueryCriteria(tableName, pk);
        query.setMaxVersions(1);
        GetRowResponse response = OTSHelper.getRow(client, query);
        Row row = response.getRow();
        assertEquals(row.getColumns().length, 2);
        assertEquals(row.getColumn("attr").get(0).getValue().asString(), "hello world");
        assertEquals(row.getColumn("attr1").get(0).getValue().asString(), "hello world");
    }
    
    /**
     * 插入存在的行，期望行存在性：不存在，抛错
     * @throws Exception
     */
    @Test
    public void testPutNonExistRowWithExpectNonExistForInternal() throws Exception {
    	Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("PK1", PrimaryKeyType.STRING);
        pks.put("PK2", PrimaryKeyType.INTEGER);
        
    	CreateTable(client, tableName, pks);
    	
        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("haha"))
                .addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(1234))
                .build();
        Map<String, ColumnValue> columns = new TreeMap<String, ColumnValue>();
        columns.put("attr", ColumnValue.fromString("hello world"));
        OTSHelper.putRow(client, tableName, pk, columns, RowExistenceExpectation.IGNORE);
        
        // 写入相同行
        try {
            columns.put("attr1", ColumnValue.fromString("hello world"));
            OTSHelper.putRow(client, tableName, pk, columns, RowExistenceExpectation.EXPECT_NOT_EXIST);
        } catch (TableStoreException e) {
        	LOG.info(e.toString());
        	assertEquals(ErrorCode.CONDITION_CHECK_FAIL, e.getErrorCode());
        }
    }
}
