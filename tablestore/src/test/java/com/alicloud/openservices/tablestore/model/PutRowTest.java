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
        OTSHelper.deleteAllTable(client);
    }   

    private void CreateTable(SyncClientInterface ots, String tableName, Map<String, PrimaryKeyType> pk) throws Exception {
        OTSHelper.createTable(ots, tableName, pk);
        Thread.sleep(MILLISECONDS_UNTIL_TABLE_READY);
    }
    
    /**
     * The primary keys of rows written internally are unordered.
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
        	assertTableStoreException(ErrorCode.INVALID_PK, "Validate PK name fail. Input: PK2, Meta: PK1.", 400, e);
        }
    }
    
    /**
     * Insert a non-existent row, expected row existence: IGNORE
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
     * Insert a non-existent row. Expected row existence: exists, throw an error.
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
        	assertEquals(ErrorCode.CONDITION_CHECK_FAIL, e.getErrorCode());
        }
    }
    
    /**
     * Insert a non-existent row, expected row existence: does not exist
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
     * Insert the existing row, expected row existence: IGNORE
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
        
        // Write to the same row
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
     * Insert an existing row, expected row existence: exists
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
        
        // Write to the same row
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
     * Insert an existing row, expected row existence: does not exist, throw an error
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
        
        // Write the same row
        try {
            columns.put("attr1", ColumnValue.fromString("hello world"));
            OTSHelper.putRow(client, tableName, pk, columns, RowExistenceExpectation.EXPECT_NOT_EXIST);
        } catch (TableStoreException e) {
        	assertEquals(ErrorCode.CONDITION_CHECK_FAIL, e.getErrorCode());
        }
    }
}
