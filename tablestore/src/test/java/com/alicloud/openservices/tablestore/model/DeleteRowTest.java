package com.alicloud.openservices.tablestore.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.common.OTSHelper;
import com.alicloud.openservices.tablestore.common.Utils;
import com.alicloud.openservices.tablestore.core.ErrorCode;
import com.google.gson.JsonSyntaxException;

public class DeleteRowTest {
    private static final int MILLISECONDS_UNTIL_TABLE_READY = 10 * 1000;

    private static final String tableName = "DeleteRowTest";
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
     * Delete a non-existent row, expected row existence: IGNORE
     * @throws Exception
     */
    @Test
    public void testDeleteNonExistRowWithIGnoreForInternal() throws Exception {
    	Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("PK1", PrimaryKeyType.STRING);
        pks.put("PK2", PrimaryKeyType.INTEGER);
        
    	CreateTable(client, tableName, pks);
    	
        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("haha"))
                .addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(1234))
                .build();
        OTSHelper.deleteRow(client, tableName, pk, RowExistenceExpectation.IGNORE);
        
        SingleRowQueryCriteria query = new SingleRowQueryCriteria(tableName, pk);
        query.setMaxVersions(1);
        GetRowResponse response = OTSHelper.getRow(client, query);
        Row row = response.getRow();
        assertEquals(row, null);
    }
    
    /**
     * Delete a non-existent row, expected row existence: exists, throw an error
     * @throws Exception
     */
    @Test
    public void testDeleteNonExistRowWithExpectExistForInternal() throws Exception {
    	Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("PK1", PrimaryKeyType.STRING);
        pks.put("PK2", PrimaryKeyType.INTEGER);
        
    	CreateTable(client, tableName, pks);
    	
        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("haha"))
                .addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(1234))
                .build();
        try {
            OTSHelper.deleteRow(client, tableName, pk, RowExistenceExpectation.EXPECT_EXIST);
        	assertTrue(false);
        } catch (TableStoreException e) {
        	assertEquals(ErrorCode.CONDITION_CHECK_FAIL, e.getErrorCode());
        }
    }
    
    /**
     * Delete a non-existent row, expected row existence: non-existent
     * @throws Exception
     */
    @Test
    public void testDeleteExistRowWithExpectNonExistForInternal() throws Exception {
    	Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("PK1", PrimaryKeyType.STRING);
        pks.put("PK2", PrimaryKeyType.INTEGER);
        
    	CreateTable(client, tableName, pks);
    	
        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("haha"))
                .addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(1234))
                .build();
        OTSHelper.deleteRow(client, tableName, pk, RowExistenceExpectation.EXPECT_NOT_EXIST);
    }
    
    /**
     * Delete the existing row, expected row existence: IGNORE
     * @throws Exception
     */
    @Test
    public void testDeleteExistRowWithIGnoreForInternal() throws Exception {
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
        
        OTSHelper.deleteRow(client, tableName, pk, RowExistenceExpectation.IGNORE);
        
        SingleRowQueryCriteria query = new SingleRowQueryCriteria(tableName, pk);
        query.setMaxVersions(1);
        GetRowResponse response = OTSHelper.getRow(client, query);
        Row row = response.getRow();
        assertEquals(row, null);
    }
    
    /**
     * Delete an existing row, expected row existence: exists
     * @throws Exception
     */
    @Test
    public void testDeleteExistRowWithExpectExistForInternal() throws Exception {
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
        
        // Delete the same row
        OTSHelper.deleteRow(client, tableName, pk, RowExistenceExpectation.EXPECT_EXIST);
        
        SingleRowQueryCriteria query = new SingleRowQueryCriteria(tableName, pk);
        query.setMaxVersions(1);
        GetRowResponse response = OTSHelper.getRow(client, query);
        Row row = response.getRow();
        assertEquals(row, null);
    }
    
    /**
     * Delete the existing row, expected row existence: non-existent, throw an error
     * @throws Exception
     */
    @Test
    public void testDeleteNonExistRowWithExpectNonExistForInternal() throws Exception {
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
        
        try {
            OTSHelper.deleteRow(client, tableName, pk, RowExistenceExpectation.EXPECT_NOT_EXIST);
        } catch (TableStoreException e) {
        	assertEquals(ErrorCode.CONDITION_CHECK_FAIL, e.getErrorCode());
        }
    }

}
