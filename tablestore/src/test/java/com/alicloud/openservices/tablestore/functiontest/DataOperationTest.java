package com.alicloud.openservices.tablestore.functiontest;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.core.ErrorCode;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.BatchGetRowResponse.RowResult;
import com.alicloud.openservices.tablestore.core.utils.Bytes;
import com.alicloud.openservices.tablestore.core.utils.Pair;
import com.alicloud.openservices.tablestore.common.BaseFT;
import com.alicloud.openservices.tablestore.common.OTSHelper;
import com.alicloud.openservices.tablestore.common.Utils;
import com.google.gson.JsonSyntaxException;

public class DataOperationTest extends BaseFT{

    private static String tableName = "TableOptionsFunctiontest";
    
    private static SyncClientInterface ots;
    
    private static final Logger LOG = LoggerFactory.getLogger(DataOperationTest.class);
    
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
        // Clean up the environment
        OTSHelper.deleteAllTable(ots);
    }
    
    @After
    public void teardown() {
        
    }
    
    /**
     * Create a table with PK 'PK1', type STRING/BINARY/INTEGER, and test all row operation APIs. 
     * PK 'PK2' has the same type as 'PK1' when creating the table, expecting to return OTSParameterInvalid.
     * @throws Exception
     */
    @Test
    public void testCase1() throws Exception {
        List<List<PrimaryKeySchema>> input = new ArrayList<List<PrimaryKeySchema>>();
        {
            List<PrimaryKeySchema> pk = new ArrayList<PrimaryKeySchema>();
            pk.add(new PrimaryKeySchema("PK1", PrimaryKeyType.STRING));
            input.add(pk);
        }
        {
            List<PrimaryKeySchema> pk = new ArrayList<PrimaryKeySchema>();
            pk.add(new PrimaryKeySchema("PK1", PrimaryKeyType.INTEGER));
            input.add(pk);
        }
        {
            List<PrimaryKeySchema> pk = new ArrayList<PrimaryKeySchema>();
            pk.add(new PrimaryKeySchema("PK1", PrimaryKeyType.BINARY));
            input.add(pk);
        }
        int index = 0;
        for (List<PrimaryKeySchema> scheme : input) {
            LOG.info("Index : {}", index++);
            OTSHelper.createTable(ots, tableName, scheme);
            
            Utils.waitForPartitionLoad(tableName);
            
            List<PrimaryKeyColumn> pk = new ArrayList<PrimaryKeyColumn>();
            pk.add(new PrimaryKeyColumn("PK2", Utils.getPKColumnValue(scheme.get(0).getType(), "1000")));
            
            List<Column> columns = new ArrayList<Column>();
            columns.add(new Column("name", ColumnValue.fromString("value")));

            try {
                OTSHelper.putRow(ots, tableName, new PrimaryKey(pk), columns);
                assertTrue(false);
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PK, "Validate PK name fail. Input: PK2, Meta: PK1.", 400, e);
            }
            
            try {
                OTSHelper.getRowForAll(ots, tableName, new PrimaryKey(pk));
                assertTrue(false);
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PK, "Validate PK name fail. Input: PK2, Meta: PK1.", 400, e);
            }
            
            try {
                OTSHelper.updateRow(ots, tableName, new PrimaryKey(pk), columns, new ArrayList<String>(), new ArrayList<Pair<String, Long>>());
                assertTrue(false);
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PK, "Validate PK name fail. Input: PK2, Meta: PK1.", 400, e);
            }
            
            try {
                OTSHelper.deleteRow(ots, tableName, new PrimaryKey(pk));
                assertTrue(false);
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PK, "Validate PK name fail. Input: PK2, Meta: PK1.", 400, e);
            }
            
            {
                List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
                {
                    MultiRowQueryCriteria c = new MultiRowQueryCriteria(tableName);
                    List<PrimaryKey> primaryKeys = new ArrayList<PrimaryKey>();
                    primaryKeys.add(new PrimaryKey(pk));
                    c.setRowKeys(primaryKeys);
                    c.setMaxVersions(Integer.MAX_VALUE);
                    criterias.add(c);
                }
                BatchGetRowResponse result = OTSHelper.batchGetRow(ots, criterias);
                List<RowResult> rrs = result.getFailedRows();
                assertEquals(1, rrs.size());
                assertEquals(ErrorCode.INVALID_PK, rrs.get(0).getError().getCode());
                assertEquals("Validate PK name fail. Input: PK2, Meta: PK1.", rrs.get(0).getError().getMessage());
            }
            
            {
                List<RowPutChange> puts = new ArrayList<RowPutChange>();
                {
                    RowPutChange change = new RowPutChange(tableName, new PrimaryKey(pk));
                    change.addColumn("attr_0", ColumnValue.fromString("bigbang"));
                    puts.add(change);
                }
                List<RowUpdateChange> updates = new ArrayList<RowUpdateChange>();
                List<RowDeleteChange> deletes = new ArrayList<RowDeleteChange>();
                BatchWriteRowResponse result = OTSHelper.batchWriteRow(ots, puts, updates, deletes);
                List<BatchWriteRowResponse.RowResult> rrs = result.getFailedRows();

                assertEquals(1, rrs.size());
                assertEquals(ErrorCode.INVALID_PK, rrs.get(0).getError().getCode());
                assertEquals("Validate PK name fail. Input: PK2, Meta: PK1.", rrs.get(0).getError().getMessage());
            }
            OTSHelper.deleteTable(ots, tableName);
        }
    }

    /**
     * Create a table with PK 'PK1' of type STRING/BINARY/INTEGER, and test all row operation APIs. 
     * Use a different type for 'PK1' than the one specified during table creation, expecting to return OTSParameterInvalid.
     * @throws Exception
     */
    @Test
    public void testCase2() throws Exception {
        List<List<PrimaryKeySchema>> input = new ArrayList<List<PrimaryKeySchema>>();
        {
            List<PrimaryKeySchema> pk = new ArrayList<PrimaryKeySchema>();
            pk.add(new PrimaryKeySchema("PK1", PrimaryKeyType.STRING));
            input.add(pk);
        }
        {
            List<PrimaryKeySchema> pk = new ArrayList<PrimaryKeySchema>();
            pk.add(new PrimaryKeySchema("PK1", PrimaryKeyType.INTEGER));
            input.add(pk);
        }
        {
            List<PrimaryKeySchema> pk = new ArrayList<PrimaryKeySchema>();
            pk.add(new PrimaryKeySchema("PK1", PrimaryKeyType.BINARY));
            input.add(pk);
        }
        int index = 0;
        for (List<PrimaryKeySchema> scheme : input) {
            LOG.info("Index : {}", index++);
            OTSHelper.createTable(ots, tableName, scheme);

            Utils.waitForPartitionLoad(tableName);

	    String errorMsg;
            List<PrimaryKeyColumn> pk = new ArrayList<PrimaryKeyColumn>();
            PrimaryKeyType type = scheme.get(0).getType();
            if (type == PrimaryKeyType.STRING) {
                pk.add(new PrimaryKeyColumn("PK1", Utils.getPKColumnValue(PrimaryKeyType.INTEGER, "1000")));
		errorMsg = "Validate PK type fail. Input: VT_INTEGER, Meta: VT_STRING.";
            } else if (type == PrimaryKeyType.INTEGER) {
                pk.add(new PrimaryKeyColumn("PK1", Utils.getPKColumnValue(PrimaryKeyType.STRING, "1000")));
		errorMsg = "Validate PK type fail. Input: VT_STRING, Meta: VT_INTEGER.";
            } else {
                pk.add(new PrimaryKeyColumn("PK1", Utils.getPKColumnValue(PrimaryKeyType.INTEGER, "1000")));
		errorMsg = "Validate PK type fail. Input: VT_INTEGER, Meta: VT_BLOB.";
            }

            List<Column> columns = new ArrayList<Column>();
            columns.add(new Column("name", ColumnValue.fromString("value")));

            try {
                OTSHelper.putRow(ots, tableName, new PrimaryKey(pk), columns);
                assertTrue(false);
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PK, errorMsg, 400, e);
            }

            try {
                OTSHelper.getRowForAll(ots, tableName, new PrimaryKey(pk));
                assertTrue(false);
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PK, errorMsg, 400, e);
            }

            try {
                OTSHelper.updateRow(ots, tableName, new PrimaryKey(pk), columns, new ArrayList<String>(), new ArrayList<Pair<String, Long>>());
                assertTrue(false);
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PK, errorMsg, 400, e);
            }

            try {
                OTSHelper.deleteRow(ots, tableName, new PrimaryKey(pk));
                assertTrue(false);
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PK, errorMsg, 400, e);
            }

            {
                List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
                {
                    MultiRowQueryCriteria c = new MultiRowQueryCriteria(tableName);
                    List<PrimaryKey> primaryKeys = new ArrayList<PrimaryKey>();
                    primaryKeys.add(new PrimaryKey(pk));
                    c.setRowKeys(primaryKeys);
                    c.setMaxVersions(Integer.MAX_VALUE);
                    criterias.add(c);
                }
                BatchGetRowResponse result = OTSHelper.batchGetRow(ots, criterias);
                List<RowResult> rrs = result.getFailedRows();
                assertEquals(1, rrs.size());
                assertEquals(ErrorCode.INVALID_PK, rrs.get(0).getError().getCode());
                assertEquals(errorMsg, rrs.get(0).getError().getMessage());
            }

            {
                List<RowPutChange> puts = new ArrayList<RowPutChange>();
                {
                    RowPutChange change = new RowPutChange(tableName, new PrimaryKey(pk));
                    change.addColumn("attr_0", ColumnValue.fromString("bigbang"));
                    puts.add(change);
                }
                List<RowUpdateChange> updates = new ArrayList<RowUpdateChange>();
                List<RowDeleteChange> deletes = new ArrayList<RowDeleteChange>();
                BatchWriteRowResponse result = OTSHelper.batchWriteRow(ots, puts, updates, deletes);
                List<BatchWriteRowResponse.RowResult> rrs = result.getFailedRows();

                assertEquals(1, rrs.size());
                assertEquals(ErrorCode.INVALID_PK, rrs.get(0).getError().getCode());
                assertEquals(errorMsg, rrs.get(0).getError().getMessage());
            }

            OTSHelper.deleteTable(ots, tableName);
        }
    }

    /**
     * Create a table with PK as [('PK1', 'STRING'), ('PK2', 'INTEGER'), ('PK3', 'BINARY')],
     * Test all row operation APIs.
     * PK is {'PK1' : 'blah'}.
     * PK is {'PK1' : 'blah', 'PK2' : 123, 'PK4' : 'blah'}.
     * PK is {'PK1' : 'blah', 'PK2' : 123, 'PK3' : bytearray(3), 'pk4' : 'blah'},
     * Expect to return OTSParameterInvalid.
     * @throws Exception
     */
    @Test
    public void testCase3() throws Exception {
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("PK1", PrimaryKeyType.STRING));
        scheme.add(new PrimaryKeySchema("PK2", PrimaryKeyType.INTEGER));
        scheme.add(new PrimaryKeySchema("PK3", PrimaryKeyType.BINARY));

        OTSHelper.createTable(ots, tableName, scheme);

        Utils.waitForPartitionLoad(tableName);

        List<PrimaryKey> pks = new ArrayList<PrimaryKey>();
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("blah"))
                    .build();
            pks.add(pk);
        }
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("blah"))
                    .addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(123))
                    .build();
            pks.add(pk);
        }
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("blah"))
                    .addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(123))
                    .addPrimaryKeyColumn("PK3", PrimaryKeyValue.fromBinary(Bytes.toBytes(3)))
                    .addPrimaryKeyColumn("PK4", PrimaryKeyValue.fromString("blah"))
                    .build();
            pks.add(pk);
        }

        List<Column> columns = new ArrayList<Column>();
        columns.add(new Column("attr", ColumnValue.fromString("hello world")));

        for (PrimaryKey pk : pks) {
            LOG.info("PK : {}", pk.toString());
	    String errorMsg = "Validate PK size fail. Input: " + pk.size() + ", Meta: 3.";

            try {
                OTSHelper.putRow(ots, tableName, pk, columns);
                assertTrue(false);
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PK, errorMsg, 400, e);
            }

            try {
                OTSHelper.getRowForAll(ots, tableName, pk);
                assertTrue(false);
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PK, errorMsg, 400, e);
            }

            try {
                OTSHelper.updateRow(ots, tableName, pk, columns, new ArrayList<String>(), new ArrayList<Pair<String, Long>>());
                assertTrue(false);
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PK, errorMsg, 400, e);
            }

            try {
                OTSHelper.deleteRow(ots, tableName, pk);
                assertTrue(false);
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PK, errorMsg, 400, e);
            }

            {
                List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
                {
                    MultiRowQueryCriteria c = new MultiRowQueryCriteria(tableName);
                    List<PrimaryKey> primaryKeys = new ArrayList<PrimaryKey>();
                    primaryKeys.add(pk);
                    c.setRowKeys(primaryKeys);
                    c.setMaxVersions(Integer.MAX_VALUE);
                    criterias.add(c);
                }
                BatchGetRowResponse result = OTSHelper.batchGetRow(ots, criterias);
                List<RowResult> rrs = result.getFailedRows();
                assertEquals(1, rrs.size());
                assertEquals(ErrorCode.INVALID_PK, rrs.get(0).getError().getCode());
                assertEquals(errorMsg, rrs.get(0).getError().getMessage());
            }

            {
                List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
                {
                    MultiRowQueryCriteria c = new MultiRowQueryCriteria(tableName);
                    List<PrimaryKey> primaryKeys = new ArrayList<PrimaryKey>();
                    primaryKeys.add(pk);
                    c.setRowKeys(primaryKeys);
                    c.setMaxVersions(Integer.MAX_VALUE);
                    criterias.add(c);
                }
                BatchGetRowResponse result = OTSHelper.batchGetRow(ots, criterias);
                List<RowResult> rrs = result.getFailedRows();
                assertEquals(1, rrs.size());
                assertEquals(ErrorCode.INVALID_PK, rrs.get(0).getError().getCode());
                assertEquals(errorMsg, rrs.get(0).getError().getMessage());
            }
        }
    }

    /**
     * Create a table with PK being [('PK1', 'STRING'), ('PK2', 'INTEGER')],
     * Test all row operation APIs.
     * PK is {'PK2': 123, 'PK1', 'blah'},
     * Expect to return OTSParameterInvalid.
     */
    @Test
    public void testCase4() {
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("PK1", PrimaryKeyType.STRING));
        scheme.add(new PrimaryKeySchema("PK2", PrimaryKeyType.INTEGER));

        OTSHelper.createTable(ots, tableName, scheme);
        Utils.waitForPartitionLoad(tableName);

        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(123))
                .addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("blah"))
                .build();

        List<Column> columns = new ArrayList<Column>();
        columns.add(new Column("attr", ColumnValue.fromString("hello world")));

        try {
            OTSHelper.putRow(ots, tableName, pk, columns);
            assertTrue(false);
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PK, "Validate PK name fail. Input: PK2, Meta: PK1.", 400, e);
        }

        try {
            OTSHelper.getRowForAll(ots, tableName, pk);
            assertTrue(false);
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PK, "Validate PK name fail. Input: PK2, Meta: PK1.", 400, e);
        }

        try {
            OTSHelper.updateRow(ots, tableName, pk, columns, new ArrayList<String>(), new ArrayList<Pair<String, Long>>());
            assertTrue(false);
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PK, "Validate PK name fail. Input: PK2, Meta: PK1.", 400, e);
        }

        try {
            OTSHelper.deleteRow(ots, tableName, pk);
            assertTrue(false);
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PK, "Validate PK name fail. Input: PK2, Meta: PK1.", 400, e);
        }

        {
            List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
            {
                MultiRowQueryCriteria c = new MultiRowQueryCriteria(tableName);
                List<PrimaryKey> primaryKeys = new ArrayList<PrimaryKey>();
                primaryKeys.add(pk);
                c.setRowKeys(primaryKeys);
                c.setMaxVersions(Integer.MAX_VALUE);
                criterias.add(c);
            }
            BatchGetRowResponse result = OTSHelper.batchGetRow(ots, criterias);
            List<RowResult> rrs = result.getFailedRows();
            assertEquals(1, rrs.size());
            assertEquals(ErrorCode.INVALID_PK, rrs.get(0).getError().getCode());
            assertEquals("Validate PK name fail. Input: PK2, Meta: PK1.", rrs.get(0).getError().getMessage());
        }

        {
            List<RowPutChange> puts = new ArrayList<RowPutChange>();
            {
                RowPutChange change = new RowPutChange(tableName, pk);
                change.addColumn("attr_0", ColumnValue.fromString("bigbang"));
                puts.add(change);
            }
            List<RowUpdateChange> updates = new ArrayList<RowUpdateChange>();
            List<RowDeleteChange> deletes = new ArrayList<RowDeleteChange>();
            BatchWriteRowResponse result = OTSHelper.batchWriteRow(ots, puts, updates, deletes);
            List<BatchWriteRowResponse.RowResult> rrs = result.getFailedRows();
            
            assertEquals(1, rrs.size());
            assertEquals(ErrorCode.INVALID_PK, rrs.get(0).getError().getCode());
            assertEquals("Validate PK name fail. Input: PK2, Meta: PK1.", rrs.get(0).getError().getMessage());
        } 
    }
}
