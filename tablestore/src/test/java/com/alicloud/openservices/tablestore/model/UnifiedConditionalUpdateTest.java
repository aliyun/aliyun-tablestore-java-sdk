/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 */

package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.common.OTSHelper;
import com.alicloud.openservices.tablestore.common.Utils;
import com.alicloud.openservices.tablestore.model.condition.ColumnCondition;
import com.alicloud.openservices.tablestore.model.condition.CompositeColumnValueCondition;
import com.alicloud.openservices.tablestore.model.condition.SingleColumnValueCondition;
import com.google.gson.JsonSyntaxException;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import static org.junit.Assert.*;


public class UnifiedConditionalUpdateTest {
    private static final int MILLISECONDS_UNTIL_TABLE_READY = 10 * 1000;
    
    private static String tableName = "conditional_update_test_table";

    private static SyncClientInterface ots;
    private static Logger LOG = Logger.getLogger(UnifiedConditionalUpdateTest.class.getName());

    @BeforeClass
    public static void classBefore() throws JsonSyntaxException, IOException {
        ots = Utils.getOTSInstance();
    }

    @Before
    public void setup() throws Exception {
        OTSHelper.deleteAllTable(ots);
    }

    @Test
    public void testSingleFilter() throws Exception {
        CreateTable();

        // put a row first
        boolean success = putRow(19, "Col1", ColumnValue.fromString("Value1"), null);
        assertTrue(success);

        // put row with condition: col1 != value1
        success = putRow(19, "Col2", ColumnValue.fromString("Value2"),
                new SingleColumnValueCondition("Col1",
                        SingleColumnValueCondition.CompareOperator.NOT_EQUAL,
                        ColumnValue.fromString("Value1")));
        assertTrue(!success);

        // put row with condition: col1 == value1
        success = putRow(19, "Col2", ColumnValue.fromString("Value2"),
                new SingleColumnValueCondition("Col1",
                        SingleColumnValueCondition.CompareOperator.EQUAL,
                        ColumnValue.fromString("Value1")));
        assertTrue(success);

        // update row with condition: col2 < value1
        success = putRow(19, "Col3", ColumnValue.fromString("Value3"),
                new SingleColumnValueCondition("Col2",
                        SingleColumnValueCondition.CompareOperator.LESS_THAN,
                        ColumnValue.fromString("Value1")));
        assertTrue(!success);

        // update row with condition: col2 >= value2
        success = putRow(19, "Col3", ColumnValue.fromString("Value3"),
                new SingleColumnValueCondition("Col2",
                        SingleColumnValueCondition.CompareOperator.GREATER_EQUAL,
                        ColumnValue.fromString("Value2")));
        assertTrue(success);

        // delete row with condition: col3 <= value2
        success = deleteRow(19, "Col3", ColumnValue.fromString("Value3"),
                new SingleColumnValueCondition("Col3",
                        SingleColumnValueCondition.CompareOperator.LESS_EQUAL,
                        ColumnValue.fromString("Value2")));
        assertTrue(!success);

        // delete row with condition: col3 > value2
        success = deleteRow(19, "Col3", ColumnValue.fromString("Value3"),
                new SingleColumnValueCondition("Col3",
                        SingleColumnValueCondition.CompareOperator.GREATER_THAN,
                        ColumnValue.fromString("Value2")));
        assertTrue(success);
    }

    @Test
    public void testColumnMissing() throws Exception {
        CreateTable();

        // put a row first
        boolean success = putRow(19, "Col1", ColumnValue.fromString("Value1"), null);
        assertTrue(success);

        // put row with condition: colX != valueY
        // with passIfMissing == true, this should succeed
        success = putRow(19, "Col2", ColumnValue.fromString("Value2"),
                new SingleColumnValueCondition("ColX",
                        SingleColumnValueCondition.CompareOperator.NOT_EQUAL,
                        ColumnValue.fromString("ValueY")));
        assertTrue(success);

        // put row with condition: colX != valueY
        // with passIfMissing == false, this should fail
        SingleColumnValueCondition cond = new SingleColumnValueCondition("ColX",
                SingleColumnValueCondition.CompareOperator.NOT_EQUAL,
                ColumnValue.fromString("ValueY"));
        cond.setPassIfMissing(false);
        success = putRow(19, "Col2", ColumnValue.fromString("Value2"), cond);
        assertTrue(!success);
    }

    @Test
    public void testCompositeFilter() throws Exception {
        CreateTable();

        // put a row first
        boolean success = putRow(19, "Col1", ColumnValue.fromString("Value1"), null);
        assertTrue(success);
        success = updateRow(19, "Col2", ColumnValue.fromString("Value2"), null);
        assertTrue(success);

        // update with condition:
        // col1 == value2 OR col2 == value1
        CompositeColumnValueCondition cond = new CompositeColumnValueCondition(CompositeColumnValueCondition.LogicOperator.OR);
        cond.addCondition(new SingleColumnValueCondition(
                "Col1",
                SingleColumnValueCondition.CompareOperator.EQUAL,
                ColumnValue.fromString("Value2")))
        .addCondition(new SingleColumnValueCondition(
                "Col2",
                SingleColumnValueCondition.CompareOperator.EQUAL,
                ColumnValue.fromString("Value1")));
        success = updateRow(19, "Col3", ColumnValue.fromString("Value3"), cond);
        assertTrue(!success);

        // update with condition:
        // Not col1 == value2
        cond = new CompositeColumnValueCondition(CompositeColumnValueCondition.LogicOperator.NOT);
        cond.addCondition(new SingleColumnValueCondition(
                "Col1",
                SingleColumnValueCondition.CompareOperator.EQUAL,
                ColumnValue.fromString("Value2")));
        success = updateRow(19, "Col3", ColumnValue.fromString("Value3"), cond);
        assertTrue(success);

        // delete with condition:
        // col1 == valueX OR (col2 == value2 AND col3 == value3)
        cond = new CompositeColumnValueCondition(CompositeColumnValueCondition.LogicOperator.OR);
        cond.addCondition(new SingleColumnValueCondition(
                "Col1",
                SingleColumnValueCondition.CompareOperator.EQUAL,
                ColumnValue.fromString("ValueX")));
        CompositeColumnValueCondition cond2 = new CompositeColumnValueCondition(CompositeColumnValueCondition.LogicOperator.AND);
        cond2.addCondition(new SingleColumnValueCondition(
                        "Col2",
                        SingleColumnValueCondition.CompareOperator.EQUAL,
                        ColumnValue.fromString("Value2")))
                .addCondition(new SingleColumnValueCondition(
                        "Col3",
                        SingleColumnValueCondition.CompareOperator.EQUAL,
                        ColumnValue.fromString("Value3")));
        cond.addCondition(cond2);

        success = deleteRow(19, "Col3", ColumnValue.fromString("Value3"), cond);
        assertTrue(success);
    }

    // Txn:
    // oldV = read
    // newV = oldV + 1
    // update v = newV if v == oldV
    public class DoTxn implements Runnable {
        private volatile int count = 0;
        private int round = 100;

        @Override
        public void run() {
            for (int i = 0; i < round; ++i) {
                Row r = readRow(tableName, 23);
                ColumnValue oldValue = r.getColumn("Col1").get(0).getValue();

                ColumnValue newValue = ColumnValue.fromLong(oldValue.asLong() + 1);
                SingleColumnValueCondition cond = new SingleColumnValueCondition(
                        "Col1", SingleColumnValueCondition.CompareOperator.EQUAL, oldValue);
                boolean success = updateRow(23, "Col1", newValue, cond);
                if (success) {
                    ++count;
                }
            }
        }

        public int getValue() {
            return count;
        }
    }

    @Test
    public void testTransactionalUpdate() throws Exception {
        CreateTable();

        boolean success = putRow(23, "Col1", ColumnValue.fromLong(0), null);
        assertTrue(success);

        int threadNum = 100;
        List<DoTxn> runnables = new ArrayList<DoTxn>();
        List<Thread> threads = new ArrayList<Thread>();

        for (int i = 0; i < threadNum; ++i) {
            runnables.add(new DoTxn());
            threads.add(new Thread(runnables.get(i)));
            threads.get(i).start();
        }

        for (Thread t: threads) {
            t.join();
        }

        int total = 0;
        for (DoTxn t: runnables) {
            total += t.getValue();
        }

        ColumnValue v = readRow(tableName, 23).getColumn("Col1").get(0).getValue();
        assertTrue(total > 0);
        assertEquals(total, v.asLong());
    }

    @Test
    public void testLimits() throws Exception {
        CreateTable();

        // column condition count <= 32
        CompositeColumnValueCondition cond = new CompositeColumnValueCondition(CompositeColumnValueCondition.LogicOperator.OR);
        for (int i = 0; i < 31; i++) {
            cond.addCondition(new SingleColumnValueCondition(
                    "ColX" + i, SingleColumnValueCondition.CompareOperator.EQUAL,
                    ColumnValue.fromString("ValueX")
            ));
        }

        boolean success = putRow(23, "Col1", ColumnValue.fromString("Value1"), cond);
        assertTrue(success);

        cond.addCondition(new SingleColumnValueCondition(
                "ColX10", SingleColumnValueCondition.CompareOperator.EQUAL,
                ColumnValue.fromString("ValueX")));
        success = putRow(23, "Col1", ColumnValue.fromString("Value1"), cond);
        assertTrue(!success);

        // invalid column value in column condition
        String invalidValue = "";
        for (int i = 0; i < 64 * 1024 + 1; ++i) {
            invalidValue += "x";
        }
        cond = new CompositeColumnValueCondition(CompositeColumnValueCondition.LogicOperator.AND);
        cond.addCondition(new SingleColumnValueCondition(
                "ColX9", SingleColumnValueCondition.CompareOperator.EQUAL,
                ColumnValue.fromString("ValueX")))
                .addCondition(new SingleColumnValueCondition(
                        "ColX9", SingleColumnValueCondition.CompareOperator.EQUAL,
                        ColumnValue.fromString("ValueX")));
        assertTrue(!success);
    }

    /**
     * Write one row with one column that contains 10 cells, the values being 0, 1, 2 ... 9 respectively. Test the scenario where LatestVersionOnly is set to its default value, 
     * or explicitly set to True or False. Also, test the condition when the condition is <=1 and >=3 for updates and verification.
     * @throws Exception
     */
    @Test
    public void testLatestVersionOnly() throws Exception {
        CreateTable(100);

        int row = 1;
        String colName = "col";
        {
        	// Prepare data
	        RowUpdateChange change = new RowUpdateChange(tableName);
	        change.setPrimaryKey(getPrimaryKeys(row));
	        for (int i = 0; i < 10; ++i) {
	        	ColumnValue colValue = ColumnValue.fromLong(i);
	        	change.put(colName, colValue, i);
	        }
	        UpdateRowRequest request = new UpdateRowRequest();
	        request.setRowChange(change);
	        ots.updateRow(request);
        }

        {
        	// LatestVersionOnly:true, <=1, Update failed
            SingleColumnValueCondition columnCondition = new SingleColumnValueCondition(
                    colName, SingleColumnValueCondition.CompareOperator.EQUAL,
                    ColumnValue.fromLong(1));
            columnCondition.setLatestVersionsOnly(true);
            assertTrue(!updateRow(row, colName, ColumnValue.fromLong(9), columnCondition));
        }
        {
        	// LatestVersionOnly:true, >=3, Update succeeded
            SingleColumnValueCondition columnCondition = new SingleColumnValueCondition(
                    colName, SingleColumnValueCondition.CompareOperator.GREATER_THAN,
                    ColumnValue.fromLong(3));
            columnCondition.setLatestVersionsOnly(true);
            assertTrue(updateRow(row, colName, ColumnValue.fromLong(9), columnCondition));
        }
        {
        	// LatestVersionOnly:false, <=1, Update succeeded
            SingleColumnValueCondition columnCondition = new SingleColumnValueCondition(
                    colName, SingleColumnValueCondition.CompareOperator.EQUAL,
                    ColumnValue.fromLong(1));
            columnCondition.setLatestVersionsOnly(false);
            assertTrue(updateRow(row, colName, ColumnValue.fromLong(9), columnCondition));
        }
        {
        	// LatestVersionOnly:false, >=3, Update succeeded
            SingleColumnValueCondition columnCondition = new SingleColumnValueCondition(
                    colName, SingleColumnValueCondition.CompareOperator.GREATER_THAN,
                    ColumnValue.fromLong(3));
            columnCondition.setLatestVersionsOnly(false);
            assertTrue(updateRow(row, colName, ColumnValue.fromLong(9), columnCondition));
        }
    }

    @Test
    public void testBatchWriteRow() throws Exception {
        CreateTable();

        boolean success = putRow(20, "Col1", ColumnValue.fromString("Value20"), null);
        assertTrue(success);

        success = putRow(21, "Col1", ColumnValue.fromString("Value21"), null);
        assertTrue(success);

        success = putRow(22, "Col1", ColumnValue.fromString("Value22"), null);

        RowPutChange putChange = new RowPutChange(tableName);
        putChange.setPrimaryKey(getPrimaryKeys(20));
        putChange.addColumn("Col2", ColumnValue.fromString("Value2"));
        Condition cond = new Condition(RowExistenceExpectation.IGNORE);
        cond.setColumnCondition(new SingleColumnValueCondition(
                "Col1", SingleColumnValueCondition.CompareOperator.NOT_EQUAL,
                ColumnValue.fromString("Value20")));
        putChange.setCondition(cond);

        RowUpdateChange updateChange = new RowUpdateChange(tableName);
        updateChange.setPrimaryKey(getPrimaryKeys(21));
        updateChange.put("Col3", ColumnValue.fromString("Value3"));
        cond = new Condition(RowExistenceExpectation.IGNORE);
        cond.setColumnCondition(new SingleColumnValueCondition(
                "Col1", SingleColumnValueCondition.CompareOperator.EQUAL,
                ColumnValue.fromString("Value21")));
        updateChange.setCondition(cond);

        RowDeleteChange deleteChange = new RowDeleteChange(tableName);
        deleteChange.setPrimaryKey(getPrimaryKeys(22));
        cond = new Condition(RowExistenceExpectation.IGNORE);
        cond.setColumnCondition(new SingleColumnValueCondition(
                "Col1", SingleColumnValueCondition.CompareOperator.GREATER_THAN,
                ColumnValue.fromString("Value22")));
        deleteChange.setCondition(cond);

        BatchWriteRowRequest request = new BatchWriteRowRequest();
        request.addRowChange(putChange);
        request.addRowChange(updateChange);
        request.addRowChange(deleteChange);

        BatchWriteRowResponse Response = ots.batchWriteRow(request);
        List<BatchWriteRowResponse.RowResult> rowStatus = Response.getRowStatus(tableName);
        assertEquals(3, rowStatus.size());
        assertTrue(!rowStatus.get(0).isSucceed());
        assertTrue(rowStatus.get(1).isSucceed());
        assertTrue(!rowStatus.get(2).isSucceed());
    }

    private void CreateTable() throws Exception {
    	CreateTable(1);
    }

    private void CreateTable(int maxVersions) throws Exception {
        TableMeta tableMeta = getTestTableMeta();
        TableOptions options = new TableOptions();
        options.setMaxVersions(maxVersions);
        options.setMaxTimeDeviation(Long.MAX_VALUE / 1000000);
        options.setTimeToLive(-1);
        CreateTableRequest request = new CreateTableRequest(tableMeta, options);
        CapacityUnit tableCU = getTestCapacityUnit();
        request.setReservedThroughput(new ReservedThroughput(tableCU));
        ots.createTable(request);

        Thread.sleep(MILLISECONDS_UNTIL_TABLE_READY);
    }

    private boolean putRow(int row, String colName, ColumnValue colValue, ColumnCondition cond)
    {
        RowPutChange change = new RowPutChange(tableName);
        change.setPrimaryKey(getPrimaryKeys(row));
        change.addColumn(colName, colValue);
        Condition c = new Condition(RowExistenceExpectation.IGNORE);
        c.setColumnCondition(cond);
        change.setCondition(c);
        PutRowRequest request = new PutRowRequest();
        request.setRowChange(change);

        boolean success = true;
        try {
            ots.putRow(request);
        } catch (TableStoreException e) {
            success = false;
        }

        return success;
    }

    private boolean updateRow(int row, String colName, ColumnValue colValue, ColumnCondition cond)
    {
        RowUpdateChange change = new RowUpdateChange(tableName);
        change.setPrimaryKey(getPrimaryKeys(row));
        change.put(colName, colValue);
        Condition c = new Condition(RowExistenceExpectation.IGNORE);
        c.setColumnCondition(cond);
        change.setCondition(c);
        UpdateRowRequest request = new UpdateRowRequest();
        request.setRowChange(change);

        boolean success = true;
        try {
            ots.updateRow(request);
        } catch (TableStoreException e) {
            success = false;
        }

        return success;
    }
    private boolean updateRow(int row, String colName, ColumnValue colValue, long ts, ColumnCondition cond)
    {
        RowUpdateChange change = new RowUpdateChange(tableName);
        change.setPrimaryKey(getPrimaryKeys(row));
        change.put(colName, colValue, ts);
        Condition c = new Condition(RowExistenceExpectation.IGNORE);
        c.setColumnCondition(cond);
        change.setCondition(c);
        UpdateRowRequest request = new UpdateRowRequest();
        request.setRowChange(change);

        boolean success = true;
        try {
            ots.updateRow(request);
        } catch (TableStoreException e) {
            LOG.warning("UpdateRow fails: " + e.toString());
            success = false;
        }

        return success;
    }

    private boolean deleteRow(int row, String colName, ColumnValue colValue, ColumnCondition cond)
    {
        RowDeleteChange change = new RowDeleteChange(tableName);
        change.setPrimaryKey(getPrimaryKeys(row));
        Condition c = new Condition(RowExistenceExpectation.IGNORE);
        c.setColumnCondition(cond);
        change.setCondition(c);
        DeleteRowRequest request = new DeleteRowRequest();
        request.setRowChange(change);

        boolean success = true;
        try {
            ots.deleteRow(request);
        } catch (TableStoreException e) {
            success = false;
        }

        return success;
    }

    private TableMeta getTestTableMeta() {
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("uid", PrimaryKeyType.INTEGER);
        tableMeta.addPrimaryKeyColumn("name", PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn("flag", PrimaryKeyType.INTEGER);
        return tableMeta;
    }
    
    private CapacityUnit getTestCapacityUnit() {
        CapacityUnit capacityUnit = new CapacityUnit();
        capacityUnit.setReadCapacityUnit(0);
        capacityUnit.setWriteCapacityUnit(0);
        return capacityUnit;
    }
    
    private Row readRow(String tableName, int i) {
        SingleRowQueryCriteria rowQueryCriteria = new SingleRowQueryCriteria(tableName);
        rowQueryCriteria.setPrimaryKey(getPrimaryKeys(i));
        rowQueryCriteria.setMaxVersions(1);
        GetRowRequest request = new GetRowRequest();
        request.setRowQueryCriteria(rowQueryCriteria);
        GetRowResponse Response = ots.getRow(request);
        return Response.getRow();
    }
    
    private PrimaryKey getPrimaryKeys(int i) {
        PrimaryKeyBuilder primaryKeys = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeys.addPrimaryKeyColumn("uid", PrimaryKeyValue.fromLong(i));
        primaryKeys.addPrimaryKeyColumn("name", PrimaryKeyValue.fromString(String.format("%05d", i)));
        primaryKeys.addPrimaryKeyColumn("flag", PrimaryKeyValue.fromLong(i * i));
        return primaryKeys.build();
    }
}
