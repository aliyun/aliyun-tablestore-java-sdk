package com.aliyun.openservices.ots.integration;

import com.aliyun.openservices.ots.*;
import com.aliyun.openservices.ots.model.*;
import com.aliyun.openservices.ots.model.condition.*;
import com.aliyun.openservices.ots.utils.ServiceSettings;

import org.apache.commons.logging.Log;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import static org.junit.Assert.*;


public class ConditionalUpdateTest {
    private static final int MILLISECONDS_UNTIL_TABLE_READY = 15 * 1000;
    
    private static String tableName = "conditional_update_test_table";
    private static final OTS ots = OTSClientFactory.createOTSClient(
            ServiceSettings.load(), new ClientConfiguration());
    private static Logger LOG = Logger.getLogger(ConditionalUpdateTest.class.getName());

    @Before
    public void setup() throws Exception {
        LOG.info("Instance: " + ServiceSettings.load().getOTSInstanceName());

        ListTableResult r = ots.listTable();

        for (String table: r.getTableNames()) {
            DeleteTableRequest deleteTableRequest = new DeleteTableRequest(table);
            ots.deleteTable(deleteTableRequest);
            LOG.info("Delete table: " + table);

            Thread.sleep(1000);
        }
    }

    @Test
    public void testSingleColumnCondition() throws Exception {
        LOG.info("Start testSingleColumnCondition");

        CreateTable();

        // put a row first
        boolean success = putRow(19, "Col1", ColumnValue.fromString("Value1"), null);
        assertTrue(success);

        // put row with condition: col1 != value1
        success = putRow(19, "Col2", ColumnValue.fromString("Value2"),
                new RelationalCondition("Col1",
                        RelationalCondition.CompareOperator.NOT_EQUAL,
                        ColumnValue.fromString("Value1")));
        assertTrue(!success);

        // put row with condition: col1 == value1
        success = putRow(19, "Col2", ColumnValue.fromString("Value2"),
                new RelationalCondition("Col1",
                        RelationalCondition.CompareOperator.EQUAL,
                        ColumnValue.fromString("Value1")));
        assertTrue(success);

        // update row with condition: col2 < value1
        success = putRow(19, "Col3", ColumnValue.fromString("Value3"),
                new RelationalCondition("Col2",
                        RelationalCondition.CompareOperator.LESS_THAN,
                        ColumnValue.fromString("Value1")));
        assertTrue(!success);

        // update row with condition: col2 >= value2
        success = putRow(19, "Col3", ColumnValue.fromString("Value3"),
                new RelationalCondition("Col2",
                        RelationalCondition.CompareOperator.GREATER_EQUAL,
                        ColumnValue.fromString("Value2")));
        assertTrue(success);

        // delete row with condition: col3 <= value2
        success = deleteRow(19, "Col3", ColumnValue.fromString("Value3"),
                new RelationalCondition("Col3",
                        RelationalCondition.CompareOperator.LESS_EQUAL,
                        ColumnValue.fromString("Value2")));
        assertTrue(!success);

        // delete row with condition: col3 > value2
        success = deleteRow(19, "Col3", ColumnValue.fromString("Value3"),
                new RelationalCondition("Col3",
                        RelationalCondition.CompareOperator.GREATER_THAN,
                        ColumnValue.fromString("Value2")));
        assertTrue(success);
    }

    @Test
    public void testColumnMissing() throws  Exception {
        LOG.info("Start testColumnMissing");

        CreateTable();

        // put a row first
        boolean success = putRow(19, "Col1", ColumnValue.fromString("Value1"), null);
        assertTrue(success);

        // put row with condition: colX != valueY
        // with passIfMissing == true, this should succeed
        success = putRow(19, "Col2", ColumnValue.fromString("Value2"),
                new RelationalCondition("ColX",
                        RelationalCondition.CompareOperator.NOT_EQUAL,
                        ColumnValue.fromString("ValueY")));
        assertTrue(success);

        // put row with condition: colX != valueY
        // with passIfMissing == false, this should fail
        RelationalCondition cond = new RelationalCondition("ColX",
                RelationalCondition.CompareOperator.NOT_EQUAL,
                ColumnValue.fromString("ValueY"));
        cond.setPassIfMissing(false);
        success = putRow(19, "Col2", ColumnValue.fromString("Value2"), cond);
        assertTrue(!success);
    }

    @Test
    public void testCompositeCondition() throws Exception {
        LOG.info("Start testCompositeCondition");

        CreateTable();

        // put a row first
        boolean success = putRow(19, "Col1", ColumnValue.fromString("Value1"), null);
        assertTrue(success);
        success = updateRow(19, "Col2", ColumnValue.fromString("Value2"), null);
        assertTrue(success);

        // update with condition:
        // col1 == value2 OR col2 == value1
        CompositeCondition cond = new CompositeCondition(CompositeCondition.LogicOperator.OR);
        cond.addCondition(new RelationalCondition(
                "Col1",
                RelationalCondition.CompareOperator.EQUAL,
                ColumnValue.fromString("Value2")))
        .addCondition(new RelationalCondition(
                "Col2",
                RelationalCondition.CompareOperator.EQUAL,
                ColumnValue.fromString("Value1")));
        success = updateRow(19, "Col3", ColumnValue.fromString("Value3"), cond);
        assertTrue(!success);

        // update with condition:
        // Not col1 == value2
        cond = new CompositeCondition(CompositeCondition.LogicOperator.NOT);
        cond.addCondition(new RelationalCondition(
                "Col1",
                RelationalCondition.CompareOperator.EQUAL,
                ColumnValue.fromString("Value2")));
        success = updateRow(19, "Col3", ColumnValue.fromString("Value3"), cond);
        assertTrue(success);

        // delete with condition:
        // col1 == valueX OR (col2 == value2 AND col3 == value3)
        cond = new CompositeCondition(CompositeCondition.LogicOperator.OR);
        cond.addCondition(new RelationalCondition(
                "Col1",
                RelationalCondition.CompareOperator.EQUAL,
                ColumnValue.fromString("ValueX")));
        CompositeCondition cond2 = new CompositeCondition(CompositeCondition.LogicOperator.AND);
        cond2.addCondition(new RelationalCondition(
                        "Col2",
                        RelationalCondition.CompareOperator.EQUAL,
                        ColumnValue.fromString("Value2")))
                .addCondition(new RelationalCondition(
                        "Col3",
                        RelationalCondition.CompareOperator.EQUAL,
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
                ColumnValue oldValue = r.getColumns().get("Col1");

                ColumnValue newValue = ColumnValue.fromLong(oldValue.asLong() + 1);
                RelationalCondition cond = new RelationalCondition(
                        "Col1", RelationalCondition.CompareOperator.EQUAL, oldValue);
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
        LOG.info("Start testTransactionalUpdate");

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

        ColumnValue v = readRow(tableName, 23).getColumns().get("Col1");
        assertEquals(total, v.asLong());
    }

    @Test
    public void testLimits() throws  Exception {
        LOG.info("Start testLimits");

        CreateTable();

        // column condition count <= 10
        CompositeCondition cond = new CompositeCondition(CompositeCondition.LogicOperator.OR);
        cond.addCondition(new RelationalCondition(
                "ColX1", RelationalCondition.CompareOperator.EQUAL,
                ColumnValue.fromString("ValueX")))
                .addCondition(new RelationalCondition(
                        "ColX2", RelationalCondition.CompareOperator.EQUAL,
                        ColumnValue.fromString("ValueX")))
                .addCondition(new RelationalCondition(
                        "ColX3", RelationalCondition.CompareOperator.EQUAL,
                        ColumnValue.fromString("ValueX")))
                .addCondition(new RelationalCondition(
                        "ColX4", RelationalCondition.CompareOperator.EQUAL,
                        ColumnValue.fromString("ValueX")))
                .addCondition(new RelationalCondition(
                        "ColX5", RelationalCondition.CompareOperator.EQUAL,
                        ColumnValue.fromString("ValueX")))
                .addCondition(new RelationalCondition(
                        "ColX6", RelationalCondition.CompareOperator.EQUAL,
                        ColumnValue.fromString("ValueX")))
                .addCondition(new RelationalCondition(
                        "ColX7", RelationalCondition.CompareOperator.EQUAL,
                        ColumnValue.fromString("ValueX")))
                .addCondition(new RelationalCondition(
                        "ColX8", RelationalCondition.CompareOperator.EQUAL,
                        ColumnValue.fromString("ValueX")))
                .addCondition(new RelationalCondition(
                        "ColX9", RelationalCondition.CompareOperator.EQUAL,
                        ColumnValue.fromString("ValueX")));

        boolean success = putRow(23, "Col1", ColumnValue.fromString("Value1"), cond);
        assertTrue(success);

        cond.addCondition(new RelationalCondition(
                "ColX10", RelationalCondition.CompareOperator.EQUAL,
                ColumnValue.fromString("ValueX")));
        success = putRow(23, "Col1", ColumnValue.fromString("Value1"), cond);
        assertTrue(!success);

        // invalid column name in column condition
        cond = new CompositeCondition(CompositeCondition.LogicOperator.AND);
        cond.addCondition(new RelationalCondition(
                "9Col", RelationalCondition.CompareOperator.EQUAL,
                ColumnValue.fromString("ValueX")))
                .addCondition(new RelationalCondition(
                        "ColX9", RelationalCondition.CompareOperator.EQUAL,
                        ColumnValue.fromString("ValueX")));
        success = putRow(23, "Col1", ColumnValue.fromString("Value1"), cond);
        assertTrue(success);
    }

    @Test
    public void testBatchWriteRow() throws Exception {
        LOG.info("Start testBatchWriteRow");

        CreateTable();

        boolean success = putRow(20, "Col1", ColumnValue.fromString("Value20"), null);
        assertTrue(success);

        success = putRow(21, "Col1", ColumnValue.fromString("Value21"), null);
        assertTrue(success);

        success = putRow(22, "Col1", ColumnValue.fromString("Value22"), null);

        RowPutChange putChange = new RowPutChange(tableName);
        putChange.setPrimaryKey(getRowPrimaryKeys(20));
        putChange.addAttributeColumn("Col2", ColumnValue.fromString("Value2"));
        Condition cond = new Condition(RowExistenceExpectation.IGNORE);
        cond.setColumnCondition(new RelationalCondition(
                "Col1", RelationalCondition.CompareOperator.NOT_EQUAL,
                ColumnValue.fromString("Value20")));
        putChange.setCondition(cond);

        RowUpdateChange updateChange = new RowUpdateChange(tableName);
        updateChange.setPrimaryKey(getRowPrimaryKeys(21));
        updateChange.addAttributeColumn("Col3", ColumnValue.fromString("Value3"));
        cond = new Condition(RowExistenceExpectation.IGNORE);
        cond.setColumnCondition(new RelationalCondition(
                "Col1", RelationalCondition.CompareOperator.EQUAL,
                ColumnValue.fromString("Value21")));
        updateChange.setCondition(cond);

        RowDeleteChange deleteChange = new RowDeleteChange(tableName);
        deleteChange.setPrimaryKey(getRowPrimaryKeys(22));
        cond = new Condition(RowExistenceExpectation.IGNORE);
        cond.setColumnCondition(new RelationalCondition(
                "Col1", RelationalCondition.CompareOperator.GREATER_THAN,
                ColumnValue.fromString("Value22")));
        deleteChange.setCondition(cond);

        BatchWriteRowRequest request = new BatchWriteRowRequest();
        request.addRowPutChange(putChange);
        request.addRowUpdateChange(updateChange);
        request.addRowDeleteChange(deleteChange);

        BatchWriteRowResult result = ots.batchWriteRow(request);
        List<BatchWriteRowResult.RowStatus> putStatus = result.getPutRowStatus(tableName);
        assertEquals(1, putStatus.size());
        assertTrue(!putStatus.get(0).isSucceed());

        List<BatchWriteRowResult.RowStatus> updateStatus = result.getUpdateRowStatus(tableName);
        assertEquals(1, updateStatus.size());
        assertTrue(updateStatus.get(0).isSucceed());

        List<BatchWriteRowResult.RowStatus> deleteStatus = result.getDeleteRowStatus(tableName);
        assertEquals(1, deleteStatus.size());
        assertTrue(!deleteStatus.get(0).isSucceed());
    }

    private void CreateTable() throws Exception{
        TableMeta tableMeta = getTestTableMeta();
        CapacityUnit tableCU = getTestCapacityUnit();
        CreateTableRequest request = new CreateTableRequest();
        request.setTableMeta(tableMeta);
        request.setReservedThroughput(tableCU);
        ots.createTable(request);

        LOG.info("Create table: " + tableName);
        Thread.sleep(MILLISECONDS_UNTIL_TABLE_READY);
    }

    private boolean putRow(int row, String colName, ColumnValue colValue, ColumnCondition cond)
    {
        RowPutChange change = new RowPutChange(tableName);
        change.setPrimaryKey(getRowPrimaryKeys(row));
        change.addAttributeColumn(colName, colValue);
        Condition c = new Condition(RowExistenceExpectation.IGNORE);
        c.setColumnCondition(cond);
        change.setCondition(c);
        PutRowRequest request = new PutRowRequest();
        request.setRowChange(change);

        boolean success = true;
        try {
            ots.putRow(request);
        } catch (ServiceException e) {
            LOG.warning("PutRow fails: " + e.toString());
            success = false;
        }

        return success;
    }

    private boolean updateRow(int row, String colName, ColumnValue colValue, ColumnCondition cond)
    {
        RowUpdateChange change = new RowUpdateChange(tableName);
        change.setPrimaryKey(getRowPrimaryKeys(row));
        change.addAttributeColumn(colName, colValue);
        Condition c = new Condition(RowExistenceExpectation.IGNORE);
        c.setColumnCondition(cond);
        change.setCondition(c);
        UpdateRowRequest request = new UpdateRowRequest();
        request.setRowChange(change);

        boolean success = true;
        try {
            ots.updateRow(request);
        } catch (ServiceException e) {
            LOG.warning("UpdateRow fails: " + e.toString());
            success = false;
        }

        return success;
    }

    private boolean deleteRow(int row, String colName, ColumnValue colValue, ColumnCondition cond)
    {
        RowDeleteChange change = new RowDeleteChange(tableName);
        change.setPrimaryKey(getRowPrimaryKeys(row));
        Condition c = new Condition(RowExistenceExpectation.IGNORE);
        c.setColumnCondition(cond);
        change.setCondition(c);
        DeleteRowRequest request = new DeleteRowRequest();
        request.setRowChange(change);

        boolean success = true;
        try {
            ots.deleteRow(request);
        } catch (ServiceException e) {
            LOG.warning("DeleteRow fails: " + e.toString());
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
        capacityUnit.setReadCapacityUnit(1);
        capacityUnit.setWriteCapacityUnit(1);
        return capacityUnit;
    }
    
    private Row readRow(String tableName, int i) {
        SingleRowQueryCriteria rowQueryCriteria = new SingleRowQueryCriteria(tableName);
        rowQueryCriteria.setPrimaryKey(getRowPrimaryKeys(i));
        GetRowRequest request = new GetRowRequest();
        request.setRowQueryCriteria(rowQueryCriteria);
        GetRowResult result = ots.getRow(request);
        return result.getRow();
    }
    
    private RowPrimaryKey getRowPrimaryKeys(int i) {
        RowPrimaryKey primaryKeys = new RowPrimaryKey();
        primaryKeys.addPrimaryKeyColumn("uid", PrimaryKeyValue.fromLong(i));
        primaryKeys.addPrimaryKeyColumn("name", PrimaryKeyValue.fromString(String.format("%05d", i)));
        primaryKeys.addPrimaryKeyColumn("flag", PrimaryKeyValue.fromLong(i * i));
        return primaryKeys;
    }
}
