package com.alicloud.openservices.tablestore.functiontest;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.core.ErrorCode;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.Error;
import com.alicloud.openservices.tablestore.model.BatchGetRowResponse.RowResult;
import com.alicloud.openservices.tablestore.core.utils.Pair;
import com.alicloud.openservices.tablestore.common.*;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;
import com.google.gson.JsonSyntaxException;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class ParameterLegalityTest extends BaseFT {

    private static String tableName = "ParameterLegalityFunctiontest";

    private static SyncClientInterface ots;

    private static final Logger LOG = LoggerFactory.getLogger(ParameterLegalityTest.class);

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
        // 清理环境
        OTSHelper.deleteAllTable(ots);
    }

    @After
    public void teardown() {

    }

    /**
     * 创建测试表, 指定maxVersions
     *
     * @param maxVersions
     */
    private void createTable(int maxVersions) {
        List<PrimaryKeySchema> pks = new ArrayList<PrimaryKeySchema>();
        pks.add(new PrimaryKeySchema("pk", PrimaryKeyType.STRING));
        TableMeta meta = new TableMeta(tableName);
        meta.addPrimaryKeyColumns(pks);
        TableOptions tableOptions = new TableOptions();
        tableOptions.setMaxVersions(maxVersions);
        tableOptions.setTimeToLive(-1);
        tableOptions.setMaxTimeDeviation(Long.MAX_VALUE / 1000000);
        OTSHelper.createTable(ots, meta, new CapacityUnit(0, 0),
                tableOptions);
        Utils.waitForPartitionLoad(tableName);
    }

    private PrimaryKey getPrimaryKey() {
        return getPrimaryKey("pk");
    }

    private PrimaryKey getPrimaryKey(String pkValue) {
        List<PrimaryKeyColumn> primaryKey = new ArrayList<PrimaryKeyColumn>();
        primaryKey.add(new PrimaryKeyColumn("pk", PrimaryKeyValue
                .fromString(pkValue)));
        PrimaryKey pk = new PrimaryKey(primaryKey);
        return pk;
    }

    private void checkException(Exception ex, Exception e) {
        LOG.info(e.toString());
        if (ex instanceof TableStoreException) {
            assertTableStoreException((TableStoreException) ex, (TableStoreException) e);
        }
        if (ex instanceof ClientException) {
            assertClientException((ClientException) ex, (ClientException) e);
        }
    }

    private void checkCreateTable(SyncClientInterface ots, String tableName, Exception ex) {
        List<PrimaryKeySchema> pks = new ArrayList<PrimaryKeySchema>();
        pks.add(new PrimaryKeySchema("pk", PrimaryKeyType.STRING));
        TableMeta meta = new TableMeta(tableName);
        meta.addPrimaryKeyColumns(pks);
        TableOptions tableOptions = new TableOptions();
        tableOptions.setMaxVersions(Integer.MAX_VALUE);
        tableOptions.setMaxTimeDeviation(Long.MAX_VALUE / 1000000);
        tableOptions.setTimeToLive(-1);
        if (ex == null) {
            OTSHelper.createTable(ots, meta, new CapacityUnit(0, 0),
                    tableOptions);
        } else {
            try {
                OTSHelper.createTable(ots, meta, new CapacityUnit(0, 0),
                        tableOptions);
                fail();
            } catch (Exception e) {
                checkException(ex, e);
            }
        }
    }

    private void checkUpdateTable(SyncClientInterface ots, String tableName, Exception ex) {
        TableOptions tableOptions = new TableOptions();
        tableOptions.setMaxVersions(10);
        if (ex == null) {
            OTSHelper.updateTable(ots, tableName, null, tableOptions);
        } else {
            try {
                OTSHelper.updateTable(ots, tableName, null, tableOptions);
                fail();
            } catch (Exception e) {
                checkException(ex, e);
            }
        }
    }

    private DescribeTableResponse checkDescribeTable(SyncClientInterface ots, String tableName, Exception ex) {
        if (ex == null) {
            return OTSHelper.describeTable(ots, tableName);
        } else {
            try {
                OTSHelper.describeTable(ots, tableName);
                fail();
            } catch (Exception e) {
                checkException(ex, e);
            }
            return null;
        }
    }

    private void checkDeleteTable(SyncClientInterface ots, String tableName, Exception ex) {
        if (ex == null) {
            OTSHelper.deleteTable(ots, tableName);
        } else {
            try {
                OTSHelper.deleteTable(ots, tableName);
                fail();
            } catch (Exception e) {
                checkException(ex, e);
            }
        }
    }

    private GetRowResponse checkGetRow(SyncClientInterface ots, String tableName, PrimaryKey pk, String columnName, Exception ex) {
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, pk);
        criteria.addColumnsToGet(columnName);
        criteria.setMaxVersions(Integer.MAX_VALUE);
        if (ex == null) {
            return OTSHelper.getRow(ots, criteria);
        } else {
            try {
                OTSHelper.getRow(ots, criteria);
                fail();
            } catch (Exception e) {
                checkException(ex, e);
            }
            return null;
        }
    }

    private void checkUpdateRow(SyncClientInterface ots, String tableName, PrimaryKey pk, String columnName, Exception ex) {
        List<Column> puts = new ArrayList<Column>();
        Column column = new Column(columnName, ColumnValue.fromString("col_value"), 1L);
        puts.add(column);
        puts.add(new Column(columnName, ColumnValue.fromString("col_value"), 2L));
        List<String> deletes = new ArrayList<String>();
        deletes.add(columnName);
        List<Pair<String, Long>> deleteCells = new ArrayList<Pair<String, Long>>();
        deleteCells.add(new Pair<String, Long>(columnName, 2L));
        if (ex == null) {
            OTSHelper.updateRow(ots, tableName, pk, null, deletes, null);
            OTSHelper.updateRow(ots, tableName, pk, puts, null, null);
            OTSHelper.updateRow(ots, tableName, pk, null, null, deleteCells);
        } else {
            try {
                OTSHelper.updateRow(ots, tableName, pk, puts, null, null);
                fail();
            } catch (Exception e) {
                checkException(ex, e);
            }
            try {
                OTSHelper.updateRow(ots, tableName, pk, null, deletes, null);
                fail();
            } catch (Exception e) {
                checkException(ex, e);
            }
            try {
                OTSHelper.updateRow(ots, tableName, pk, null, null, deleteCells);
                fail();
            } catch (Exception e) {
                checkException(ex, e);
            }
        }
    }

    private void checkDeleteRow(SyncClientInterface ots, String tableName, PrimaryKey pk, Exception ex) {
        if (ex == null) {
            OTSHelper.deleteRow(ots, tableName, pk);
        } else {
            try {
                OTSHelper.deleteRow(ots, tableName, pk);
                fail();
            } catch (Exception e) {
                System.out.println("deleteRow:" + e);
                checkException(ex, e);
            }
        }
    }

    private void checkPutRow(SyncClientInterface ots, String tableName, PrimaryKey pk, String columnName, Exception ex) {
        List<Column> columns = new ArrayList<Column>();
        ColumnValue cv = ColumnValue.fromString("col_value");
        columns.add(new Column(columnName, cv, 1L));
        if (ex == null) {
            OTSHelper.putRow(ots, tableName, pk, columns);
        } else {
            try {
                OTSHelper.putRow(ots, tableName, pk, columns);
                fail();
            } catch (Exception e) {
                System.out.println(e);
                checkException(ex, e);
            }
        }
    }

    private void checkBatchWriteRow(SyncClientInterface ots, String tableName, PrimaryKey pk, String columnName, TableStoreException ex) {
        List<Column> columns = new ArrayList<Column>();
        columns.add(new Column(columnName, ColumnValue.fromString("col_value"), 1L));
        List<RowPutChange> puts = new ArrayList<RowPutChange>();
        RowPutChange rowPutChange = new RowPutChange(tableName, pk);
        rowPutChange.addColumns(columns);
        puts.add(rowPutChange);

        List<RowUpdateChange> updates = new ArrayList<RowUpdateChange>();
        RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, pk);
        rowUpdateChange.put(columns);
        updates.add(rowUpdateChange);

        List<RowDeleteChange> deletes = new ArrayList<RowDeleteChange>();
        RowDeleteChange rowDeleteChange = new RowDeleteChange(tableName, pk);
        deletes.add(rowDeleteChange);

        if (ex == null) {

            BatchWriteRowResponse result = OTSHelper.batchWriteRow(ots, puts, null, null);
            assertEquals(0, result.getFailedRows().size());

            result = OTSHelper.batchWriteRow(ots, null, updates, null);
            assertEquals(0, result.getFailedRows().size());

            result = OTSHelper.batchWriteRow(ots, null, null, deletes);
            assertEquals(0, result.getFailedRows().size());

        } else {

            BatchWriteRowResponse result = OTSHelper.batchWriteRow(ots, puts, null, null);
            assertEquals(result.getFailedRows().get(0).getError().getCode(), ex.getErrorCode());
            assertEquals(result.getFailedRows().get(0).getError().getMessage(), ex.getMessage());

            result = OTSHelper.batchWriteRow(ots, null, updates, null);
            assertEquals(result.getFailedRows().get(0).getError().getCode(), ex.getErrorCode());
            assertEquals(result.getFailedRows().get(0).getError().getMessage(), ex.getMessage());

            result = OTSHelper.batchWriteRow(ots, null, null, deletes);
            assertEquals(result.getFailedRows().get(0).getError().getCode(), ex.getErrorCode());
            assertEquals(result.getFailedRows().get(0).getError().getMessage(), ex.getMessage());

        }

    }

    private void checkBatchWriteRowForException(SyncClientInterface ots, String tableName, PrimaryKey pk, String columnName, TableStoreException ex) {
        List<Column> columns = new ArrayList<Column>();
        columns.add(new Column(columnName, ColumnValue.fromString("col_value"), 1L));
        List<RowPutChange> puts = new ArrayList<RowPutChange>();
        RowPutChange rowPutChange = new RowPutChange(tableName, pk);
        rowPutChange.addColumns(columns);
        puts.add(rowPutChange);

        List<RowUpdateChange> updates = new ArrayList<RowUpdateChange>();
        RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, pk);
        rowUpdateChange.put(columns);
        updates.add(rowUpdateChange);

        List<RowDeleteChange> deletes = new ArrayList<RowDeleteChange>();
        RowDeleteChange rowDeleteChange = new RowDeleteChange(tableName, pk);
        deletes.add(rowDeleteChange);

        if (ex == null) {

            BatchWriteRowResponse result = OTSHelper.batchWriteRow(ots, puts, null, null);
            assertEquals(0, result.getFailedRows().size());

            result = OTSHelper.batchWriteRow(ots, null, updates, null);
            assertEquals(0, result.getFailedRows().size());

            result = OTSHelper.batchWriteRow(ots, null, null, deletes);
            assertEquals(0, result.getFailedRows().size());

        } else {

            try {
                OTSHelper.batchWriteRow(ots, puts, null, null);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ex, e);
            }

            try {
                OTSHelper.batchWriteRow(ots, null, updates, null);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ex, e);
            }

            try {
                OTSHelper.batchWriteRow(ots, null, null, deletes);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ex, e);
            }
        }

    }

    private void checkBatchWriteRowNoDeleteForException(SyncClientInterface ots, String tableName, PrimaryKey pk, String columnName, TableStoreException ex) {
        List<Column> columns = new ArrayList<Column>();
        columns.add(new Column(columnName, ColumnValue.fromString("col_value"), 1L));
        List<RowPutChange> puts = new ArrayList<RowPutChange>();
        RowPutChange rowPutChange = new RowPutChange(tableName, pk);
        rowPutChange.addColumns(columns);
        puts.add(rowPutChange);

        List<RowUpdateChange> updates = new ArrayList<RowUpdateChange>();
        RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, pk);
        rowUpdateChange.put(columns);
        updates.add(rowUpdateChange);

        List<RowDeleteChange> deletes = new ArrayList<RowDeleteChange>();
        RowDeleteChange rowDeleteChange = new RowDeleteChange(tableName, pk);
        deletes.add(rowDeleteChange);

        if (ex == null) {

            BatchWriteRowResponse result = OTSHelper.batchWriteRow(ots, puts, null, null);
            assertEquals(0, result.getFailedRows().size());

            result = OTSHelper.batchWriteRow(ots, null, updates, null);
            assertEquals(0, result.getFailedRows().size());

            result = OTSHelper.batchWriteRow(ots, null, null, deletes);
            assertEquals(0, result.getFailedRows().size());

        } else {

            try {
                OTSHelper.batchWriteRow(ots, puts, null, null);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ex, e);
            }

            try {
                OTSHelper.batchWriteRow(ots, null, updates, null);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ex, e);
            }
        }

    }

    private BatchGetRowResponse checkBatchGetRow(SyncClientInterface ots, String tableName, PrimaryKey pk, String columnName, Exception ex) {
        List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
        MultiRowQueryCriteria criteria = new MultiRowQueryCriteria(tableName);
        criteria.addColumnsToGet(columnName);
        criteria.addRow(pk);
        criteria.setMaxVersions(Integer.MAX_VALUE);
        criterias.add(criteria);

        if (ex == null) {
            return OTSHelper.batchGetRow(ots, criterias);
        } else {
            try {
                List<RowResult> ss = OTSHelper.batchGetRow(ots, criterias).getFailedRows();
                System.out.println(ss.size());
                fail();
            } catch (Exception e) {
                checkException(ex, e);
            }
            return null;
        }
    }

    private BatchGetRowResponse checkBatchGetRowForException(SyncClientInterface ots, String tableName, PrimaryKey pk, String columnName, TableStoreException ex) {
        List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
        MultiRowQueryCriteria criteria = new MultiRowQueryCriteria(tableName);
        criteria.addColumnsToGet(columnName);
        criteria.addRow(pk);
        criteria.setMaxVersions(Integer.MAX_VALUE);
        criterias.add(criteria);

        if (ex == null) {
            return OTSHelper.batchGetRow(ots, criterias);
        } else {

            List<RowResult> ss = OTSHelper.batchGetRow(ots, criterias).getFailedRows();
            Error err = ss.get(0).getError();
            assertEquals(ex.getErrorCode(), err.getCode());
            assertEquals(ex.getMessage(), err.getMessage());
            return null;
        }
    }

    private GetRangeResponse checkGetRange(SyncClientInterface ots, RangeRowQueryCriteria criteria, Exception ex) {
        if (ex == null) {
            return OTSHelper.getRange(ots, criteria);
        } else {
            try {
                OTSHelper.getRange(ots, criteria);
                fail();
            } catch (Exception e) {
                checkException(ex, e);
            }
            return null;
        }
    }


    /**
     * 测试所有相关API中表名为空，'0', '#', '中文', 'T#', 'T中文', '3t', '-'的情况，
     * 期望返回ErrorCode: OTSParameterInvalid
     */
    @Test
    public void testInvalidTableName() {
        String tableNames[] = {"0", "#", "中文", "T#", "T中文", "3t", "-"};
        String columnName = "col_name";
        for (int i = 0; i < tableNames.length; i++) {
            String tableName = tableNames[i];

            String mesg = String.format("Invalid table name: '%s'.", tableNames[i]);

            TableStoreException expect = new TableStoreException(mesg, null, ErrorCode.INVALID_PARAMETER, null, 400);

            checkCreateTable(ots, tableName, expect);

            checkUpdateTable(ots, tableName, expect);

            checkDescribeTable(ots, tableName, expect);

            checkDeleteTable(ots, tableName, expect);

            checkPutRow(ots, tableName, getPrimaryKey(), columnName, expect);

            checkGetRow(ots, tableName, getPrimaryKey(), columnName, expect);

            checkUpdateRow(ots, tableName, getPrimaryKey(), columnName, expect);

            checkDeleteRow(ots, tableName, getPrimaryKey(), expect);

            checkBatchGetRow(ots, tableName, getPrimaryKey(), columnName, expect);

            checkBatchWriteRowForException(ots, tableName, getPrimaryKey(), columnName, expect);

            RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
            criteria.setInclusiveStartPrimaryKey(getPrimaryKey("1"));
            criteria.setExclusiveEndPrimaryKey(getPrimaryKey("2"));
            checkGetRange(ots, criteria, expect);
        }
    }

    /**
     * 测试所有相关API中表名为'_0', '_T', 'A0'的情况，期望操作成功
     */
    @Test
    public void testValidTableName() {

        String tableNames[] = {"_0", "_T", "A0"};
        String columnName = "col_name";
        List<Column> expectColumns = new ArrayList<Column>();
        expectColumns.add(new Column(columnName, ColumnValue.fromString("col_value")));

        for (int i = 0; i < tableNames.length; i++) {
            String tableName = tableNames[i];

            TableStoreException expect = null;

            checkCreateTable(ots, tableName, expect);

            Utils.sleepSeconds(OTSTestConst.UPDATE_TABLE_SLEEP_IN_SECOND);

            checkUpdateTable(ots, tableName, expect);

            checkDescribeTable(ots, tableName, expect);

            checkDeleteTable(ots, tableName, expect);

            checkCreateTable(ots, tableName, expect);

            Utils.waitForPartitionLoad(tableName);

            checkPutRow(ots, tableName, getPrimaryKey(), columnName, expect);

            GetRowResponse getRowResult = checkGetRow(ots, tableName, getPrimaryKey(), columnName, expect);
            Utils.checkColumns(getRowResult.getRow().getColumns(), expectColumns, false);

            checkUpdateRow(ots, tableName, getPrimaryKey(), columnName, expect);

            BatchGetRowResponse batchGetRowResult = checkBatchGetRow(ots, tableName, getPrimaryKey(), columnName, expect);
            Utils.checkColumns(batchGetRowResult.getSucceedRows().get(0).getRow().getColumns(), expectColumns, false);

            checkDeleteRow(ots, tableName, getPrimaryKey(), expect);

            checkBatchWriteRow(ots, tableName, getPrimaryKey(), columnName, expect);

            RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
            criteria.setInclusiveStartPrimaryKey(getPrimaryKey("1"));
            criteria.setExclusiveEndPrimaryKey(getPrimaryKey("2"));
            criteria.setMaxVersions(Integer.MAX_VALUE);
            checkGetRange(ots, criteria, expect);
        }
    }

    /**
     * 测试使用非法的列名时，期望得到OTSParameterInvalid
     */
    @Test
    public void testInvalidColumnName() {

        // createTable
        createTable(3);

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 256; i++) {
            sb.append('a');
        }

        String columnNames[] = {sb.toString(), "#", "#abc", "#阿里巴巴", "# "};
        for (int i = 0; i < columnNames.length; i++) {
            String columnName = columnNames[i];

            String msg = String.format("Invalid column name: '%s'.", columnName);

            TableStoreException expect = new TableStoreException(msg, null, ErrorCode.INVALID_PARAMETER, null, 400);

            checkPutRow(ots, tableName, getPrimaryKey(), columnName, expect);

            checkGetRow(ots, tableName, getPrimaryKey(), columnName, expect);

            checkUpdateRow(ots, tableName, getPrimaryKey(), columnName, expect);

            checkBatchGetRow(ots, tableName, getPrimaryKey(), columnName, expect);

            checkBatchWriteRowNoDeleteForException(ots, tableName, getPrimaryKey(), columnName, expect);

            RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
            criteria.setInclusiveStartPrimaryKey(getPrimaryKey("1"));
            criteria.setExclusiveEndPrimaryKey(getPrimaryKey("2"));
            criteria.setMaxVersions(Integer.MAX_VALUE);
            criteria.addColumnsToGet(columnName);
            checkGetRange(ots, criteria, expect);

            {
                // check filter
                SingleRowQueryCriteria singleRowCriteria = new SingleRowQueryCriteria(tableName, getPrimaryKey());
                singleRowCriteria.setMaxVersions(100);
                singleRowCriteria.addColumnsToGet("abc");
                SingleColumnValueFilter filter = new SingleColumnValueFilter(columnName, SingleColumnValueFilter.CompareOperator.EQUAL, ColumnValue.fromBoolean(false));
                singleRowCriteria.setFilter(filter);
                try {
                    OTSHelper.getRow(ots, singleRowCriteria);
                    fail();
                } catch (TableStoreException e) {
                    checkException(e, expect);
                }
            }
        }
    }

    /**
     * 测试使用合法的列名操作，期望操作成功。
     */
    @Test
    public void testValidColumnName() {

        // createTable
        createTable(3);

        List<String> columnNames = new ArrayList<String>();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 255; i++) {
            sb.append('a');
        }
        columnNames.add(sb.toString());
        columnNames.add("阿里巴巴");

        char[] specialChar = {'.', ':', '\'', '"', '$', '&', '*', '(', ')', '/', '\\', '\n', '\r', ' '};
        for (int i = 0; i < specialChar.length; i++) {
            char c = specialChar[i];
            columnNames.add("" + c + "abc"); // start with it and length > 1
            columnNames.add("abc" + c); // end with it and length > 1
            columnNames.add("" + c); // length = 1
            columnNames.add("a" + c + "b");  // contains it
        }

        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);

            List<Column> columnsExpect = new ArrayList<Column>();
            columnsExpect.add(new Column(columnName, ColumnValue.fromString("col_value")));

            TableStoreException expect = null;

            checkPutRow(ots, tableName, getPrimaryKey(), columnName, expect);

            GetRowResponse getRowResult = checkGetRow(ots, tableName, getPrimaryKey(), columnName, expect);
            Utils.checkColumns(getRowResult.getRow().getColumns(), columnsExpect, false);

            checkUpdateRow(ots, tableName, getPrimaryKey(), columnName, expect);

            BatchGetRowResponse batchGetRowResult = checkBatchGetRow(ots, tableName, getPrimaryKey(), columnName, expect);
            Utils.checkColumns(batchGetRowResult.getSucceedRows().get(0).getRow().getColumns(), columnsExpect, false);

            checkBatchWriteRow(ots, tableName, getPrimaryKey(), columnName, expect);

            RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
            criteria.setInclusiveStartPrimaryKey(getPrimaryKey("1"));
            criteria.setExclusiveEndPrimaryKey(getPrimaryKey("2"));
            criteria.setMaxVersions(Integer.MAX_VALUE);
            criteria.addColumnsToGet(columnName);
            GetRangeResponse getRangeResult = checkGetRange(ots, criteria, expect);
            assertEquals(getRangeResult.getRows().size(), 0);

            checkReadWrite(ots, tableName, getPrimaryKey(), columnName);
        }
    }

    private void checkReadWrite(SyncClientInterface ots, String tableName, PrimaryKey pk, String columnName) {
        List<Column> columns = new ArrayList<Column>();
        ColumnValue cv = ColumnValue.fromString("col_value");
        ColumnValue cv2 = ColumnValue.fromString("col_value_2");
        ColumnValue cv3 = ColumnValue.fromString("col_value_3");

        columns.add(new Column(columnName, cv, 1L));

        // put on row
        OTSHelper.putRow(ots, tableName, pk, columns);

        // get and check
        GetRowResponse result = OTSHelper.getRowForAll(ots, tableName, pk);
        checkRowContainsColumn(result.getRow(), columnName, cv);

        // update and delete the column
        RowUpdateChange rowUpdate = new RowUpdateChange(tableName, pk);
        rowUpdate.deleteColumns(columnName);
        OTSHelper.updateRow(ots, rowUpdate);

        // get and check
        result = OTSHelper.getRowForAll(ots, tableName, pk);

        // update and put the same column
        rowUpdate = new RowUpdateChange(tableName, pk);
        rowUpdate.put(columnName, cv2);
        OTSHelper.updateRow(ots, rowUpdate);

        // get and check
        result = OTSHelper.getRowForAll(ots, tableName, pk);
        checkRowContainsColumn(result.getRow(), columnName, cv2);

        // batch write rows with update (put + delete)
        {
            List<RowUpdateChange> rowUpdateChanges = new ArrayList<RowUpdateChange>();
            rowUpdate = new RowUpdateChange(tableName, pk);
            rowUpdate.deleteColumns(columnName);
            rowUpdateChanges.add(rowUpdate);
            BatchWriteRowResponse bwr = OTSHelper.batchWriteRow(ots, null, rowUpdateChanges, null);
            assertEquals(bwr.getFailedRows().size(), 0);
            // get and check
            result = OTSHelper.getRowForAll(ots, tableName, pk);

            rowUpdateChanges.clear();
            rowUpdate = new RowUpdateChange(tableName, pk);
            rowUpdate.put(columnName, cv2);
            rowUpdateChanges.add(rowUpdate);
            bwr = OTSHelper.batchWriteRow(ots, null, rowUpdateChanges, null);
            assertEquals(bwr.getFailedRows().size(), 0);

            // get and check
            result = OTSHelper.getRowForAll(ots, tableName, pk);
            checkRowContainsColumn(result.getRow(), columnName, cv2);
        }

        // batch write rows with put
        int start = 1111;
        int end = 1121;
        List<PrimaryKey> pks = new ArrayList<PrimaryKey>();
        for (int i = start; i < end; i++) {
            pks.add(PrimaryKeyBuilder.createPrimaryKeyBuilder().addPrimaryKeyColumn(
                            pk.getPrimaryKeyColumn(0).getName(), PrimaryKeyValue.fromString(Integer.toString(i))).build());
        }

        {
            List<RowPutChange> rowPutChanges = new ArrayList<RowPutChange>();
            for (PrimaryKey tmp : pks) {
                RowPutChange rowChange = new RowPutChange(tableName, tmp);
                rowChange.addColumn(columnName, cv3);
                rowPutChanges.add(rowChange);
            }
            BatchWriteRowResponse bwr = OTSHelper.batchWriteRow(ots, rowPutChanges, null, null);
            assertEquals(bwr.getFailedRows().size(), 0);
        }

        // batch get rows and check
        {
            MultiRowQueryCriteria mrc = new MultiRowQueryCriteria(tableName);
            for (PrimaryKey tmp : pks) {
                mrc.addRow(tmp);
            }
            mrc.addColumnsToGet(columnName);
            mrc.setMaxVersions(1000);

            BatchGetRowRequest bgr = new BatchGetRowRequest();
            bgr.addMultiRowQueryCriteria(mrc);
            BatchGetRowResponse bgrr = ots.batchGetRow(bgr);
            assertEquals(bgrr.getFailedRows().size(), 0);
            List<RowResult> rs = bgrr.getSucceedRows();
            for (int i = 0; i < rs.size(); i++) {
                assertEquals(rs.get(i).getRow().getPrimaryKey(), pks.get(i));
                checkRowContainsColumn(rs.get(i).getRow(), columnName, cv3);
            }
        }

        // get range and check
        {
            RangeRowQueryCriteria rrc = new RangeRowQueryCriteria(tableName);
            rrc.setInclusiveStartPrimaryKey(PrimaryKeyBuilder.createPrimaryKeyBuilder().addPrimaryKeyColumn(
                    pk.getPrimaryKeyColumn(0).getName(), PrimaryKeyValue.fromString(Integer.toString(start))).build());
            rrc.setExclusiveEndPrimaryKey(PrimaryKeyBuilder.createPrimaryKeyBuilder().addPrimaryKeyColumn(
                    pk.getPrimaryKeyColumn(0).getName(), PrimaryKeyValue.fromString(Integer.toString(end))).build());
            rrc.setMaxVersions(1000);
            rrc.addColumnsToGet(columnName);

            GetRangeResponse grr = OTSHelper.getRange(ots, rrc);
            assertEquals(grr.getRows().size(), end - start);
            for (int i = 0; i < end - start; i++) {
                assertEquals(grr.getRows().get(i).getPrimaryKey(), pks.get(i));
                checkRowContainsColumn(grr.getRows().get(i), columnName, cv3);
            }
        }
    }

    private void checkRowContainsColumn(Row row, String columnName, ColumnValue cv) {
        assertTrue(row != null);
        List<Column> cs = row.getColumn(columnName);
        assertTrue(!cs.isEmpty());
        assertEquals(cs.size(), 1);
        assertEquals(cs.get(0).getValue(), cv);
    }

    /**
     * 测试使用VT_INF_MIN或VT_INF_MAX作为PK操作，期望得到OTSParameterInvalid.
     */
    @Test
    public void testInvalidPrimaryKey() {
        // createTable
        createTable(3);

        List<PrimaryKey> pks = new ArrayList<PrimaryKey>();
        List<PrimaryKeyColumn> primaryKey = new ArrayList<PrimaryKeyColumn>();
        primaryKey.add(new PrimaryKeyColumn("pk", PrimaryKeyValue.INF_MIN));
        pks.add(new PrimaryKey(primaryKey));

        primaryKey = new ArrayList<PrimaryKeyColumn>();
        primaryKey.add(new PrimaryKeyColumn("pk", PrimaryKeyValue.INF_MAX));
        pks.add(new PrimaryKey(primaryKey));

        {
            PrimaryKey pk = pks.get(0);
            String columnName = "col_name";

            TableStoreException expect = new TableStoreException("VT_INF_MIN is an invalid type for the primary key.", null, ErrorCode.INVALID_PARAMETER, "", 400);
            checkPutRow(ots, tableName, pk, columnName, expect);

            TableStoreException expect1 = new TableStoreException("VT_INF_MIN is an invalid type for the primary key.", null, ErrorCode.INVALID_PARAMETER, "", 400);
            checkGetRow(ots, tableName, pk, columnName, expect1);

            expect = new TableStoreException("VT_INF_MIN is an invalid type for the primary key.", null, ErrorCode.INVALID_PARAMETER, "", 400);
            checkUpdateRow(ots, tableName, pk, columnName, expect);

            expect = new TableStoreException("VT_INF_MIN is an invalid type for the primary key.", null, ErrorCode.INVALID_PARAMETER, "", 400);
            checkDeleteRow(ots, tableName, pk, expect);

            checkBatchGetRow(ots, tableName, pk, columnName, expect1);

            expect = new TableStoreException("VT_INF_MIN is an invalid type for the primary key.", null, ErrorCode.INVALID_PARAMETER, "", 400);
            checkBatchWriteRowForException(ots, tableName, pk, columnName, expect);
        }

        {
            PrimaryKey pk = pks.get(1);
            String columnName = "col_name";

            TableStoreException expect = new TableStoreException("VT_INF_MAX is an invalid type for the primary key.", null, ErrorCode.INVALID_PARAMETER, "", 400);
            checkPutRow(ots, tableName, pk, columnName, expect);

            TableStoreException expect1 = new TableStoreException("VT_INF_MAX is an invalid type for the primary key.", null, ErrorCode.INVALID_PARAMETER, "", 400);
            checkGetRow(ots, tableName, pk, columnName, expect1);

            expect = new TableStoreException("VT_INF_MAX is an invalid type for the primary key.", null, ErrorCode.INVALID_PARAMETER, "", 400);
            checkUpdateRow(ots, tableName, pk, columnName, expect);

            expect = new TableStoreException("VT_INF_MAX is an invalid type for the primary key.", null, ErrorCode.INVALID_PARAMETER, "", 400);
            checkDeleteRow(ots, tableName, pk, expect);

            checkBatchGetRow(ots, tableName, pk, columnName, expect1);

            expect = new TableStoreException("VT_INF_MAX is an invalid type for the primary key.", null, ErrorCode.INVALID_PARAMETER, "", 400);
            checkBatchWriteRowForException(ots, tableName, pk, columnName, expect);
        }
    }

    /**
     * 创建一个表，包含3个主键列，类型为INTEGER, STRING, BINARY。
     * GetRange操作，start pk 与 end pk相等，期望返回 OTSParameterInvalid
     */
    @Test
    public void testGetRangeWhenStartPkEqualsEndPk() {
        // createTable
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("pk_0", PrimaryKeyType.INTEGER);
        tableMeta.addPrimaryKeyColumn("pk_1", PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn("pk_2", PrimaryKeyType.BINARY);
        OTSHelper.createTable(ots, tableMeta, new CapacityUnit(0, 0), null);

        Utils.waitForPartitionLoad(tableName);

        List<PrimaryKeyColumn> primaryKeyColumns = new ArrayList<PrimaryKeyColumn>();
        primaryKeyColumns.add(new PrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(10)));
        primaryKeyColumns.add(new PrimaryKeyColumn("pk_1", PrimaryKeyValue.fromString("pk")));
        primaryKeyColumns.add(new PrimaryKeyColumn("pk_2", PrimaryKeyValue.fromBinary("pk".getBytes())));

        PrimaryKey primaryKey = new PrimaryKey(primaryKeyColumns);

        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(primaryKey);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(primaryKey);
        rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
        try {
            OTSHelper.getRange(ots, rangeRowQueryCriteria);
            fail();
        } catch (TableStoreException ex) {
            TableStoreException expect = new TableStoreException("Begin key must less than end key in FORWARD", null, ErrorCode.INVALID_PARAMETER, "", 400);
            assertTableStoreException(expect, ex);
        }
    }

    /**
     * 创建一个表，包含4个主键列，分别为 PK0 INTEGER, PK1 INTEGER, PK2 STRING, PK3 BINARY。
     * 测试所有数据操作API，分别用以下的PK顺序和类型：
     * a) PK1 INTEGER, PK0 INTEGER, PK2 STRING, PK3 BINARY
     * b) PK0 INTEGER, PK2 INTEGER, PK1 STRING, PK3 BINARY
     * c) PK0 INTEGER, PK2 STRING, PK1 INTEGER, PK3 BINARY
     * d) PK0 BINARY, PK1 INTEGER, PK2 STRING, PK3 BINARY
     * e) PK3 BINARY, PK1 INTEGER, PK2 STRING, PK0 INTEGER。期望返回 OTSParameterInvalid
     */
    @Test
    public void testInvalidPkOrder() {

        // createTable
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("pk_0", PrimaryKeyType.INTEGER);
        tableMeta.addPrimaryKeyColumn("pk_1", PrimaryKeyType.INTEGER);
        tableMeta.addPrimaryKeyColumn("pk_2", PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn("pk_3", PrimaryKeyType.BINARY);
        OTSHelper.createTable(ots, tableMeta, new CapacityUnit(0, 0), null);

        Utils.waitForPartitionLoad(tableName);

        List<PrimaryKeyColumn> primaryKeyColumns = new ArrayList<PrimaryKeyColumn>();
        primaryKeyColumns.add(new PrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(100)));
        primaryKeyColumns.add(new PrimaryKeyColumn("pk_1", PrimaryKeyValue.fromLong(0)));
        primaryKeyColumns.add(new PrimaryKeyColumn("pk_2", PrimaryKeyValue.fromString("pk")));
        primaryKeyColumns.add(new PrimaryKeyColumn("pk_3", PrimaryKeyValue.fromBinary("pk".getBytes())));
        PrimaryKey primaryKeyEnd = new PrimaryKey(primaryKeyColumns);

        List<PrimaryKey> primaryKeys = new ArrayList<PrimaryKey>();
	List<String> errorMsgs = new ArrayList<String>();

        // a) PK1 INTEGER, PK0 INTEGER, PK2 STRING, PK3 BINARY
        primaryKeyColumns = new ArrayList<PrimaryKeyColumn>();
        primaryKeyColumns.add(new PrimaryKeyColumn("pk_1", PrimaryKeyValue.fromLong(1)));
        primaryKeyColumns.add(new PrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(0)));
        primaryKeyColumns.add(new PrimaryKeyColumn("pk_2", PrimaryKeyValue.fromString("")));
        primaryKeyColumns.add(new PrimaryKeyColumn("pk_3", PrimaryKeyValue.fromBinary("pk".getBytes())));

        primaryKeys.add(new PrimaryKey(primaryKeyColumns));
	errorMsgs.add("Validate PK name fail. Input: pk_1, Meta: pk_0.");

        // b) PK0 INTEGER, PK2 INTEGER, PK1 STRING, PK3 BINARY
        primaryKeyColumns = new ArrayList<PrimaryKeyColumn>();
        primaryKeyColumns.add(new PrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(1)));
        primaryKeyColumns.add(new PrimaryKeyColumn("pk_2", PrimaryKeyValue.fromLong(2)));
        primaryKeyColumns.add(new PrimaryKeyColumn("pk_1", PrimaryKeyValue.fromString("")));
        primaryKeyColumns.add(new PrimaryKeyColumn("pk_3", PrimaryKeyValue.fromBinary("".getBytes())));

        primaryKeys.add(new PrimaryKey(primaryKeyColumns));
	errorMsgs.add("Validate PK name fail. Input: pk_2, Meta: pk_1.");

        // c) PK0 INTEGER, PK2 STRING, PK1 INTEGER, PK3 BINARY
        primaryKeyColumns = new ArrayList<PrimaryKeyColumn>();
        primaryKeyColumns.add(new PrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(0)));
        primaryKeyColumns.add(new PrimaryKeyColumn("pk_2", PrimaryKeyValue.fromString("")));
        primaryKeyColumns.add(new PrimaryKeyColumn("pk_1", PrimaryKeyValue.fromLong(1)));
        primaryKeyColumns.add(new PrimaryKeyColumn("pk_3", PrimaryKeyValue.fromBinary("".getBytes())));

        primaryKeys.add(new PrimaryKey(primaryKeyColumns));
	errorMsgs.add("Validate PK name fail. Input: pk_2, Meta: pk_1.");

        // d) PK0 BINARY, PK1 INTEGER, PK2 STRING, PK3 BINARY
        primaryKeyColumns = new ArrayList<PrimaryKeyColumn>();
        primaryKeyColumns.add(new PrimaryKeyColumn("pk_0", PrimaryKeyValue.fromBinary("".getBytes())));
        primaryKeyColumns.add(new PrimaryKeyColumn("pk_1", PrimaryKeyValue.fromLong(1)));
        primaryKeyColumns.add(new PrimaryKeyColumn("pk_2", PrimaryKeyValue.fromString("")));
        primaryKeyColumns.add(new PrimaryKeyColumn("pk_3", PrimaryKeyValue.fromBinary("".getBytes())));

        primaryKeys.add(new PrimaryKey(primaryKeyColumns));
	errorMsgs.add("Validate PK type fail. Input: VT_BLOB, Meta: VT_INTEGER.");

        // e) PK3 BINARY, PK1 INTEGER, PK2 STRING, PK0 INTEGER。
        primaryKeyColumns = new ArrayList<PrimaryKeyColumn>();
        primaryKeyColumns.add(new PrimaryKeyColumn("pk_3", PrimaryKeyValue.fromBinary("".getBytes())));
        primaryKeyColumns.add(new PrimaryKeyColumn("pk_1", PrimaryKeyValue.fromLong(1)));
        primaryKeyColumns.add(new PrimaryKeyColumn("pk_2", PrimaryKeyValue.fromString("")));
        primaryKeyColumns.add(new PrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(1)));

        primaryKeys.add(new PrimaryKey(primaryKeyColumns));
	errorMsgs.add("Validate PK name fail. Input: pk_3, Meta: pk_0.");

	int index = 0;
        for (PrimaryKey primaryKey : primaryKeys) {
	    TableStoreException expect = new TableStoreException(errorMsgs.get(index++), null, ErrorCode.INVALID_PK, "", 400);
            checkPutRow(ots, tableName, primaryKey, "col_name", expect);

            checkGetRow(ots, tableName, primaryKey, "col_name", expect);

            checkUpdateRow(ots, tableName, primaryKey, "col_name", expect);

            checkDeleteRow(ots, tableName, primaryKey, expect);

            checkBatchGetRowForException(ots, tableName, primaryKey, "col_name", expect);

            checkBatchWriteRow(ots, tableName, primaryKey, "col_name", expect);

            RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);

            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(primaryKey);
            rangeRowQueryCriteria.setExclusiveEndPrimaryKey(primaryKeyEnd);
            rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
            checkGetRange(ots, rangeRowQueryCriteria, expect);
        }
    }

    /**
     * 创建一张表，不指定table options中的TLL时期望SDK抛错或者返回OTSParameterInvalid
     * 创建一张表，不指定table options的MaxVersion时期望SDK抛错或者返回OTSParameterInvalid
     */
    @Test
    public void testNotSetTTLOrNotSetMaxVersions() {
        List<PrimaryKeySchema> pks = new ArrayList<PrimaryKeySchema>();
        pks.add(new PrimaryKeySchema("pk", PrimaryKeyType.STRING));
        TableMeta meta = new TableMeta(tableName);
        meta.addPrimaryKeyColumns(pks);
        TableOptions tableOptions = new TableOptions();
        tableOptions.setMaxVersions(3);
        try {
            OTSHelper.createTable(ots, meta, new CapacityUnit(0, 0),
                    tableOptions);
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("The maxVersions and timeToLive must be set while creating table.", ex.getMessage());
        }

        pks = new ArrayList<PrimaryKeySchema>();
        pks.add(new PrimaryKeySchema("pk", PrimaryKeyType.STRING));
        meta = new TableMeta(tableName);
        meta.addPrimaryKeyColumns(pks);
        tableOptions = new TableOptions();
        tableOptions.setTimeToLive(-1);
        try {
            OTSHelper.createTable(ots, meta, new CapacityUnit(0, 0),
                    tableOptions);
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("The maxVersions and timeToLive must be set while creating table.", ex.getMessage());
        }
    }

    /**
     * GetRow/GetRange/BatchGetRow中，不指定version相关参数时，返回错误。
     */
    @Test
    public void testNotSetVersionOptionsInGetOperations() {
        // createTable
        createTable(3);

        TableStoreException expect = new TableStoreException("No version condition is specified.", null, ErrorCode.INVALID_PARAMETER, "", 400);

        // getRow
        SingleRowQueryCriteria rowQueryCriteria = new SingleRowQueryCriteria(tableName, getPrimaryKey());
        try {
            OTSHelper.getRow(ots, rowQueryCriteria);
            fail();
        } catch (TableStoreException ex) {
            assertTableStoreException(expect, ex);
        }

        // getRange
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(getPrimaryKey(""));
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(getPrimaryKey("A"));
        try {
            OTSHelper.getRange(ots, rangeRowQueryCriteria);
            fail();
        } catch (TableStoreException ex) {
            assertTableStoreException(expect, ex);
        }

        // batchGetRow
        List<MultiRowQueryCriteria> multiRowQueryCriterias = new ArrayList<MultiRowQueryCriteria>();
        MultiRowQueryCriteria multiRowQueryCriteria = new MultiRowQueryCriteria(tableName);
        multiRowQueryCriteria.addRow(getPrimaryKey());
        multiRowQueryCriterias.add(multiRowQueryCriteria);
        try {
            OTSHelper.batchGetRow(ots, multiRowQueryCriterias);
            fail();
        } catch (TableStoreException ex) {
            expect = new TableStoreException("No version condition is specified while querying row.", null, ErrorCode.INVALID_PARAMETER, "", 400);
            assertTableStoreException(expect, ex);
        }
    }
}
