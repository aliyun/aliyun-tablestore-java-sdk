package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.common.*;
import com.alicloud.openservices.tablestore.common.Utils;
import com.alicloud.openservices.tablestore.model.filter.CompositeColumnValueFilter;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;
import com.google.gson.JsonSyntaxException;

import org.junit.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UnifiedFilterAdvanceTest extends BaseFT {
    private static Logger LOG = Logger.getLogger(UnifiedFilterAdvanceTest.class.getName());

    private static String tableName = "FilterAdvanceFunctionTest";
    private static SyncClientInterface ots;

    private static final int SECONDS_UNTIL_TABLE_READY = 10;

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

    public void checkReadRangeResponse(long expectRowNum, Direction direction) throws Exception {

        // expected Response
        List<Row> expectRows = new ArrayList<Row>();
        for (int i = 0; i < expectRowNum; i++) {
        	PrimaryKeyColumn[] pk = new PrimaryKeyColumn[1];
            if (direction == Direction.FORWARD) {
                pk[0] = new PrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(i));
            } else {
                pk[0] = new PrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(expectRowNum-i-1));
            }
            Column[] cols = new Column[10];
            for (int j = 0; j < 10; j++) {
                cols[j] = new Column("col" + j, ColumnValue.fromLong(j), 1);
            }
        	PrimaryKey primaryKey = new PrimaryKey(pk);
        	Row row = new Row(primaryKey, cols);
            expectRows.add(row);
        }

        SingleColumnValueFilter filter =
                new SingleColumnValueFilter("pk", SingleColumnValueFilter.CompareOperator.LESS_THAN, ColumnValue.fromLong(expectRowNum));

        ArrayList<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
        pks.add(new PrimaryKeyColumn("pk", PrimaryKeyValue.INF_MIN));
        PrimaryKey begin = new PrimaryKey(pks);
        pks.clear();
        pks.add(new PrimaryKeyColumn("pk", PrimaryKeyValue.INF_MAX));
        PrimaryKey end = new PrimaryKey(pks);
        PrimaryKey nextKey = null;

        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);

        if (direction == Direction.FORWARD) {
            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(begin);
            rangeRowQueryCriteria.setExclusiveEndPrimaryKey(end);
            nextKey = begin;
        } else {
            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(end);
            rangeRowQueryCriteria.setExclusiveEndPrimaryKey(begin);
            nextKey = end;
        }
        rangeRowQueryCriteria.setDirection(direction);
        rangeRowQueryCriteria.setFilter(filter);
        rangeRowQueryCriteria.setMaxVersions(1);

        List<Row> allRows = new ArrayList<Row>();
        while (nextKey != null) {
            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(nextKey);
            GetRangeResponse Response = OTSHelper.getRange(ots, rangeRowQueryCriteria);
            nextKey = Response.getNextStartPrimaryKey();
            allRows.addAll(Response.getRows());
        }

        assertEquals(expectRowNum, allRows.size());
        for (int i = 0; i < allRows.size(); i++) {
            checkRowNoTimestamp(expectRows.get(i), allRows.get(i));
        }
    }

    /**
     * 构造GetRange读取1万行，同一个partition，返回0行，1行，4999行，5001行，9999行，10000行的情况。
     * 所有的情况必须都是2次返回，区分正向和反向两种情况。
     * @throws Exception
     */
    @Test
    public void testReadRangeFilterRows() throws Exception {
        // createTable
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("pk", PrimaryKeyType.INTEGER);

        OTSHelper.createTable(ots, tableMeta);
        Utils.sleepSeconds(SECONDS_UNTIL_TABLE_READY);

        // put 10000 rows * 10 cols * 1 version
        int kRowCount = 10000;
        int kColumnCount = 10;
        Map<String, ColumnValue> columns = new HashMap<String, ColumnValue>();
        for (int i = 0; i < kColumnCount; i++) {
            columns.put("col" + i, ColumnValue.fromLong(i));
        }
        ArrayList<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
        for (int i = 0; i < kRowCount; i++) {
        	pks.clear();
            pks.add(new PrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(i)));
            PrimaryKey pk = new PrimaryKey(pks);
            OTSHelper.putRow(ots, tableName, pk, columns);
        }

        // 正向，通过filter读出的行数
        checkReadRangeResponse(0, Direction.FORWARD);
        checkReadRangeResponse(1, Direction.FORWARD);
        checkReadRangeResponse(4999, Direction.FORWARD);
        checkReadRangeResponse(5000, Direction.FORWARD);
        checkReadRangeResponse(5001, Direction.FORWARD);
        checkReadRangeResponse(9999, Direction.FORWARD);
        checkReadRangeResponse(10000, Direction.FORWARD);

        // 反向，通过filter读出的行数
        checkReadRangeResponse(0, Direction.BACKWARD);
        checkReadRangeResponse(1, Direction.BACKWARD);
        checkReadRangeResponse(4999, Direction.BACKWARD);
        checkReadRangeResponse(5000, Direction.BACKWARD);
        checkReadRangeResponse(5001, Direction.BACKWARD);
        checkReadRangeResponse(9999, Direction.BACKWARD);
        checkReadRangeResponse(10000, Direction.BACKWARD);
    }

    enum FilterIfMissingType
    {
        FT_NONE,
        FT_TRUE,
        FT_FALSE
    }

    public void checkFilterIfMissing(int rowCount, FilterIfMissingType typeForExistCol, FilterIfMissingType typeForNotExistCol, boolean expectResponse)
    {
        // columns to get
        List<String> columnNames = new ArrayList<String>();
        columnNames.add("pk");
        columnNames.add("col_exist");
        columnNames.add("col_not_exist");

        // (col == 999) AND (col_not_exist == 999)
        SingleColumnValueFilter filterForExistCol = new SingleColumnValueFilter("col_exist",
                SingleColumnValueFilter.CompareOperator.EQUAL, ColumnValue.fromLong(999));
        SingleColumnValueFilter filterForNotExistCol = new SingleColumnValueFilter("col_not_exist",
                SingleColumnValueFilter.CompareOperator.EQUAL, ColumnValue.fromLong(999));
        if (typeForExistCol == FilterIfMissingType.FT_TRUE) {
            filterForExistCol.setPassIfMissing(false);
        } else if (typeForExistCol == FilterIfMissingType.FT_FALSE) {
            filterForExistCol.setPassIfMissing(true);
        }
        if (typeForNotExistCol == FilterIfMissingType.FT_TRUE) {
            filterForNotExistCol.setPassIfMissing(false);
        } else if (typeForNotExistCol == FilterIfMissingType.FT_FALSE) {
            filterForNotExistCol.setPassIfMissing(true);
        }
        CompositeColumnValueFilter filter = new CompositeColumnValueFilter(CompositeColumnValueFilter.LogicOperator.AND);
        filter.addFilter(filterForExistCol).addFilter(filterForNotExistCol);
        
        ArrayList<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
        // 测试GetRow
        {
            for (int i = 0; i < rowCount; i++) {
            	pks.clear();
            	pks.add(new PrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(i)));
                PrimaryKey pk = new PrimaryKey(pks);

                SingleRowQueryCriteria rowQueryCriteria = new SingleRowQueryCriteria(tableName);
                rowQueryCriteria.setPrimaryKey(pk);
                rowQueryCriteria.addColumnsToGet(columnNames);
                rowQueryCriteria.setFilter(filter);
                rowQueryCriteria.setMaxVersions(1);
                GetRowRequest getRowRequest = new GetRowRequest(rowQueryCriteria);
                GetRowResponse Response = ots.getRow(getRowRequest);
                Row row = Response.getRow();
                if (expectResponse) {
                    assertTrue(row != null);
                    assertEquals(1, row.getColumns().length);
                    assertEquals(i, row.getPrimaryKey().getPrimaryKeyColumn(0).getValue().asLong());
                    assertEquals(999, row.getColumn("col_exist").get(0).getValue().asLong());
                } else {
                    checkRow(null, row);
                }
            }
        }

        // 测试BatchGetRow
        {
            List<PrimaryKey> primaryKeys = new ArrayList<PrimaryKey>();
            for (int i = 0; i < rowCount; i++) {
            	pks.clear();
            	pks.add(new PrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(i)));
                primaryKeys.add(new PrimaryKey(pks));
            }
            List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
            MultiRowQueryCriteria multiRowQueryCriteria = new MultiRowQueryCriteria(tableName);
            multiRowQueryCriteria.setRowKeys(primaryKeys);
            multiRowQueryCriteria.addColumnsToGet(columnNames);
            multiRowQueryCriteria.setFilter(filter);
            multiRowQueryCriteria.setMaxVersions(1);
            criterias.add(multiRowQueryCriteria);
            List<BatchGetRowResponse.RowResult> Response = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
            assertEquals(Response.size(), rowCount);
            for (int i = 0; i < Response.size(); i++) {
                BatchGetRowResponse.RowResult rowResponse = Response.get(i);
                Row row = rowResponse.getRow();
                assertTrue(rowResponse.isSucceed());
                if (expectResponse) {
                    assertTrue(row != null);
                    assertEquals(1, row.getColumns().length);
                    assertEquals(i, row.getPrimaryKey().getPrimaryKeyColumn(0).getValue().asLong());
                    assertEquals(999, row.getColumn("col_exist").get(0).getValue().asLong());
                } else {
                    checkRow(null, row);
                }
            }
        }

        // 测试GetRange
        {
        	pks.clear();
        	pks.add(new PrimaryKeyColumn("pk", PrimaryKeyValue.INF_MIN));
            PrimaryKey beginKey = new PrimaryKey(pks);
        	pks.clear();
        	pks.add(new PrimaryKeyColumn("pk", PrimaryKeyValue.INF_MAX));
            PrimaryKey endKey = new PrimaryKey(pks);

            RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(beginKey);
            rangeRowQueryCriteria.setExclusiveEndPrimaryKey(endKey);
            rangeRowQueryCriteria.setFilter(filter);
            rangeRowQueryCriteria.setMaxVersions(1);
            GetRangeResponse Response = OTSHelper.getRange(ots, rangeRowQueryCriteria);
            List<Row> rows = Response.getRows();
            if (expectResponse) {
                assertEquals(rows.size(), rowCount);
                for (int i = 0; i < rowCount; i++) {
                    Row row = rows.get(i);
                    assertTrue(row != null);
                    assertEquals(1, row.getColumns().length);
                    assertEquals(i, row.getPrimaryKey().getPrimaryKeyColumn(0).getValue().asLong());
                    assertEquals(999, row.getColumn("col_exist").get(0).getValue().asLong());
                }
            } else {
                assertEquals(rows.size(), 0);
            }
        }
    }

    /**
     * 构造GetRow/GetRange/BatchGetRow，包含2个列，各1个filter。构造10个行，每行只有1列。
     * 分别测试各请求的返回，在FilterIfMissing为默认值、True、False的情况。
     * @throws Exception
     */
    @Test
    public void testFilterIfMissing() throws Exception {
        // createTable
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("pk", PrimaryKeyType.INTEGER);

        OTSHelper.createTable(ots, tableMeta);
        Utils.sleepSeconds(SECONDS_UNTIL_TABLE_READY);

        // put 10 rows * 1 cols
        Map<String, ColumnValue> columns = new HashMap<String, ColumnValue>();
        columns.put("col_exist", ColumnValue.fromLong(999));

        ArrayList<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
        for (int i = 0; i < 10; i++) {
        	pks.clear();
        	pks.add(new PrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(i)));
            PrimaryKey pk = new PrimaryKey(pks);
            OTSHelper.putRow(ots, tableName, pk, columns);
        }

        // 测试GetRow/BatchGet/ReadRange，FilterIfMissing都为默认值，可以读到数据
        checkFilterIfMissing(10, FilterIfMissingType.FT_NONE, FilterIfMissingType.FT_NONE, true);

        // 测试GetRow/BatchGet/ReadRange，filterIfMissingForExistCol: true, filterIfMissingForNotExistCol: true，读不到数据
        checkFilterIfMissing(10, FilterIfMissingType.FT_TRUE, FilterIfMissingType.FT_TRUE, false);

        // 测试GetRow/BatchGet/ReadRange，filterIfMissingForExistCol: false, filterIfMissingForNotExistCol: true，读不到数据
        checkFilterIfMissing(10, FilterIfMissingType.FT_FALSE, FilterIfMissingType.FT_TRUE, false);

        // 测试GetRow/BatchGet/ReadRange，filterIfMissingForExistCol: true, filterIfMissingForNotExistCol: false，可以读到数据
        checkFilterIfMissing(10, FilterIfMissingType.FT_TRUE, FilterIfMissingType.FT_FALSE, true);

        // 测试GetRow/BatchGet/ReadRange，filterIfMissingForExistCol: false, filterIfMissingForNotExistCol: false，可以读到数据
        checkFilterIfMissing(10, FilterIfMissingType.FT_FALSE, FilterIfMissingType.FT_FALSE, true);
    }

    /**
     * 测试1000行，其中500行包含列128个列，另外500行也包含128个列，但最后一列列名不同为A。
     * 测试GetRange在FilterIfMissing为True或False的情况。
     * @throws Exception
     */
    @Test
    public void testFilterIfMissing1000Rows() throws Exception {
        // createTable
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("pk", PrimaryKeyType.INTEGER);

        OTSHelper.createTable(ots, tableMeta);
        Utils.sleepSeconds(SECONDS_UNTIL_TABLE_READY);

        List<Row> expectRows1 = new ArrayList<Row>();
        List<Row> expectRows2 = new ArrayList<Row>();

        ArrayList<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
        // put 1000 rows
        for (int i = 0; i < 500; i++) {
            Map<String, ColumnValue> columns = new HashMap<String, ColumnValue>();
            pks.clear();
            pks.add(new PrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(i)));
            PrimaryKey pk = new PrimaryKey(pks);
            Column[] cols = new Column[128];
            for (int j = 0; j < 128; j++) {
                cols[j] = new Column("col" + j, ColumnValue.fromLong(999), 1);
                columns.put("col" + j, ColumnValue.fromLong(999));
            }
            Row row = new Row(pk, cols);
            OTSHelper.putRow(ots, tableName, pk, columns);
            expectRows1.add(row);
            expectRows2.add(row);
        }
        for (int i = 500; i < 1000; i++) {
            Map<String, ColumnValue> columns = new HashMap<String, ColumnValue>();
            pks.clear();
            pks.add(new PrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(i)));
            PrimaryKey pk = new PrimaryKey(pks);
            Column[] cols = new Column[128];
            for (int j = 0; j < 127; j++) {
                cols[j] = new Column("col" + j, ColumnValue.fromLong(999), 1);
                columns.put("col" + j, ColumnValue.fromLong(999));
            }
            cols[127] = new Column("col_unique", ColumnValue.fromLong(999));
            columns.put("col_unique", ColumnValue.fromLong(999));
            Row row = new Row(pk, cols);
            OTSHelper.putRow(ots, tableName, pk, columns);
            expectRows2.add(row);
        }

        // columns to get
        List<String> columnNames = new ArrayList<String>();
        for (int i = 0; i < 128; i++) {
            columnNames.add("col" + Integer.toString(i));
        }

        // 测试GetRange，FilterIfMissing = true
        SingleColumnValueFilter filter = new SingleColumnValueFilter("col127",
                SingleColumnValueFilter.CompareOperator.EQUAL, ColumnValue.fromLong(999));
        filter.setPassIfMissing(false);

        // for GetRange
        pks.clear();
        pks.add(new PrimaryKeyColumn("pk", PrimaryKeyValue.INF_MIN));
        PrimaryKey beginKey = new PrimaryKey(pks);
        pks.clear();
        pks.add(new PrimaryKeyColumn("pk", PrimaryKeyValue.INF_MAX));
        PrimaryKey endKey = new PrimaryKey(pks);

        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(beginKey);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(endKey);
        rangeRowQueryCriteria.setFilter(filter);
        rangeRowQueryCriteria.setMaxVersions(1);

        List<Row> allRows = new ArrayList<Row>();
        PrimaryKey nextKey = beginKey;
        while (nextKey != null) {
            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(nextKey);
            GetRangeResponse Response = OTSHelper.getRange(ots, rangeRowQueryCriteria);
            nextKey = Response.getNextStartPrimaryKey();
            allRows.addAll(Response.getRows());
        }
        assertEquals(500, allRows.size());
        for (int i = 0; i < allRows.size(); i++) {
            Row row = allRows.get(i);
            checkRowNoTimestamp(expectRows1.get(i), row);
        }

        // 测试GetRange，FilterIfMissing = false
        filter.setPassIfMissing(true);
        rangeRowQueryCriteria.setFilter(filter);

        allRows.clear();
        nextKey = beginKey;
        while (nextKey != null) {
            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(nextKey);
            GetRangeResponse Response = OTSHelper.getRange(ots, rangeRowQueryCriteria);
            nextKey = Response.getNextStartPrimaryKey();
            allRows.addAll(Response.getRows());
        }
        assertEquals(1000, allRows.size());
        for (int i = 0; i < allRows.size(); i++) {
            Row row = allRows.get(i);
            checkRowNoTimestamp(expectRows2.get(i), row);
        }
    }

    /**
     * 创建1个表，插入1行，包含4个列，列值均为 BINARY('ABC')或者BINARY('XXX')，
     * 列名分别为['C0', 'C1', 'C2', 'C3']。测试filter: C0 > 'AAA' OR C1 < 'ABD' OR NOT C2 = 'XXX' AND C3 = 'ABC'
     * @throws Exception
     */
    @Test
    public void testCompoundFilter() throws Exception {
        // createTable
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("pk", PrimaryKeyType.INTEGER);

        OTSHelper.createTable(ots, tableMeta);
        Utils.sleepSeconds(SECONDS_UNTIL_TABLE_READY);
        ColumnValue abcValue = ColumnValue.fromBinary("ABC".getBytes());

        ArrayList<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
        {
            // put 1 rows * 4 cols
            int kColumnCount = 4;
            Map<String, ColumnValue> columns = new HashMap<String, ColumnValue>();
            pks.clear();
            pks.add(new PrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(0)));
            PrimaryKey pk = new PrimaryKey(pks);
            Column[] cols = new Column[kColumnCount];
            for (int j = 0; j < kColumnCount; j++) {
                columns.put("C" + Integer.toString(j), ColumnValue.fromBinary("ABC".getBytes()));
                cols[j] = new Column("C" + Integer.toString(j), ColumnValue.fromBinary("ABC".getBytes()));
            }
            Row row = new Row(pk, cols);
            OTSHelper.putRow(ots, tableName, pk, columns);

            // 测试GetRow，并设置以下filter：C0 > 'AAA' OR C1 < 'ABD' OR NOT C2 = 'XXX' AND C3 = 'ABC'
            CompositeColumnValueFilter subFilter1 = new CompositeColumnValueFilter(CompositeColumnValueFilter.LogicOperator.NOT);
            subFilter1.addFilter(new SingleColumnValueFilter("C2", SingleColumnValueFilter.CompareOperator.EQUAL,
                    ColumnValue.fromBinary("XXX".getBytes())));
            CompositeColumnValueFilter subFilter2 = new CompositeColumnValueFilter(CompositeColumnValueFilter.LogicOperator.AND);
            subFilter2.addFilter(subFilter1).addFilter(new SingleColumnValueFilter("C3",
                    SingleColumnValueFilter.CompareOperator.EQUAL, ColumnValue.fromBinary("ABC".getBytes())));
            CompositeColumnValueFilter filter = new CompositeColumnValueFilter(CompositeColumnValueFilter.LogicOperator.OR);
            filter.addFilter(new SingleColumnValueFilter("C0",
                    SingleColumnValueFilter.CompareOperator.GREATER_THAN, ColumnValue.fromBinary("AAA".getBytes())))
                    .addFilter(new SingleColumnValueFilter("C1",
                            SingleColumnValueFilter.CompareOperator.LESS_THAN, ColumnValue.fromBinary("ABD".getBytes())))
                    .addFilter(subFilter2);

            List<String> columnNames = new ArrayList<String>();
            columnNames.add("pk");
            for (int j = 0; j < kColumnCount; j++) {
                columnNames.add("C" + j);
            }
            SingleRowQueryCriteria rowQueryCriteria = new SingleRowQueryCriteria(tableName);
            rowQueryCriteria.setPrimaryKey(pk);
            rowQueryCriteria.addColumnsToGet(columnNames);
            rowQueryCriteria.setFilter(filter);
            rowQueryCriteria.setMaxVersions(1);
            GetRowRequest getRowRequest = new GetRowRequest(rowQueryCriteria);
            GetRowResponse Response = ots.getRow(getRowRequest);
            Row ResponseRow = Response.getRow();
            assertTrue(ResponseRow != null);
            checkRowNoTimestamp(row, ResponseRow);
        }

        {
            // put 1 rows * 4 cols
            int kColumnCount = 4;
            Map<String, ColumnValue> columns = new HashMap<String, ColumnValue>();
            pks.clear();
            pks.add(new PrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(1)));
            PrimaryKey pk = new PrimaryKey(pks);
            Column[] cols = new Column[kColumnCount];
            for (int j = 0; j < kColumnCount; j++) {
                columns.put("C" + Integer.toString(j), ColumnValue.fromBinary("XXX".getBytes()));
                cols[j] = new Column("C" + Integer.toString(j), ColumnValue.fromBinary("XXX".getBytes()));
            }
            Row row = new Row(pk, cols);
            OTSHelper.putRow(ots, tableName, pk, columns);

            // 测试GetRow，并设置以下filter：C0 > 'AAA' OR C1 < 'ABD' OR NOT C2 = 'XXX' AND C3 = 'ABC'
            CompositeColumnValueFilter subFilter1 = new CompositeColumnValueFilter(CompositeColumnValueFilter.LogicOperator.NOT);
            subFilter1.addFilter(new SingleColumnValueFilter("C2", SingleColumnValueFilter.CompareOperator.EQUAL,
                    ColumnValue.fromBinary("XXX".getBytes())));
            CompositeColumnValueFilter subFilter2 = new CompositeColumnValueFilter(CompositeColumnValueFilter.LogicOperator.AND);
            subFilter2.addFilter(subFilter1).addFilter(new SingleColumnValueFilter("C3",
                    SingleColumnValueFilter.CompareOperator.EQUAL, ColumnValue.fromBinary("ABC".getBytes())));
            CompositeColumnValueFilter filter = new CompositeColumnValueFilter(CompositeColumnValueFilter.LogicOperator.OR);
            filter.addFilter(new SingleColumnValueFilter("C0",
                    SingleColumnValueFilter.CompareOperator.GREATER_THAN, ColumnValue.fromBinary("AAA".getBytes())))
                    .addFilter(new SingleColumnValueFilter("C1",
                            SingleColumnValueFilter.CompareOperator.LESS_THAN, ColumnValue.fromBinary("ABD".getBytes())))
                    .addFilter(subFilter2);

            List<String> columnNames = new ArrayList<String>();
            columnNames.add("pk");
            for (int j = 0; j < kColumnCount; j++) {
                columnNames.add("C" + Integer.toString(j));
            }
            SingleRowQueryCriteria rowQueryCriteria = new SingleRowQueryCriteria(tableName);
            rowQueryCriteria.setPrimaryKey(pk);
            rowQueryCriteria.addColumnsToGet(columnNames);
            rowQueryCriteria.setFilter(filter);
            rowQueryCriteria.setMaxVersions(1);
            GetRowRequest getRowRequest = new GetRowRequest(rowQueryCriteria);
            GetRowResponse Response = ots.getRow(getRowRequest);
            Row ResponseRow = Response.getRow();
            assertTrue(ResponseRow != null);
            checkRowNoTimestamp(row, ResponseRow);
        }
    }
}
