package com.aliyun.openservices.ots.integration;

import com.aliyun.openservices.ots.ClientConfiguration;
import com.aliyun.openservices.ots.OTS;
import com.aliyun.openservices.ots.common.BaseFT;
import com.aliyun.openservices.ots.common.OTSHelper;
import com.aliyun.openservices.ots.common.Utils;
import com.aliyun.openservices.ots.model.*;
import com.aliyun.openservices.ots.model.condition.CompositeCondition;
import com.aliyun.openservices.ots.model.condition.RelationalCondition;
import com.aliyun.openservices.ots.utils.ServiceSettings;
import org.junit.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FilterAdvanceTest extends BaseFT {
    private static Logger LOG = Logger.getLogger(FilterAdvanceTest.class.getName());

    private static String tableName = "FilterAdvanceFunctionTest";

    private static final OTS ots = OTSClientFactory.createOTSClient(
            ServiceSettings.load(), new ClientConfiguration());

    private static final int SECONDS_UNTIL_TABLE_READY = 10;

    @AfterClass
    public static void classAfter() {
        ots.shutdown();
    }

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


    @After
    public void teardown() {

    }

    public void checkReadRangeResult(long expectRowNum, Direction direction) throws Exception {

        // expected result
        List<Row> expectRows = new ArrayList<Row>();
        for (int i = 0; i < expectRowNum; i++) {
            Row row = new Row();
            if (direction == Direction.FORWARD) {
                row.addColumn("pk", ColumnValue.fromLong(i));
            } else {
                row.addColumn("pk", ColumnValue.fromLong(expectRowNum-i-1));
            }
            for (int j = 0; j < 10; j++) {
                row.addColumn("col" + j, ColumnValue.fromLong(j));
            }
            expectRows.add(row);
        }

        RelationalCondition filter =
                new RelationalCondition("pk", RelationalCondition.CompareOperator.LESS_THAN, ColumnValue.fromLong(expectRowNum));

        RowPrimaryKey begin = new RowPrimaryKey().addPrimaryKeyColumn("pk", PrimaryKeyValue.INF_MIN);
        RowPrimaryKey end = new RowPrimaryKey().addPrimaryKeyColumn("pk", PrimaryKeyValue.INF_MAX);
        RowPrimaryKey nextKey = null;

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

        List<Row> allRows = new ArrayList<Row>();
        while (nextKey != null) {
            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(nextKey);
            GetRangeResult result = OTSHelper.getRange(ots, rangeRowQueryCriteria);
            nextKey = result.getNextStartPrimaryKey();
            allRows.addAll(result.getRows());
        }

        assertEquals(expectRowNum, allRows.size());
        for (int i = 0; i < allRows.size(); i++) {
            checkRow(expectRows.get(i), allRows.get(i));
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

        for (int i = 0; i < kRowCount; i++) {
            RowPrimaryKey pk = new RowPrimaryKey()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(i));
            OTSHelper.putRow(ots, tableName, pk, columns);
        }

        // 正向，通过filter读出的行数
        checkReadRangeResult(0, Direction.FORWARD);
        checkReadRangeResult(1, Direction.FORWARD);
        checkReadRangeResult(4999, Direction.FORWARD);
        checkReadRangeResult(5000, Direction.FORWARD);
        checkReadRangeResult(5001, Direction.FORWARD);
        checkReadRangeResult(9999, Direction.FORWARD);
        checkReadRangeResult(10000, Direction.FORWARD);

        // 反向，通过filter读出的行数
        checkReadRangeResult(0, Direction.BACKWARD);
        checkReadRangeResult(1, Direction.BACKWARD);
        checkReadRangeResult(4999, Direction.BACKWARD);
        checkReadRangeResult(5000, Direction.BACKWARD);
        checkReadRangeResult(5001, Direction.BACKWARD);
        checkReadRangeResult(9999, Direction.BACKWARD);
        checkReadRangeResult(10000, Direction.BACKWARD);
    }

    enum FilterIfMissingType
    {
        FT_NONE,
        FT_TRUE,
        FT_FALSE
    }

    public void checkFilterIfMissing(int rowCount, FilterIfMissingType typeForExistCol, FilterIfMissingType typeForNotExistCol, boolean expectResult)
    {
        // columns to get
        List<String> columnNames = new ArrayList<String>();
        columnNames.add("pk");
        columnNames.add("col_exist");
        columnNames.add("col_not_exist");

        // (col == 999) AND (col_not_exist == 999)
        RelationalCondition filterForExistCol = new RelationalCondition("col_exist",
                RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromLong(999));
        RelationalCondition filterForNotExistCol = new RelationalCondition("col_not_exist",
                RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromLong(999));
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
        CompositeCondition filter = new CompositeCondition(CompositeCondition.LogicOperator.AND);
        filter.addCondition(filterForExistCol).addCondition(filterForNotExistCol);

        // 测试GetRow
        {
            for (int i = 0; i < rowCount; i++) {
                RowPrimaryKey pk = new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(i));

                SingleRowQueryCriteria rowQueryCriteria = new SingleRowQueryCriteria(tableName);
                rowQueryCriteria.setPrimaryKey(pk);
                rowQueryCriteria.setColumnsToGet(columnNames);
                rowQueryCriteria.setFilter(filter);
                GetRowRequest getRowRequest = new GetRowRequest(rowQueryCriteria);
                GetRowResult result = ots.getRow(getRowRequest);
                Row row = result.getRow();
                if (expectResult) {
                    assertTrue(row != null);
                    assertEquals(2, row.getColumns().size());
                    assertEquals(i, row.getColumns().get("pk").asLong());
                    assertEquals(999, row.getColumns().get("col_exist").asLong());
                } else {
                    checkRow(new Row(), row);
                }
            }
        }

        // 测试BatchGetRow
        {
            List<RowPrimaryKey> primaryKeys = new ArrayList<RowPrimaryKey>();
            for (int i = 0; i < rowCount; i++) {
                primaryKeys.add(new RowPrimaryKey().addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(i)));
            }
            List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
            MultiRowQueryCriteria multiRowQueryCriteria = new MultiRowQueryCriteria(tableName);
            multiRowQueryCriteria.setRowKeys(primaryKeys);
            multiRowQueryCriteria.setColumnsToGet(columnNames);
            multiRowQueryCriteria.setFilter(filter);
            criterias.add(multiRowQueryCriteria);
            List<BatchGetRowResult.RowStatus> result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
            assertEquals(result.size(), rowCount);
            for (int i = 0; i < result.size(); i++) {
                BatchGetRowResult.RowStatus rowResult = result.get(i);
                Row row = rowResult.getRow();
                assertTrue(rowResult.isSucceed());
                if (expectResult) {
                    assertTrue(row != null);
                    assertEquals(2, row.getColumns().size());
                    assertEquals(i, row.getColumns().get("pk").asLong());
                    assertEquals(999, row.getColumns().get("col_exist").asLong());
                } else {
                    checkRow(new Row(), row);
                }
            }
        }

        // 测试GetRange
        {
            RowPrimaryKey beginKey = new RowPrimaryKey()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.INF_MIN);
            RowPrimaryKey endKey = new RowPrimaryKey()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.INF_MAX);

            RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(beginKey);
            rangeRowQueryCriteria.setExclusiveEndPrimaryKey(endKey);
            rangeRowQueryCriteria.setFilter(filter);
            GetRangeResult result = OTSHelper.getRange(ots, rangeRowQueryCriteria);
            List<Row> rows = result.getRows();
            if (expectResult) {
                assertEquals(rows.size(), rowCount);
                for (int i = 0; i < rowCount; i++) {
                    Row row = rows.get(i);
                    assertTrue(row != null);
                    assertEquals(2, row.getColumns().size());
                    assertEquals(i, row.getColumns().get("pk").asLong());
                    assertEquals(999, row.getColumns().get("col_exist").asLong());
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

        for (int i = 0; i < 10; i++) {
            RowPrimaryKey pk = new RowPrimaryKey()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(i));
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

        // put 1000 rows
        for (int i = 0; i < 500; i++) {
            Map<String, ColumnValue> columns = new HashMap<String, ColumnValue>();
            RowPrimaryKey pk = new RowPrimaryKey().addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(i));
            Row row = new Row();
            row.addColumn("pk", ColumnValue.fromLong(i));
            for (int j = 0; j < 128; j++) {
                columns.put("col" + j, ColumnValue.fromLong(999));
                row.addColumn("col" + j, ColumnValue.fromLong(999));
            }
            OTSHelper.putRow(ots, tableName, pk, columns);
            expectRows1.add(row);
            expectRows2.add(row);
        }
        for (int i = 500; i < 1000; i++) {
            Map<String, ColumnValue> columns = new HashMap<String, ColumnValue>();
            RowPrimaryKey pk = new RowPrimaryKey().addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(i));
            Row row = new Row();
            row.addColumn("pk", ColumnValue.fromLong(i));
            for (int j = 0; j < 127; j++) {
                columns.put("col" + j, ColumnValue.fromLong(999));
                row.addColumn("col" + j, ColumnValue.fromLong(999));
            }
            columns.put("col_unique", ColumnValue.fromLong(999));
            row.addColumn("col_unique", ColumnValue.fromLong(999));
            OTSHelper.putRow(ots, tableName, pk, columns);
            expectRows2.add(row);
        }

        // columns to get
        List<String> columnNames = new ArrayList<String>();
        for (int i = 0; i < 128; i++) {
            columnNames.add("col" + Integer.toString(i));
        }

        // 测试GetRange，FilterIfMissing = true
        RelationalCondition filter = new RelationalCondition("col127",
                RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromLong(999));
        filter.setPassIfMissing(false);

        // for GetRange
        RowPrimaryKey beginKey = new RowPrimaryKey()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.INF_MIN);
        RowPrimaryKey endKey = new RowPrimaryKey()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.INF_MAX);

        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(beginKey);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(endKey);
        rangeRowQueryCriteria.setFilter(filter);

        List<Row> allRows = new ArrayList<Row>();
        RowPrimaryKey nextKey = beginKey;
        while (nextKey != null) {
            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(nextKey);
            GetRangeResult result = OTSHelper.getRange(ots, rangeRowQueryCriteria);
            nextKey = result.getNextStartPrimaryKey();
            allRows.addAll(result.getRows());
        }
        assertEquals(500, allRows.size());
        for (int i = 0; i < allRows.size(); i++) {
            Row row = allRows.get(i);
            checkRow(expectRows1.get(i), row);
        }

        // 测试GetRange，FilterIfMissing = false
        filter.setPassIfMissing(true);
        rangeRowQueryCriteria.setFilter(filter);

        allRows.clear();
        nextKey = beginKey;
        while (nextKey != null) {
            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(nextKey);
            GetRangeResult result = OTSHelper.getRange(ots, rangeRowQueryCriteria);
            nextKey = result.getNextStartPrimaryKey();
            allRows.addAll(result.getRows());
        }
        assertEquals(1000, allRows.size());
        for (int i = 0; i < allRows.size(); i++) {
            Row row = allRows.get(i);
            checkRow(expectRows2.get(i), row);
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

        {
            // put 1 rows * 4 cols
            int kColumnCount = 4;
            Map<String, ColumnValue> columns = new HashMap<String, ColumnValue>();
            RowPrimaryKey pk = new RowPrimaryKey().addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(0));
            Row row = new Row();
            row.addColumn("pk", ColumnValue.fromLong(0));
            for (int j = 0; j < kColumnCount; j++) {
                columns.put("C" + Integer.toString(j), ColumnValue.fromBinary("ABC".getBytes()));
                row.addColumn("C" + Integer.toString(j), ColumnValue.fromBinary("ABC".getBytes()));
            }
            OTSHelper.putRow(ots, tableName, pk, columns);

            // 测试GetRow，并设置以下filter：C0 > 'AAA' OR C1 < 'ABD' OR NOT C2 = 'XXX' AND C3 = 'ABC'
            CompositeCondition subFilter1 = new CompositeCondition(CompositeCondition.LogicOperator.NOT);
            subFilter1.addCondition(new RelationalCondition("C2", RelationalCondition.CompareOperator.EQUAL,
                    ColumnValue.fromBinary("XXX".getBytes())));
            CompositeCondition subFilter2 = new CompositeCondition(CompositeCondition.LogicOperator.AND);
            subFilter2.addCondition(subFilter1).addCondition(new RelationalCondition("C3",
                    RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromBinary("ABC".getBytes())));
            CompositeCondition filter = new CompositeCondition(CompositeCondition.LogicOperator.OR);
            filter.addCondition(new RelationalCondition("C0",
                    RelationalCondition.CompareOperator.GREATER_THAN, ColumnValue.fromBinary("AAA".getBytes())))
                    .addCondition(new RelationalCondition("C1",
                            RelationalCondition.CompareOperator.LESS_THAN, ColumnValue.fromBinary("ABD".getBytes())))
                    .addCondition(subFilter2);

            List<String> columnNames = new ArrayList<String>();
            columnNames.add("pk");
            for (int j = 0; j < kColumnCount; j++) {
                columnNames.add("C" + j);
            }
            SingleRowQueryCriteria rowQueryCriteria = new SingleRowQueryCriteria(tableName);
            rowQueryCriteria.setPrimaryKey(pk);
            rowQueryCriteria.setColumnsToGet(columnNames);
            rowQueryCriteria.setFilter(filter);
            GetRowRequest getRowRequest = new GetRowRequest(rowQueryCriteria);
            GetRowResult result = ots.getRow(getRowRequest);
            Row resultRow = result.getRow();
            assertTrue(resultRow != null);
            checkRow(row, resultRow);
        }

        {
            // put 1 rows * 4 cols
            int kColumnCount = 4;
            Map<String, ColumnValue> columns = new HashMap<String, ColumnValue>();
            RowPrimaryKey pk = new RowPrimaryKey().addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(1));
            Row row = new Row();
            row.addColumn("pk", ColumnValue.fromLong(1));
            for (int j = 0; j < kColumnCount; j++) {
                columns.put("C" + Integer.toString(j), ColumnValue.fromBinary("XXX".getBytes()));
                row.addColumn("C" + Integer.toString(j), ColumnValue.fromBinary("XXX".getBytes()));
            }
            OTSHelper.putRow(ots, tableName, pk, columns);

            // 测试GetRow，并设置以下filter：C0 > 'AAA' OR C1 < 'ABD' OR NOT C2 = 'XXX' AND C3 = 'ABC'
            CompositeCondition subFilter1 = new CompositeCondition(CompositeCondition.LogicOperator.NOT);
            subFilter1.addCondition(new RelationalCondition("C2", RelationalCondition.CompareOperator.EQUAL,
                    ColumnValue.fromBinary("XXX".getBytes())));
            CompositeCondition subFilter2 = new CompositeCondition(CompositeCondition.LogicOperator.AND);
            subFilter2.addCondition(subFilter1).addCondition(new RelationalCondition("C3",
                    RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromBinary("ABC".getBytes())));
            CompositeCondition filter = new CompositeCondition(CompositeCondition.LogicOperator.OR);
            filter.addCondition(new RelationalCondition("C0",
                    RelationalCondition.CompareOperator.GREATER_THAN, ColumnValue.fromBinary("AAA".getBytes())))
                    .addCondition(new RelationalCondition("C1",
                            RelationalCondition.CompareOperator.LESS_THAN, ColumnValue.fromBinary("ABD".getBytes())))
                    .addCondition(subFilter2);

            List<String> columnNames = new ArrayList<String>();
            columnNames.add("pk");
            for (int j = 0; j < kColumnCount; j++) {
                columnNames.add("C" + Integer.toString(j));
            }
            SingleRowQueryCriteria rowQueryCriteria = new SingleRowQueryCriteria(tableName);
            rowQueryCriteria.setPrimaryKey(pk);
            rowQueryCriteria.setColumnsToGet(columnNames);
            rowQueryCriteria.setFilter(filter);
            GetRowRequest getRowRequest = new GetRowRequest(rowQueryCriteria);
            GetRowResult result = ots.getRow(getRowRequest);
            Row resultRow = result.getRow();
            assertTrue(resultRow != null);
            checkRow(row, resultRow);
        }
    }
}
