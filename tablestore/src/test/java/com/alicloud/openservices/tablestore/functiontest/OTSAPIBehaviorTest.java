package com.alicloud.openservices.tablestore.functiontest;


import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.core.utils.Pair;
import com.alicloud.openservices.tablestore.common.OTSHelper;
import com.alicloud.openservices.tablestore.common.Utils;
import com.google.gson.JsonSyntaxException;

import org.junit.*;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OTSAPIBehaviorTest {

    private static String tableName = "SystemStatusTestTable";

    private static SyncClientInterface ots;

    // For numerical padding, for example: 2 -> 002, 12 -> 012.
    private static NumberFormat formatter = NumberFormat.getNumberInstance();

    @BeforeClass
    public static void classBefore() throws JsonSyntaxException, IOException {
        ots = Utils.getOTSInstance();
        formatter.setMinimumIntegerDigits(3);
        formatter.setGroupingUsed(false);
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
     * Create a test table, specify maxVersions
     *
     * @param maxVersions
     */
    private void createTable(int maxVersions) {
        createTable(maxVersions, -1);
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

    /**
     * Create a test table, specify maxVersions and TTL
     *
     * @param maxVersions
     * @param timeToLive
     */
    private void createTable(int maxVersions, int timeToLive) {
        List<PrimaryKeySchema> pks = new ArrayList<PrimaryKeySchema>();
        pks.add(new PrimaryKeySchema("pk", PrimaryKeyType.STRING));
        TableMeta meta = new TableMeta(tableName);
        meta.addPrimaryKeyColumns(pks);
        TableOptions tableOptions = new TableOptions();
        tableOptions.setMaxVersions(maxVersions);
        tableOptions.setTimeToLive(timeToLive);
        tableOptions.setMaxTimeDeviation(Long.MAX_VALUE / 1000000);
        OTSHelper.createTable(ots, meta, new CapacityUnit(0, 0),
                tableOptions);
        Utils.waitForPartitionLoad(tableName);
    }

    /**
     * MaxVersion=100, the put in BatchWriteRow includes 10 rows with the same PK.
     * Each row contains 16 columns, the column name length is 255, and each column contains 8 cells. 
     * BatchGetRow is expected to read and verify the data of the last row.
     */
    @Test
    public void testBatchWriteRowToPutIdenticalRowPk() {
        // createTable
        createTable(100);

        int rowNum = 10;
        int columnNum = 16;
        int versionNum = 8;
        StringBuilder columnNamePreBuilder = new StringBuilder();
        for (int i = 0; i < 252; i++) {
            columnNamePreBuilder.append("A");
        }
        String columnNamePre = columnNamePreBuilder.toString();

        List<RowPutChange> puts = new ArrayList<RowPutChange>();
        for (int row = 0; row < rowNum; row++) {

            List<Column> columns = new ArrayList<Column>();
            for (int col = 0; col < columnNum; col++) {
                for (int ver = 0; ver < versionNum; ver++) {
                    columns.add(new Column(columnNamePre + formatter.format(col),
                            ColumnValue.fromString("col_value" + formatter.format(col)
                                    + ", row" + formatter.format(row)
                                    + ", version" + formatter.format(ver)), ver));
                }
            }

            RowPutChange rowPutChange = new RowPutChange(tableName, getPrimaryKey());
            rowPutChange.addColumns(columns);
            puts.add(rowPutChange);
        }
        OTSHelper.batchWriteRowNoLimit(ots, puts, null, null);

        // Validation
        List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
        MultiRowQueryCriteria criteria = new MultiRowQueryCriteria(tableName);
        criteria.addRow(getPrimaryKey());
        criteria.setMaxVersions(Integer.MAX_VALUE);
        criterias.add(criteria);
        BatchGetRowResponse result = OTSHelper.batchGetRow(ots, criterias);

        List<Column> expect = new ArrayList<Column>();
        for (int col = 0; col < columnNum; col++) {
            for (int ver = versionNum - 1; ver >= 0; ver--) {
                expect.add(new Column(columnNamePre + formatter.format(col),
                        ColumnValue.fromString("col_value" + formatter.format(col)
                                + ", row" + formatter.format(rowNum - 1)
                                + ", version" + formatter.format(ver)), ver));
            }
        }
        Utils.checkColumns(result.getSucceedRows().get(0).getRow().getColumns(), expect);
    }

    /**
     * MaxVersion=100, the update in BatchWriteRow includes 10 rows, these
     * 10 rows have the same PK. Each row contains 1 column, the column name length is 255, 
     * each column contains 8 cells, and the column names in each row are different.
     * BatchGetRow reads and verifies that it expects to get data of 80 cells.
     */
    @Test
    public void testBatchWriteRowToUpdateIdenticalRowPk() {
        // createTable
        createTable(100);

        int rowNum = 10;
        int versionNum = 8;
        StringBuilder columnNamePreBuilder = new StringBuilder();
        for (int i = 0; i < 252; i++) {
            columnNamePreBuilder.append("A");
        }
        String columnNamePre = columnNamePreBuilder.toString();

        List<RowUpdateChange> rowUpdateChanges = new ArrayList<RowUpdateChange>();
        List<Column> columns = new ArrayList<Column>();
        for (int row = 0; row < rowNum; row++) {
            for (int ver = 0; ver < versionNum; ver++) {
                columns.add(new Column(columnNamePre + formatter.format(row),
                        ColumnValue.fromString("col_value" + formatter.format(row)
                                + ", row" + formatter.format(row)
                                + ", version" + formatter.format(ver)), ver));
            }
            RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, getPrimaryKey());
            rowUpdateChange.put(columns);
            rowUpdateChanges.add(rowUpdateChange);
        }
        OTSHelper.batchWriteRowNoLimit(ots, null, rowUpdateChanges, null);

        // Validation
        List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
        MultiRowQueryCriteria criteria = new MultiRowQueryCriteria(tableName);
        criteria.addRow(getPrimaryKey());
        criteria.setMaxVersions(Integer.MAX_VALUE);
        criterias.add(criteria);
        BatchGetRowResponse result = OTSHelper.batchGetRow(ots, criterias);

        List<Column> expect = new ArrayList<Column>();
        for (int row = 0; row < rowNum; row++) {
            for (int ver = versionNum - 1; ver >= 0; ver--) {
                expect.add(new Column(columnNamePre + formatter.format(row),
                        ColumnValue.fromString("col_value" + formatter.format(row)
                                + ", row" + formatter.format(row)
                                + ", version" + formatter.format(ver)), ver));
            }
        }
        Utils.checkColumns(result.getSucceedRows().get(0).getRow().getColumns(), expect);
    }

    /**
     * MaxVersion=100, the update in BatchWriteRow includes 10 rows. These 10 rows have the same PK, 
     * and each row contains 1 column with a column name length of 255. Each column contains 8 cells, 
     * and the column names in each row are the same. BatchGetRow reads to verify that it retrieves 8 cell data, 
     * which should match the last row.
     */
    @Test
    public void testBatchWriteRowToUpdateIdenticalRowPkAndColumnName() {
        // createTable
        createTable(100);

        int rowNum = 10;
        int versionNum = 8;
        StringBuilder columnNamePreBuilder = new StringBuilder();
        for (int i = 0; i < 252; i++) {
            columnNamePreBuilder.append("A");
        }
        String columnNamePre = columnNamePreBuilder.toString();

        List<RowUpdateChange> rowUpdateChanges = new ArrayList<RowUpdateChange>();
        List<Column> columns = new ArrayList<Column>();
        for (int row = 0; row < rowNum; row++) {
            for (int ver = 0; ver < versionNum; ver++) {
                columns.add(new Column(
                                columnNamePre + formatter.format(ver),
                                ColumnValue.fromString("col_value" + formatter.format(row) + ", row" + formatter.format(row) + ", version" + formatter.format(ver)), 
                                ver));
            }
            RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, getPrimaryKey());
            rowUpdateChange.put(columns);
            rowUpdateChanges.add(rowUpdateChange);
        }
        OTSHelper.batchWriteRowNoLimit(ots, null, rowUpdateChanges, null);

        // Validation
        List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
        MultiRowQueryCriteria criteria = new MultiRowQueryCriteria(tableName);
        criteria.addRow(getPrimaryKey());
        criteria.setMaxVersions(100);
        criterias.add(criteria);
        BatchGetRowResponse result = OTSHelper.batchGetRow(ots, criterias);

        List<Column> expect = new ArrayList<Column>();
        for (int ver = versionNum - 1; ver >= 0; ver--) {
            expect.add(new Column(columnNamePre + formatter.format(versionNum - 1 - ver),
                    ColumnValue.fromString("col_value" + formatter.format(rowNum - 1)
                            + ", row" + formatter.format(rowNum - 1)
                            + ", version" + formatter.format(versionNum - 1 - ver)), versionNum - 1 - ver));
        }
        Utils.checkColumns(result.getSucceedRows().get(0).getRow().getColumns(), expect);
    }

    /**
     * When the partition keys of batch atomic writes on the same table are identical, the write operation succeeds.
     */
    @Test
    public void testAtomicBatchWriteRowWithSamePartKey() {
        // createTable
        createTable(100);
        int rowNum = 10;
        BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();
        batchWriteRowRequest.setAtomic(true);
        for (int row = 0; row < rowNum; row++) {
            RowPutChange rowPutChange = new RowPutChange(tableName, getPrimaryKey());
            batchWriteRowRequest.addRowChange(rowPutChange);
        }
        BatchWriteRowResponse result = ots.batchWriteRow(batchWriteRowRequest);
        List<BatchWriteRowResponse.RowResult> successRows = result.getSucceedRows();
        List<BatchWriteRowResponse.RowResult> failedRows = result.getFailedRows();
        assertEquals(rowNum, successRows.size());
        assertEquals(0, failedRows.size());
        for (BatchWriteRowResponse.RowResult rowResult : successRows) {
            assertTrue(rowResult.isSucceed());
        }
    }

    /**
     * When the partition keys for batch atomic writes on the same table are different, all writes to this table will fail.
     * Note: The backend flag sqlonline_worker_ForbidAtomicBatchModify needs to be set to false.
     */
    @Test
    public void testAtomicBatchWriteRowWithDiffPartKey() {
        // createTable
        createTable(100);
        int rowNum = 10;
        BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();
        batchWriteRowRequest.setAtomic(true);
        for (int row = 0; row < rowNum; row++) {
            RowPutChange rowPutChange = new RowPutChange(tableName, getPrimaryKey(String.valueOf(row)));
            batchWriteRowRequest.addRowChange(rowPutChange);
        }
        BatchWriteRowResponse result = ots.batchWriteRow(batchWriteRowRequest);
        List<BatchWriteRowResponse.RowResult> successRows = result.getSucceedRows();
        List<BatchWriteRowResponse.RowResult> failedRows = result.getFailedRows();
        assertEquals(0, successRows.size());
        assertEquals(rowNum, failedRows.size());
        for (BatchWriteRowResponse.RowResult rowResult : failedRows) {
            assertEquals("OTSDataOutOfRange", rowResult.getError().getCode());
            assertEquals("Data out of scope of atomic. Atomic PartKey:0. Data PartKey:1", rowResult.getError().getMessage());
        }
    }

    /**
     * Write 2 rows (PK0, PK1), each row containing 2 columns (C0, C1), and each column containing 2 cells (T0, T1).
     * Test the following cases:
     * 1) Delete cell (PK0, C0, T0), then use GetRow to read each cell, each column, and each row individually.
     * 2) Delete cell (PK1, C1, T1), then use GetRow to read each cell, each column, and each row individually.
     * 3) Delete column (PK0, C0), then use GetRow to read each cell, each column, and each row individually.
     * 4) Delete row PK0, then use GetRow to read each cell, each column, and each row individually.
     */
    @Test
    public void testDeleteCell() {
        // createTable
        createTable(3);

        // prepare data
        int rowNum = 2;
        int columnNum = 2;
        int versionNum = 2;

        for (int row = 0; row < rowNum; row++) {
            List<Column> columns = new ArrayList<Column>();
            for (int col = 0; col < columnNum; col++) {
                for (int ver = 0; ver < versionNum; ver++) {
                    columns.add(new Column("col_name" + formatter.format(col),
                            ColumnValue.fromString("col_value" + formatter.format(col)
                                + ", version" + formatter.format(ver)), ver));
                }
            }
            OTSHelper.putRow(ots, tableName, getPrimaryKey(formatter.format(row)), columns);
        }

        // 1) Delete cell (PK0, C0, T0)
        List<Pair<String, Long>> deleteCells = new ArrayList<Pair<String, Long>>();
        deleteCells.add(new Pair<String, Long>("col_name" + formatter.format(0), 0L));
        OTSHelper.updateRow(ots, tableName, getPrimaryKey(formatter.format(0)), null, null, deleteCells);

        // GetRow reads each cell, each column, and each row separately.
        for (int row = 0; row < rowNum; row++) {
            for (int col = 0; col < columnNum; col++) {
                for (int ver = 0; ver < versionNum; ver++) {
                    SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName,
                            getPrimaryKey(formatter.format(row)));
                    criteria.addColumnsToGet("col_name" + formatter.format(col));
                    criteria.setTimestamp(ver);
                    GetRowResponse result = OTSHelper.getRow(ots, criteria);
                    if (row == 0 && col == 0 && ver == 0) {
                        assertEquals(null, result.getRow());
                    } else {
                        List<Column> expect = new ArrayList<Column>();
                        expect.add(new Column("col_name" + formatter.format(col),
                                ColumnValue.fromString("col_value" + formatter.format(col)
                                + ", version" + formatter.format(ver)), ver));
                        Utils.checkColumns(result.getRow().getColumns(), expect);
                    }
                }
            }
        }

        // 2) Delete Cell (PK1, C1, T1)
        deleteCells = new ArrayList<Pair<String, Long>>();
        deleteCells.add(new Pair<String, Long>("col_name" + formatter.format(1), 1L));
        OTSHelper.updateRow(ots, tableName, getPrimaryKey(formatter.format(1)), null, null, deleteCells);

        // GetRow reads each cell, each column, and each row separately.
        for (int row = 0; row < rowNum; row++) {
            for (int col = 0; col < columnNum; col++) {
                for (int ver = 0; ver < versionNum; ver++) {
                    SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName,
                            getPrimaryKey(formatter.format(row)));
                    criteria.addColumnsToGet("col_name" + formatter.format(col));
                    criteria.setTimestamp(ver);
                    GetRowResponse result = OTSHelper.getRow(ots, criteria);
                    if (row == 0 && col == 0 && ver == 0) {
                        assertEquals(null, result.getRow());
                    } else if (row == 1 && col == 1 && ver == 1) {
                        assertEquals(null, result.getRow());
                    } else {
                        List<Column> expect = new ArrayList<Column>();
                        expect.add(new Column("col_name" + formatter.format(col),
                                ColumnValue.fromString("col_value" + formatter.format(col)
                                        + ", version" + formatter.format(ver)), ver));
                        Utils.checkColumns(result.getRow().getColumns(), expect);
                    }
                }
            }
        }

        // 3) Delete column (PK0, C0)
        List<String> deletes = new ArrayList<String>();
        deletes.add("col_name" + formatter.format(0));
        OTSHelper.updateRow(ots, tableName, getPrimaryKey(formatter.format(0)), null, deletes, null);

        // GetRow reads each cell, each column, and each row separately.
        for (int row = 0; row < rowNum; row++) {
            for (int col = 0; col < columnNum; col++) {
                for (int ver = 0; ver < versionNum; ver++) {
                    SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName,
                            getPrimaryKey(formatter.format(row)));
                    criteria.addColumnsToGet("col_name" + formatter.format(col));
                    criteria.setTimestamp(ver);
                    GetRowResponse result = OTSHelper.getRow(ots, criteria);
                    if (row == 0 && col == 0) {
                        assertEquals(null, result.getRow());
                    } else if (row == 1 && col == 1 && ver == 1) {
                        assertEquals(null, result.getRow());
                    } else {
                        List<Column> expect = new ArrayList<Column>();
                        expect.add(new Column("col_name" + formatter.format(col),
                                ColumnValue.fromString("col_value" + formatter.format(col)
                                        + ", version" + formatter.format(ver)), ver));
                        Utils.checkColumns(result.getRow().getColumns(), expect);
                    }
                }
            }
        }

        // 4) Delete row PK0
        OTSHelper.deleteRow(ots, tableName, getPrimaryKey(formatter.format(0)));

        // GetRow reads each cell, each column, and each row separately.
        for (int row = 0; row < rowNum; row++) {
            for (int col = 0; col < columnNum; col++) {
                for (int ver = 0; ver < versionNum; ver++) {
                    SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName,
                            getPrimaryKey(formatter.format(row)));
                    criteria.addColumnsToGet("col_name" + formatter.format(col));
                    criteria.setTimestamp(ver);
                    GetRowResponse result = OTSHelper.getRow(ots, criteria);
                    if (row == 0) {
                        assertEquals(null, result.getRow());
                    } else if (row == 1 && col == 1 && ver == 1) {
                        assertEquals(null, result.getRow());
                    } else {
                        List<Column> expect = new ArrayList<Column>();
                        expect.add(new Column("col_name" + formatter.format(col),
                                ColumnValue.fromString("col_value" + formatter.format(col)
                                        + ", version" + formatter.format(ver)), ver));
                        Utils.checkColumns(result.getRow().getColumns(), expect);
                    }
                }
            }
        }
    }

    /**
     * Write 2 rows, each row contains 128 columns, and each column contains 2 cells. Sequentially delete one cell, one column, and an entire row, 
     * and validate by reading all cells with GetRange each time. Then write the same one cell, one column, and an entire row again, 
     * and validate by reading all cells with GetRange each time.
     */
    @Test
    public void testDeleteCellThenWriteBack1() {
        // createTable
        createTable(3);

        int rowNum = 2;
        int columnNum = 128;
        int versionNum = 2;

        // prepare data
        for (int row = 0; row < rowNum; row++) {
            for (int ver = 0; ver < versionNum; ver++) {
                List<Column> columns = new ArrayList<Column>();
                for (int col = 0; col < columnNum; col++) {
                    columns.add(new Column("col_name" + formatter.format(col),
                            ColumnValue.fromString("col_value" + formatter.format(col)
                                    + ", version" + formatter.format(ver)), ver));
                }
                OTSHelper.updateRow(ots, tableName,
                        getPrimaryKey(formatter.format(row)), columns, null, null);
            }
        }

        // Delete cell (Delete Row1, Col10, Ver0)
        List<Pair<String, Long>> deleteCells = new ArrayList<Pair<String, Long>>();
        deleteCells.add(new Pair<String, Long>("col_name" + formatter.format(10), 0L));
        OTSHelper.updateRow(ots, tableName,
                        getPrimaryKey(formatter.format(1)), null, null, deleteCells);

        // getRange validation
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(getPrimaryKey(""));
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(getPrimaryKey("2"));
        rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
        GetRangeResponse result = OTSHelper.getRange(ots, rangeRowQueryCriteria);

        assertEquals(2, result.getRows().size());
        for (Row row : result.getRows()) {
            PrimaryKey primaryKey = row.getPrimaryKey();
            for (int col = 0; col < columnNum; col++) {
                List<Column> columns = row.getColumn("col_name" + formatter.format(col));
                if (primaryKey.equals(getPrimaryKey(formatter.format(1))) && col == 10) {
                    assertEquals(1, columns.size());
                    assertEquals("col_name" + formatter.format(col), columns.get(0).getName());
                    assertEquals("col_value" + formatter.format(col)
                            + ", version" + formatter.format(1), columns.get(0).getValue().asString());
                    assertEquals(1, columns.get(0).getTimestamp());
                } else {
                    assertEquals(2, columns.size());
                    for (int ver = 0; ver < 2; ver++) {
                        assertEquals("col_name" + formatter.format(col), columns.get(1 - ver).getName());
                        assertEquals("col_value" + formatter.format(col)
                                + ", version" + formatter.format(ver), columns.get(1 - ver).getValue().asString());
                        assertEquals(ver, columns.get(1 - ver).getTimestamp());
                    }
                }
            }
        }

        // Delete Column (Delete Row1, Col100)
        List<String> deletes = new ArrayList<String>();
        deletes.add("col_name" + formatter.format(100));
        OTSHelper.updateRow(ots, tableName, getPrimaryKey(formatter.format(1)), null, deletes, null);

        // getRange validation
        rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(getPrimaryKey(""));
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(getPrimaryKey("2"));
        rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
        result = OTSHelper.getRange(ots, rangeRowQueryCriteria);

        assertEquals(2, result.getRows().size());
        for (Row row : result.getRows()) {
            PrimaryKey primaryKey = row.getPrimaryKey();
            for (int col = 0; col < columnNum; col++) {
                List<Column> columns = row.getColumn("col_name" + formatter.format(col));
                if (primaryKey.equals(getPrimaryKey(formatter.format(1))) && col == 100) {
                    assertEquals(0, columns.size());
                } else if (primaryKey.equals(getPrimaryKey(formatter.format(1))) && col == 10) {
                    assertEquals(1, columns.size());
                    assertEquals("col_name" + formatter.format(col), columns.get(0).getName());
                    assertEquals("col_value" + formatter.format(col)
                            + ", version" + formatter.format(1), columns.get(0).getValue().asString());
                    assertEquals(1, columns.get(0).getTimestamp());
                } else {
                    assertEquals(2, columns.size());
                    for (int ver = 0; ver < 2; ver++) {
                        assertEquals("col_name" + formatter.format(col), columns.get(1 - ver).getName());
                        assertEquals("col_value" + formatter.format(col)
                                + ", version" + formatter.format(ver), columns.get(1 - ver).getValue().asString());
                        assertEquals(ver, columns.get(1 - ver).getTimestamp());
                    }
                }
            }
        }

        // Delete row (Delete Row0)
        OTSHelper.deleteRow(ots, tableName, getPrimaryKey(formatter.format(0)));

        // getRange validation
        rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(getPrimaryKey(""));
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(getPrimaryKey("2"));
        rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
        result = OTSHelper.getRange(ots, rangeRowQueryCriteria);

        assertEquals(1, result.getRows().size());
        for (Row row : result.getRows()) {
            PrimaryKey primaryKey = row.getPrimaryKey();
            assertEquals(getPrimaryKey(formatter.format(1)), primaryKey);
            for (int col = 0; col < columnNum; col++) {
                List<Column> columns = row.getColumn("col_name" + formatter.format(col));
                if (col == 100) {
                    assertEquals(0, columns.size());
                } else if (col == 10) {
                    assertEquals(1, columns.size());
                    assertEquals("col_name" + formatter.format(col), columns.get(0).getName());
                    assertEquals("col_value" + formatter.format(col)
                            + ", version" + formatter.format(1), columns.get(0).getValue().asString());
                    assertEquals(1, columns.get(0).getTimestamp());
                } else {
                    assertEquals(2, columns.size());
                    for (int ver = 0; ver < 2; ver++) {
                        assertEquals("col_name" + formatter.format(col), columns.get(1 - ver).getName());
                        assertEquals("col_value" + formatter.format(col)
                                + ", version" + formatter.format(ver), columns.get(1 - ver).getValue().asString());
                        assertEquals(ver, columns.get(1 - ver).getTimestamp());
                    }
                }
            }
        }

        // Write Row1 Col10 Ver0
        List<Column> puts = new ArrayList<Column>();
        puts.add(new Column("col_name" + formatter.format(10),
                ColumnValue.fromString("col_value" + formatter.format(10)
                        + ", version" + formatter.format(0)), 0));
        OTSHelper.updateRow(ots, tableName, getPrimaryKey(formatter.format(1)),
                    puts, null, null);

        // getRange validation
        rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(getPrimaryKey(""));
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(getPrimaryKey("2"));
        rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
        result = OTSHelper.getRange(ots, rangeRowQueryCriteria);

        assertEquals(1, result.getRows().size());
        for (Row row : result.getRows()) {
            PrimaryKey primaryKey = row.getPrimaryKey();
            assertEquals(getPrimaryKey(formatter.format(1)), primaryKey);
            for (int col = 0; col < columnNum; col++) {
                List<Column> columns = row.getColumn("col_name" + formatter.format(col));
                if (col == 100) {
                    assertEquals(0, columns.size());
                } else {
                    assertEquals(2, columns.size());
                    for (int ver = 0; ver < 2; ver++) {
                        assertEquals("col_name" + formatter.format(col), columns.get(1 - ver).getName());
                        assertEquals("col_value" + formatter.format(col)
                                + ", version" + formatter.format(ver), columns.get(1 - ver).getValue().asString());
                        assertEquals(ver, columns.get(1 - ver).getTimestamp());
                    }
                }
            }
        }

        // Write Row1 Col100
        puts = new ArrayList<Column>();
        for (int ver = 0; ver < versionNum; ver++) {
            puts.add(new Column("col_name" + formatter.format(100),
                    ColumnValue.fromString("col_value" + formatter.format(100)
                        + ", version" + formatter.format(ver)), ver));
        }
        OTSHelper.updateRow(ots, tableName, getPrimaryKey(formatter.format(1)),
                    puts, null, null);

        // getRange validation
        rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(getPrimaryKey(""));
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(getPrimaryKey("2"));
        rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
        result = OTSHelper.getRange(ots, rangeRowQueryCriteria);

        assertEquals(1, result.getRows().size());
        for (Row row : result.getRows()) {
            PrimaryKey primaryKey = row.getPrimaryKey();
            assertEquals(getPrimaryKey(formatter.format(1)), primaryKey);
            for (int col = 0; col < columnNum; col++) {
                List<Column> columns = row.getColumn("col_name" + formatter.format(col));
                assertEquals(2, columns.size());
                for (int ver = 0; ver < 2; ver++) {
                    assertEquals("col_name" + formatter.format(col), columns.get(1 - ver).getName());
                    assertEquals("col_value" + formatter.format(col)
                            + ", version" + formatter.format(ver), columns.get(1 - ver).getValue().asString());
                    assertEquals(ver, columns.get(1 - ver).getTimestamp());
                }
            }
        }

        // Write Row0
        for (int ver = 0; ver < versionNum; ver++) {
            List<Column> columns = new ArrayList<Column>();
            for (int col = 0; col < columnNum; col++) {
                columns.add(new Column("col_name" + formatter.format(col),
                        ColumnValue.fromString("col_value" + formatter.format(col)
                                + ", version" + formatter.format(ver)), ver));
            }
            OTSHelper.updateRow(ots, tableName,
                    getPrimaryKey(formatter.format(0)), columns, null, null);
        }

        // getRange validation
        rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(getPrimaryKey(""));
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(getPrimaryKey("2"));
        rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
        result = OTSHelper.getRange(ots, rangeRowQueryCriteria);

        assertEquals(2, result.getRows().size());
        for (Row row : result.getRows()) {
            PrimaryKey primaryKey = row.getPrimaryKey();
            for (int col = 0; col < columnNum; col++) {
                List<Column> columns = row.getColumn("col_name" + formatter.format(col));
                assertEquals(2, columns.size());
                for (int ver = 0; ver < 2; ver++) {
                    assertEquals("col_name" + formatter.format(col), columns.get(1 - ver).getName());
                    assertEquals("col_value" + formatter.format(col)
                            + ", version" + formatter.format(ver), columns.get(1 - ver).getValue().asString());
                    assertEquals(ver, columns.get(1 - ver).getTimestamp());
                }
            }
        }
    }

    /**
     * Write 2 rows, each row contains 2 columns, and each column contains 128 CELLS. Sequentially delete an entire row, one column, and one cell, 
     * and verify by reading all CELLS with GetRange each time. Then write the same entire row, one column, and one cell again, and verify by reading all CELLS with GetRange each time.
     */
    @Test
    public void testDeleteCellThenWriteBack2() {
        // createTable
        createTable(200);

        int rowNum = 2;
        int columnNum = 2;
        int versionNum = 128;

        // prepare data
        for (int row = 0; row < rowNum; row++) {
            for (int ver = 0; ver < versionNum; ver++) {
                List<Column> columns = new ArrayList<Column>();
                for (int col = 0; col < columnNum; col++) {
                    columns.add(new Column("col_name" + formatter.format(col),
                            ColumnValue.fromString("col_value" + formatter.format(col)
                                    + ", version" + formatter.format(ver)), ver));
                }
                OTSHelper.updateRow(ots, tableName,
                        getPrimaryKey(formatter.format(row)), columns, null, null);
            }
        }

        // Delete row (Delete Row0)
        OTSHelper.deleteRow(ots, tableName, getPrimaryKey(formatter.format(0)));

        // getRange validation
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(getPrimaryKey(""));
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(getPrimaryKey("2"));
        rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
        GetRangeResponse result = OTSHelper.getRange(ots, rangeRowQueryCriteria);

        assertEquals(1, result.getRows().size());
        for (Row row : result.getRows()) {
            PrimaryKey primaryKey = row.getPrimaryKey();
            assertEquals(getPrimaryKey(formatter.format(1)), primaryKey);
            for (int col = 0; col < columnNum; col++) {
                List<Column> columns = row.getColumn("col_name" + formatter.format(col));
                assertEquals(versionNum, columns.size());
                for (int ver = 0; ver < versionNum; ver++) {
                    assertEquals("col_name" + formatter.format(col), columns.get(versionNum - 1 - ver).getName());
                    assertEquals("col_value" + formatter.format(col)
                            + ", version" + formatter.format(ver),
                            columns.get(versionNum - 1 - ver).getValue().asString());
                    assertEquals(ver, columns.get(versionNum - 1 - ver).getTimestamp());
                }
            }
        }

        // Delete Column (Delete Row1, Col0)
        List<String> deletes = new ArrayList<String>();
        deletes.add("col_name" + formatter.format(0));
        OTSHelper.updateRow(ots, tableName, getPrimaryKey(formatter.format(1)), null, deletes, null);

        // getRange validation
        rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(getPrimaryKey(""));
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(getPrimaryKey("2"));
        rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
        result = OTSHelper.getRange(ots, rangeRowQueryCriteria);

        assertEquals(1, result.getRows().size());
        for (Row row : result.getRows()) {
            PrimaryKey primaryKey = row.getPrimaryKey();
            assertEquals(getPrimaryKey(formatter.format(1)), primaryKey);
            for (int col = 0; col < columnNum; col++) {
                List<Column> columns = row.getColumn("col_name" + formatter.format(col));
                if (col == 0) {
                    assertEquals(0, columns.size());
                } else {
                    assertEquals(versionNum, columns.size());
                    for (int ver = 0; ver < versionNum; ver++) {
                        assertEquals("col_name" + formatter.format(col), columns.get(versionNum - 1 - ver).getName());
                        assertEquals("col_value" + formatter.format(col)
                                + ", version" + formatter.format(ver), columns.get(versionNum - 1 - ver).getValue().asString());
                        assertEquals(ver, columns.get(versionNum - 1 - ver).getTimestamp());
                    }
                }
            }
        }

        // Delete cell (Delete Row1, Col1, Ver0)
        List<Pair<String, Long>> deleteCells = new ArrayList<Pair<String, Long>>();
        deleteCells.add(new Pair<String, Long>("col_name" + formatter.format(1), 0L));
        OTSHelper.updateRow(ots, tableName,
                getPrimaryKey(formatter.format(1)), null, null, deleteCells);

        // getRange validation
        rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(getPrimaryKey(""));
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(getPrimaryKey("2"));
        rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
        result = OTSHelper.getRange(ots, rangeRowQueryCriteria);

        assertEquals(1, result.getRows().size());
        for (Row row : result.getRows()) {
            PrimaryKey primaryKey = row.getPrimaryKey();
            assertEquals(getPrimaryKey(formatter.format(1)), primaryKey);
            for (int col = 0; col < columnNum; col++) {
                List<Column> columns = row.getColumn("col_name" + formatter.format(col));
                if (col == 0) {
                    assertEquals(0, columns.size());
                } else if (col == 1) {
                    assertEquals(127, columns.size());
                    for (int ver = 127; ver > 0; ver--) {
                        assertEquals("col_name" + formatter.format(col), columns.get(127 - ver).getName());
                        assertEquals("col_value" + formatter.format(col)
                                + ", version" + formatter.format(ver), columns.get(127 - ver).getValue().asString());
                        assertEquals(ver, columns.get(127 - ver).getTimestamp());
                    }
                } else {
                    assertEquals(versionNum, columns.size());
                    for (int ver = 0; ver < versionNum; ver++) {
                        assertEquals("col_name" + formatter.format(col), columns.get(versionNum - 1 - ver).getName());
                        assertEquals("col_value" + formatter.format(col)
                                + ", version" + formatter.format(ver), columns.get(versionNum - 1 - ver).getValue().asString());
                        assertEquals(ver, columns.get(versionNum - 1 - ver).getTimestamp());
                    }
                }
            }
        }

        // Write Row0
        for (int ver = 0; ver < versionNum; ver++) {
            List<Column> columns = new ArrayList<Column>();
            for (int col = 0; col < columnNum; col++) {
                columns.add(new Column("col_name" + formatter.format(col),
                        ColumnValue.fromString("col_value" + formatter.format(col)
                                + ", version" + formatter.format(ver)), ver));
            }
            OTSHelper.updateRow(ots, tableName,
                    getPrimaryKey(formatter.format(0)), columns, null, null);
        }

        // getRange validation
        rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(getPrimaryKey(""));
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(getPrimaryKey("2"));
        rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
        result = OTSHelper.getRange(ots, rangeRowQueryCriteria);

        assertEquals(2, result.getRows().size());
        for (Row row : result.getRows()) {
            PrimaryKey primaryKey = row.getPrimaryKey();
            for (int col = 0; col < columnNum; col++) {
                List<Column> columns = row.getColumn("col_name" + formatter.format(col));
                if (primaryKey.equals(getPrimaryKey(formatter.format(1))) && col == 0) {
                    assertEquals(0, columns.size());
                } else if (primaryKey.equals(getPrimaryKey(formatter.format(1))) && col == 1) {
                    assertEquals(127, columns.size());
                    for (int ver = 127; ver > 0; ver--) {
                        assertEquals("col_name" + formatter.format(col), columns.get(127 - ver).getName());
                        assertEquals("col_value" + formatter.format(col)
                                + ", version" + formatter.format(ver), columns.get(127 - ver).getValue().asString());
                        assertEquals(ver, columns.get(127 - ver).getTimestamp());
                    }
                } else {
                    assertEquals(versionNum, columns.size());
                    for (int ver = 0; ver < versionNum; ver++) {
                        assertEquals("col_name" + formatter.format(col), columns.get(versionNum - 1 - ver).getName());
                        assertEquals("col_value" + formatter.format(col)
                                + ", version" + formatter.format(ver),
                                columns.get(versionNum - 1 - ver).getValue().asString());
                        assertEquals(ver, columns.get(versionNum - 1 - ver).getTimestamp());
                    }
                }
            }
        }

        // Write to Row1 Col0
        ArrayList<Column> puts = new ArrayList<Column>();
        for (int ver = 0; ver < versionNum; ver++) {
            puts.add(new Column("col_name" + formatter.format(0),
                    ColumnValue.fromString("col_value" + formatter.format(0)
                            + ", version" + formatter.format(ver)), ver));
        }
        OTSHelper.updateRow(ots, tableName, getPrimaryKey(formatter.format(1)),
                puts, null, null);

        // getRange validation
        rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(getPrimaryKey(""));
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(getPrimaryKey("2"));
        rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
        result = OTSHelper.getRange(ots, rangeRowQueryCriteria);

        assertEquals(2, result.getRows().size());
        for (Row row : result.getRows()) {
            PrimaryKey primaryKey = row.getPrimaryKey();
            for (int col = 0; col < columnNum; col++) {
                List<Column> columns = row.getColumn("col_name" + formatter.format(col));
                if (primaryKey.equals(getPrimaryKey(formatter.format(1))) && col == 1) {
                    assertEquals(127, columns.size());
                    for (int ver = 127; ver > 0; ver--) {
                        assertEquals("col_name" + formatter.format(col), columns.get(127 - ver).getName());
                        assertEquals("col_value" + formatter.format(col)
                                + ", version" + formatter.format(ver), columns.get(127 - ver).getValue().asString());
                        assertEquals(ver, columns.get(127 - ver).getTimestamp());
                    }
                } else {
                    assertEquals(versionNum, columns.size());
                    for (int ver = 0; ver < versionNum; ver++) {
                        assertEquals("col_name" + formatter.format(col), columns.get(versionNum - 1 - ver).getName());
                        assertEquals("col_value" + formatter.format(col)
                                + ", version" + formatter.format(ver),
                                columns.get(versionNum - 1 - ver).getValue().asString());
                        assertEquals(ver, columns.get(versionNum - 1 - ver).getTimestamp());
                    }
                }
            }
        }

        // Write Row1 Col1 Ver0
        puts = new ArrayList<Column>();
        puts.add(new Column("col_name" + formatter.format(1),
                ColumnValue.fromString("col_value" + formatter.format(1)
                        + ", version" + formatter.format(0)), 0));
        OTSHelper.updateRow(ots, tableName, getPrimaryKey(formatter.format(1)),
                puts, null, null);

        // getRange validation
        rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(getPrimaryKey(""));
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(getPrimaryKey("2"));
        rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
        result = OTSHelper.getRange(ots, rangeRowQueryCriteria);

        assertEquals(2, result.getRows().size());
        for (Row row : result.getRows()) {
            PrimaryKey primaryKey = row.getPrimaryKey();
            for (int col = 0; col < columnNum; col++) {
                List<Column> columns = row.getColumn("col_name" + formatter.format(col));
                assertEquals(versionNum, columns.size());
                for (int ver = 0; ver < versionNum; ver++) {
                    assertEquals("col_name" + formatter.format(col), columns.get(versionNum - 1 - ver).getName());
                    assertEquals("col_value" + formatter.format(col)
                            + ", version" + formatter.format(ver),
                            columns.get(versionNum - 1 - ver).getValue().asString());
                    assertEquals(ver, columns.get(versionNum - 1 - ver).getTimestamp());
                }
            }
        }
    }
}
