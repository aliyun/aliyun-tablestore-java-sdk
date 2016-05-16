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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;

public class RelationFilterTest extends BaseFT {
    private static Logger LOG = Logger.getLogger(RelationFilterTest.class.getName());

    private static String tableName = "RelationFilterFunctionTest";

    private static final OTS ots = OTSClientFactory.createOTSClient(
            ServiceSettings.load(), new ClientConfiguration());

    private static final int SECONDS_UNTIL_TABLE_READY = 10;

    @AfterClass
    public static void classAfter() {
        ots.shutdown();
    }

    @Before
    public void setup() throws Exception {
        OTSHelper.deleteAllTable(ots);
        LOG.info("Instance: " + ServiceSettings.load().getOTSInstanceName());

        ListTableResult r = ots.listTable();

        for (String table : r.getTableNames()) {
            DeleteTableRequest deleteTableRequest = new DeleteTableRequest(table);
            ots.deleteTable(deleteTableRequest);
            LOG.info("Delete table: " + table);

            Thread.sleep(1000);
        }
    }

    private void VerifyRow(Row testRow, Row goldRow) {
        checkRow(goldRow, testRow);
    }

    /**
     * | pk   | col1 | col2       |
     * |------+------+------------|
     * | row1 | ABC  | DON'T CARE |
     * | row2 | ABC  | DON'T CARE |
     * | row3 | ABC  | DON'T CARE |
     * | row4 | ABB  | DON'T CARE |
     * | row5 | ABD  | DON'T CARE |
     */
    private HashMap<String, Row> WriteFixture4Case1() {
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("pk", PrimaryKeyType.STRING);

        OTSHelper.createTable(ots, tableMeta);
        Utils.sleepSeconds(SECONDS_UNTIL_TABLE_READY);

        HashMap<String, Row> fixture = new HashMap<String, Row>();
        for (int i = 1; i <= 5; ++i) {
            String rowKey = "row" + i;
            RowPrimaryKey pk = new RowPrimaryKey()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(rowKey));

            String value = "Uninitialized";
            if (i <= 3) {
                value = "ABC";
            } else if (i == 4) {
                value = "ABB";
            } else if (i == 5) {
                value = "ABD";
            }

            Map<String, ColumnValue> puts = new HashMap<String, ColumnValue>();
            puts.put("col1", ColumnValue.fromString(value));
            puts.put("col2", ColumnValue.fromString("DON'T CARE"));
            OTSHelper.updateRow(ots, tableName, pk, puts, null);
            Row row = new Row();
            row.addColumn("pk", ColumnValue.fromString(rowKey));
            row.addColumn("col1", ColumnValue.fromString(value));
            row.addColumn("col2", ColumnValue.fromString("DON'T CARE"));
            fixture.put(rowKey, row);
        }
        return fixture;
    }

    /**
     * Test relation filter with STRING type
     * Test filters with the following combinations:
     * ["col1"] x [=, !=, >, >=, <, <=] x ["ABC", "ABB", "ABD"]
     * for GetRow/BatchGetRow/GetRange
     */
    @Test
    public void testCase1() throws UnsupportedEncodingException {
        LOG.info("Start testCase1");

        long ts = (new Date()).getTime();
        LOG.info("Begin time stamp : " + ts);

        HashMap<String, Row> fixture = WriteFixture4Case1();

        RowPrimaryKey pk = new RowPrimaryKey()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row1"));

        // = "ABC" => PASS
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);
        criteria.setPrimaryKey(pk);
         
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromString("ABC")));

        Row row = OTSHelper.getRow(ots, criteria).getRow();
        VerifyRow(row, fixture.get("row1"));

        // != "ABC" => DROP
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.NOT_EQUAL, ColumnValue.fromString("ABC")));
        row = OTSHelper.getRow(ots, criteria).getRow();
        checkRow(new Row(), row);

        // > "ABC" => DROP
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_THAN, ColumnValue.fromString("ABC")));
        row = OTSHelper.getRow(ots, criteria).getRow();
        checkRow(new Row(), row);

        // >= "ABC" => PASS
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_EQUAL, ColumnValue.fromString("ABC")));
        row = OTSHelper.getRow(ots, criteria).getRow();
        VerifyRow(row, fixture.get("row1"));

        // < "ABC" => DROP
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN, ColumnValue.fromString("ABC")));
        row = OTSHelper.getRow(ots, criteria).getRow();
        checkRow(new Row(), row);

        // <= "ABC" => PASS
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_EQUAL, ColumnValue.fromString("ABC")));
        row = OTSHelper.getRow(ots, criteria).getRow();
        VerifyRow(row, fixture.get("row1"));

        // > "AB" AND < "ABCD" => PASS
        CompositeCondition compositeFilter = new CompositeCondition(CompositeCondition.LogicOperator.AND);
        compositeFilter.addCondition(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_THAN, ColumnValue.fromString("AB")));
        compositeFilter.addCondition(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN, ColumnValue.fromString("ABCD")));
        row = OTSHelper.getRow(ots, criteria).getRow();
        VerifyRow(row, fixture.get("row1"));

        // BatchGetRow (Get row1, row4, row5)
        // = "ABC" => row1
        List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
        MultiRowQueryCriteria c = new MultiRowQueryCriteria(tableName);
         
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromString("ABC")));
        c.addRow(new RowPrimaryKey()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row1"))
                );
        c.addRow(new RowPrimaryKey()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row4"))
                );
        c.addRow(new RowPrimaryKey()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row5"))
                );
        criterias.add(c);
        List< BatchGetRowResult.RowStatus> result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getTableName(), tableName);
        VerifyRow(result.get(0).getRow(), fixture.get("row1"));
        assertEquals(result.get(1).getRow().getColumns().size(), 0);
        assertEquals(result.get(2).getRow().getColumns().size(), 0);

        // != "ABC" => row4, row5
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.NOT_EQUAL, ColumnValue.fromString("ABC")));
        result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getRow().getColumns().size(), 0);
        assertEquals(result.get(1).getTableName(), tableName);
        VerifyRow(result.get(1).getRow(), fixture.get("row4"));
        assertEquals(result.get(2).getTableName(), tableName);
        VerifyRow(result.get(2).getRow(), fixture.get("row5"));

        // > "ABC" => row5
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_THAN, ColumnValue.fromString("ABC")));
        result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getRow().getColumns().size(), 0);
        assertEquals(result.get(1).getRow().getColumns().size(), 0);
        assertEquals(result.get(2).getTableName(), tableName);
        VerifyRow(result.get(2).getRow(), fixture.get("row5"));

        // >= "ABC" => row1, row5
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_EQUAL, ColumnValue.fromString("ABC")));
        result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getTableName(), tableName);
        VerifyRow(result.get(0).getRow(), fixture.get("row1"));
        assertEquals(result.get(1).getRow().getColumns().size(), 0);
        assertEquals(result.get(2).getTableName(), tableName);
        VerifyRow(result.get(2).getRow(), fixture.get("row5"));

        // < "ABC" => row4
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN, ColumnValue.fromString("ABC")));
        result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getRow().getColumns().size(), 0);
        assertEquals(result.get(1).getTableName(), tableName);
        VerifyRow(result.get(1).getRow(), fixture.get("row4"));
        assertEquals(result.get(2).getRow().getColumns().size(), 0);

        // <= "ABC" => row1, row4
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_EQUAL, ColumnValue.fromString("ABC")));
        result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getTableName(), tableName);
        VerifyRow(result.get(0).getRow(), fixture.get("row1"));
        assertEquals(result.get(1).getTableName(), tableName);
        VerifyRow(result.get(1).getRow(), fixture.get("row4"));
        assertEquals(result.get(2).getRow().getColumns().size(), 0);

        // GetRange [row2, row6)
        // = "ABC" => row2, row3
        RangeRowQueryCriteria rangeCriteria = new RangeRowQueryCriteria(tableName);
        rangeCriteria.setInclusiveStartPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row2"))
                        );
        rangeCriteria.setExclusiveEndPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row6"))
                        );
         
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromString("ABC")));
        List<Row> rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 2);
        VerifyRow(rows.get(0), fixture.get("row2"));
        VerifyRow(rows.get(1), fixture.get("row3"));

        // != "ABC" => row4, row5
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.NOT_EQUAL, ColumnValue.fromString("ABC")));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 2);
        VerifyRow(rows.get(0), fixture.get("row4"));
        VerifyRow(rows.get(1), fixture.get("row5"));

        // > "ABC" => row5
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_THAN, ColumnValue.fromString("ABC")));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 1);
        VerifyRow(rows.get(0), fixture.get("row5"));

        // >= "ABC" => row2, row3, row5
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_EQUAL, ColumnValue.fromString("ABC")));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 3);
        VerifyRow(rows.get(0), fixture.get("row2"));
        VerifyRow(rows.get(1), fixture.get("row3"));
        VerifyRow(rows.get(2), fixture.get("row5"));

        // < "ABC" => row4
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN, ColumnValue.fromString("ABC")));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 1);
        VerifyRow(rows.get(0), fixture.get("row4"));

        // <= "ABC" => row2, row3, row4
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_EQUAL, ColumnValue.fromString("ABC")));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 3);
        VerifyRow(rows.get(0), fixture.get("row2"));
        VerifyRow(rows.get(1), fixture.get("row3"));
        VerifyRow(rows.get(2), fixture.get("row4"));

        // GetRangeReverse [row5, row1)
        // = "ABC" => row3, row2
        rangeCriteria.setInclusiveStartPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row5"))
                        );
        rangeCriteria.setExclusiveEndPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row1"))
                        );
        rangeCriteria.setDirection(Direction.BACKWARD);
         
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromString("ABC")));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 2);
        VerifyRow(rows.get(0), fixture.get("row3"));
        VerifyRow(rows.get(1), fixture.get("row2"));

        // != "ABC" => row4, row4
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.NOT_EQUAL, ColumnValue.fromString("ABC")));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 2);
        VerifyRow(rows.get(0), fixture.get("row5"));
        VerifyRow(rows.get(1), fixture.get("row4"));

        // > "ABC" => row5
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_THAN, ColumnValue.fromString("ABC")));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 1);
        VerifyRow(rows.get(0), fixture.get("row5"));

        // >= "ABC" => row5, row3, row2
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_EQUAL, ColumnValue.fromString("ABC")));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 3);
        VerifyRow(rows.get(0), fixture.get("row5"));
        VerifyRow(rows.get(1), fixture.get("row3"));
        VerifyRow(rows.get(2), fixture.get("row2"));

        // < "ABC" => row4
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN, ColumnValue.fromString("ABC")));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 1);
        VerifyRow(rows.get(0), fixture.get("row4"));

        // <= "ABC" => row4, row3, row2
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_EQUAL, ColumnValue.fromString("ABC")));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 3);
        VerifyRow(rows.get(0), fixture.get("row4"));
        VerifyRow(rows.get(1), fixture.get("row3"));
        VerifyRow(rows.get(2), fixture.get("row2"));
    }

    /**
     * | pk   | col1 | col2       |
     * |------+------+------------|
     * | row1 | 100  | DON'T CARE |
     * | row2 | 100  | DON'T CARE |
     * | row3 | 100  | DON'T CARE |
     * | row4 | -1   | DON'T CARE |
     * | row5 | 1000 | DON'T CARE |
     */
    private HashMap<String, Row> WriteFixture4Case2() {
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("pk", PrimaryKeyType.STRING);

        OTSHelper.createTable(ots, tableMeta);
        Utils.sleepSeconds(SECONDS_UNTIL_TABLE_READY);

        HashMap<String, Row> fixture = new HashMap<String, Row>();
        for (int i = 1; i <= 5; ++i) {
            String rowKey = "row" + i;
            RowPrimaryKey pk = new RowPrimaryKey()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(rowKey));

            long value = 0;
            if (i <= 3) {
                value = 100;
            } else if (i == 4) {
                value = -1;
            } else if (i == 5) {
                value = 1000;
            }

            Map<String, ColumnValue> puts = new HashMap<String, ColumnValue>();
            puts.put("col1", ColumnValue.fromLong(value));
            puts.put("col2", ColumnValue.fromString("DON'T CARE"));
            OTSHelper.updateRow(ots, tableName, pk, puts, null);
            Row row = new Row();
            row.addColumn("pk", ColumnValue.fromString(rowKey));
            row.addColumn("col1", ColumnValue.fromLong(value));
            row.addColumn("col2", ColumnValue.fromString("DON'T CARE"));
            fixture.put(rowKey, row);
        }
        return fixture;
    }

    /**
     * Test relation filter with INTEGER type
     * Test filters with the following combinations:
     * ["col1"] x [=, !=, >, >=, <, <=] x [100, 99, 1000]
     * for GetRow/BatchGetRow/GetRange
     */
    @Test
    public void testCase2() throws UnsupportedEncodingException {
        LOG.info("Start testCase2");

        long ts = (new Date()).getTime();
        LOG.info("Begin time stamp : " + ts);

        HashMap<String, Row> fixture = WriteFixture4Case2();

        RowPrimaryKey pk = new RowPrimaryKey()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row1"));

        // = 100 => PASS
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);
        criteria.setPrimaryKey(pk);
         
        criteria.setFilter(new RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromLong(100)));

        Row row = OTSHelper.getRow(ots, criteria).getRow();
        VerifyRow(row, fixture.get("row1"));

        // != 100 => DROP
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.NOT_EQUAL, ColumnValue.fromLong(100)));
        row = OTSHelper.getRow(ots, criteria).getRow();
        checkRow(new Row(), row);

        // > 100 => DROP
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_THAN, ColumnValue.fromLong(100)));
        row = OTSHelper.getRow(ots, criteria).getRow();
        checkRow(new Row(), row);

        // >= 100 => PASS
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_EQUAL, ColumnValue.fromLong(100)));
        row = OTSHelper.getRow(ots, criteria).getRow();
        VerifyRow(row, fixture.get("row1"));

        // < 100 => DROP
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN, ColumnValue.fromLong(100)));
        row = OTSHelper.getRow(ots, criteria).getRow();
        checkRow(new Row(), row);

        // <= 100 => PASS
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_EQUAL, ColumnValue.fromLong(100)));
        row = OTSHelper.getRow(ots, criteria).getRow();
        VerifyRow(row, fixture.get("row1"));

        // row4: col1 < 0 => PASS
        criteria.setPrimaryKey(new RowPrimaryKey()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row4"))
                );
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN, ColumnValue.fromLong(0)));
        row = OTSHelper.getRow(ots, criteria).getRow();
        VerifyRow(row, fixture.get("row4"));

        // row4: col1 < 1 => PASS
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN, ColumnValue.fromLong(1)));
        row = OTSHelper.getRow(ots, criteria).getRow();
        VerifyRow(row, fixture.get("row4"));

        // row4: col1 >= 0 => DROP
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_EQUAL, ColumnValue.fromLong(0)));
        row = OTSHelper.getRow(ots, criteria).getRow();
        checkRow(new Row(), row);

        // row4: col1 >= 1 => DROP
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_EQUAL, ColumnValue.fromLong(1)));
        row = OTSHelper.getRow(ots, criteria).getRow();
        checkRow(new Row(), row);

        // BatchGetRow (Get row1, row4, row5)
        // = 100 => row1
        List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
        MultiRowQueryCriteria c = new MultiRowQueryCriteria(tableName);
         
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromLong(100)));
        c.addRow(new RowPrimaryKey()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row1"))
                );
        c.addRow(new RowPrimaryKey()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row4"))
                );
        c.addRow(new RowPrimaryKey()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row5"))
                );
        criterias.add(c);
        List< BatchGetRowResult.RowStatus> result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getTableName(), tableName);
        VerifyRow(result.get(0).getRow(), fixture.get("row1"));
        assertEquals(result.get(1).getRow().getColumns().size(), 0);
        assertEquals(result.get(2).getRow().getColumns().size(), 0);

        // != 100 => row4, row5
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.NOT_EQUAL, ColumnValue.fromLong(100)));
        result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getRow().getColumns().size(), 0);
        assertEquals(result.get(1).getTableName(), tableName);
        VerifyRow(result.get(1).getRow(), fixture.get("row4"));
        assertEquals(result.get(2).getTableName(), tableName);
        VerifyRow(result.get(2).getRow(), fixture.get("row5"));

        // > 100 => row5
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_THAN, ColumnValue.fromLong(100)));
        result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getRow().getColumns().size(), 0);
        assertEquals(result.get(1).getRow().getColumns().size(), 0);
        assertEquals(result.get(2).getTableName(), tableName);
        VerifyRow(result.get(2).getRow(), fixture.get("row5"));

        // >= 100 => row1, row5
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_EQUAL, ColumnValue.fromLong(100)));
        result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getTableName(), tableName);
        VerifyRow(result.get(0).getRow(), fixture.get("row1"));
        assertEquals(result.get(1).getRow().getColumns().size(), 0);
        assertEquals(result.get(2).getTableName(), tableName);
        VerifyRow(result.get(2).getRow(), fixture.get("row5"));

        // < 100 => row4
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN, ColumnValue.fromLong(100)));
        result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getRow().getColumns().size(), 0);
        assertEquals(result.get(1).getTableName(), tableName);
        VerifyRow(result.get(1).getRow(), fixture.get("row4"));
        assertEquals(result.get(2).getRow().getColumns().size(), 0);

        // <= 100 => row1, row4
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_EQUAL, ColumnValue.fromLong(100)));
        result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getTableName(), tableName);
        VerifyRow(result.get(0).getRow(), fixture.get("row1"));
        assertEquals(result.get(1).getTableName(), tableName);
        VerifyRow(result.get(1).getRow(), fixture.get("row4"));
        assertEquals(result.get(2).getRow().getColumns().size(), 0);

        // GetRange [row2, row6)
        // = 100 => row2, row3
        RangeRowQueryCriteria rangeCriteria = new RangeRowQueryCriteria(tableName);
        rangeCriteria.setInclusiveStartPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row2"))
                        );
        rangeCriteria.setExclusiveEndPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row6"))
                        );
         
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromLong(100)));
        List<Row> rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 2);
        VerifyRow(rows.get(0), fixture.get("row2"));
        VerifyRow(rows.get(1), fixture.get("row3"));

        // != 100 => row4, row5
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.NOT_EQUAL, ColumnValue.fromLong(100)));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 2);
        VerifyRow(rows.get(0), fixture.get("row4"));
        VerifyRow(rows.get(1), fixture.get("row5"));

        // > 100 => row5
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_THAN, ColumnValue.fromLong(100)));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 1);
        VerifyRow(rows.get(0), fixture.get("row5"));

        // >= 100 => row2, row3, row5
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_EQUAL, ColumnValue.fromLong(100)));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 3);
        VerifyRow(rows.get(0), fixture.get("row2"));
        VerifyRow(rows.get(1), fixture.get("row3"));
        VerifyRow(rows.get(2), fixture.get("row5"));

        // < 100 => row4
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN, ColumnValue.fromLong(100)));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 1);
        VerifyRow(rows.get(0), fixture.get("row4"));

        // <= 100 => row2, row3, row4
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_EQUAL, ColumnValue.fromLong(100)));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 3);
        VerifyRow(rows.get(0), fixture.get("row2"));
        VerifyRow(rows.get(1), fixture.get("row3"));
        VerifyRow(rows.get(2), fixture.get("row4"));

        // GetRangeReverse [row5, row1)
        // = 100 => row3, row2
        rangeCriteria.setInclusiveStartPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row5"))
                        );
        rangeCriteria.setExclusiveEndPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row1"))
                        );
        rangeCriteria.setDirection(Direction.BACKWARD);
         
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromLong(100)));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 2);
        VerifyRow(rows.get(0), fixture.get("row3"));
        VerifyRow(rows.get(1), fixture.get("row2"));

        // != 100 => row4, row4
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.NOT_EQUAL, ColumnValue.fromLong(100)));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 2);
        VerifyRow(rows.get(0), fixture.get("row5"));
        VerifyRow(rows.get(1), fixture.get("row4"));

        // > 100 => row5
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_THAN, ColumnValue.fromLong(100)));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 1);
        VerifyRow(rows.get(0), fixture.get("row5"));

        // >= 100 => row5, row3, row2
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_EQUAL, ColumnValue.fromLong(100)));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 3);
        VerifyRow(rows.get(0), fixture.get("row5"));
        VerifyRow(rows.get(1), fixture.get("row3"));
        VerifyRow(rows.get(2), fixture.get("row2"));

        // < 100 => row4
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN, ColumnValue.fromLong(100)));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 1);
        VerifyRow(rows.get(0), fixture.get("row4"));

        // <= 100 => row4, row3, row2
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_EQUAL, ColumnValue.fromLong(100)));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 3);
        VerifyRow(rows.get(0), fixture.get("row4"));
        VerifyRow(rows.get(1), fixture.get("row3"));
        VerifyRow(rows.get(2), fixture.get("row2"));
    }

    /**
     * | pk   | col1   | col2       |
     * |------+--------+------------|
     * | row1 | 100.1  | DON'T CARE |
     * | row2 | 100.1  | DON'T CARE |
     * | row3 | 100.1  | DON'T CARE |
     * | row4 | 99.9   | DON'T CARE |
     * | row5 | 1000.5 | DON'T CARE |
     */
    private HashMap<String, Row> WriteFixture4Case3() {
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("pk", PrimaryKeyType.STRING);

        OTSHelper.createTable(ots, tableMeta);
        Utils.sleepSeconds(SECONDS_UNTIL_TABLE_READY);

        HashMap<String, Row> fixture = new HashMap<String, Row>();
        for (int i = 1; i <= 5; ++i) {
            String rowKey = "row" + i;
            RowPrimaryKey pk = new RowPrimaryKey()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(rowKey));


            double value = 0;
            if (i <= 3) {
                value = 100.1;
            } else if (i == 4) {
                value = 99.9;
            } else if (i == 5) {
                value = 1000.5;
            }

            Map<String, ColumnValue> puts = new HashMap<String, ColumnValue>();
            puts.put("col1", ColumnValue.fromDouble(value));
            puts.put("col2", ColumnValue.fromString("DON'T CARE"));
            OTSHelper.updateRow(ots, tableName, pk, puts, null);
            Row row = new Row();
            row.addColumn("pk", ColumnValue.fromString(rowKey));
            row.addColumn("col1", ColumnValue.fromDouble(value));
            row.addColumn("col2", ColumnValue.fromString("DON'T CARE"));
            fixture.put(rowKey, row);
        }
        return fixture;
    }

    /**
     * Test relation filter with DOUBLE type
     * Test filters with the following combinations:
     * ["col1"] x [=, !=, >, >=, <, <=] x [100.1, 99.9, 1000.5]
     * for GetRow/BatchGetRow/GetRange
     */
    @Test
    public void testCase3() throws UnsupportedEncodingException {
        LOG.info("Start testCase3");

        long ts = (new Date()).getTime();
        LOG.info("Begin time stamp : " + ts);

        HashMap<String, Row> fixture = WriteFixture4Case3();

        RowPrimaryKey pk = new RowPrimaryKey()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row1"));

        // = 100.1 => PASS
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);
        criteria.setPrimaryKey(pk);
         
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromDouble(100.1)));

        Row row = OTSHelper.getRow(ots, criteria).getRow();
        VerifyRow(row, fixture.get("row1"));

        // != 100.1 => DROP
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.NOT_EQUAL, ColumnValue.fromDouble(100.1)));
        row = OTSHelper.getRow(ots, criteria).getRow();
        checkRow(new Row(), row);

        // > 100.1 => DROP
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_THAN, ColumnValue.fromDouble(100.1)));
        row = OTSHelper.getRow(ots, criteria).getRow();
        checkRow(new Row(), row);

        // >= 100.1 => PASS
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_EQUAL, ColumnValue.fromDouble(100.1)));
        row = OTSHelper.getRow(ots, criteria).getRow();
        VerifyRow(row, fixture.get("row1"));

        // < 100.1 => DROP
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN, ColumnValue.fromDouble(100.1)));
        row = OTSHelper.getRow(ots, criteria).getRow();
        checkRow(new Row(), row);

        // <= 100.1 => PASS
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_EQUAL, ColumnValue.fromDouble(100.1)));
        row = OTSHelper.getRow(ots, criteria).getRow();
        VerifyRow(row, fixture.get("row1"));

        // BatchGetRow (Get row1, row4, row5)
        // = 100.1 => row1
        List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
        MultiRowQueryCriteria c = new MultiRowQueryCriteria(tableName);
         
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromDouble(100.1)));
        c.addRow(new RowPrimaryKey()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row1"))
                );
        c.addRow(new RowPrimaryKey()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row4"))
                );
        c.addRow(new RowPrimaryKey()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row5"))
                );
        criterias.add(c);
        List< BatchGetRowResult.RowStatus> result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getTableName(), tableName);
        VerifyRow(result.get(0).getRow(), fixture.get("row1"));
        assertEquals(result.get(1).getRow().getColumns().size(), 0);
        assertEquals(result.get(2).getRow().getColumns().size(), 0);

        // != 100.1 => row4, row5
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.NOT_EQUAL, ColumnValue.fromDouble(100.1)));
        result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getRow().getColumns().size(), 0);
        assertEquals(result.get(1).getTableName(), tableName);
        VerifyRow(result.get(1).getRow(), fixture.get("row4"));
        assertEquals(result.get(2).getTableName(), tableName);
        VerifyRow(result.get(2).getRow(), fixture.get("row5"));

        // > 100.1 => row5
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_THAN, ColumnValue.fromDouble(100.1)));
        result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getRow().getColumns().size(), 0);
        assertEquals(result.get(1).getRow().getColumns().size(), 0);
        assertEquals(result.get(2).getTableName(), tableName);
        VerifyRow(result.get(2).getRow(), fixture.get("row5"));

        // >= 100.1 => row1, row5
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_EQUAL, ColumnValue.fromDouble(100.1)));
        result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getTableName(), tableName);
        VerifyRow(result.get(0).getRow(), fixture.get("row1"));
        assertEquals(result.get(1).getRow().getColumns().size(), 0);
        assertEquals(result.get(2).getTableName(), tableName);
        VerifyRow(result.get(2).getRow(), fixture.get("row5"));

        // < 100.1 => row4
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN, ColumnValue.fromDouble(100.1)));
        result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getRow().getColumns().size(), 0);
        assertEquals(result.get(1).getTableName(), tableName);
        VerifyRow(result.get(1).getRow(), fixture.get("row4"));
        assertEquals(result.get(2).getRow().getColumns().size(), 0);

        // <= 100.1 => row1, row4
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_EQUAL, ColumnValue.fromDouble(100.1)));
        result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getTableName(), tableName);
        VerifyRow(result.get(0).getRow(), fixture.get("row1"));
        assertEquals(result.get(1).getTableName(), tableName);
        VerifyRow(result.get(1).getRow(), fixture.get("row4"));
        assertEquals(result.get(2).getRow().getColumns().size(), 0);

        // GetRange [row2, row6)
        // = 100.1 => row2, row3
        RangeRowQueryCriteria rangeCriteria = new RangeRowQueryCriteria(tableName);
        rangeCriteria.setInclusiveStartPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row2"))
                        );
        rangeCriteria.setExclusiveEndPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row6"))
                        );
         
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromDouble(100.1)));
        List<Row> rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 2);
        VerifyRow(rows.get(0), fixture.get("row2"));
        VerifyRow(rows.get(1), fixture.get("row3"));

        // != 100.1 => row4, row5
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.NOT_EQUAL, ColumnValue.fromDouble(100.1)));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 2);
        VerifyRow(rows.get(0), fixture.get("row4"));
        VerifyRow(rows.get(1), fixture.get("row5"));

        // > 100.1 => row5
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_THAN, ColumnValue.fromDouble(100.1)));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 1);
        VerifyRow(rows.get(0), fixture.get("row5"));

        // >= 100.1 => row2, row3, row5
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_EQUAL, ColumnValue.fromDouble(100.1)));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 3);
        VerifyRow(rows.get(0), fixture.get("row2"));
        VerifyRow(rows.get(1), fixture.get("row3"));
        VerifyRow(rows.get(2), fixture.get("row5"));

        // < 100.1 => row4
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN, ColumnValue.fromDouble(100.1)));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 1);
        VerifyRow(rows.get(0), fixture.get("row4"));

        // <= 100.1 => row2, row3, row4
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_EQUAL, ColumnValue.fromDouble(100.1)));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 3);
        VerifyRow(rows.get(0), fixture.get("row2"));
        VerifyRow(rows.get(1), fixture.get("row3"));
        VerifyRow(rows.get(2), fixture.get("row4"));

        // GetRangeReverse [row5, row1)
        // = 100.1 => row3, row2
        rangeCriteria.setInclusiveStartPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row5"))
                        );
        rangeCriteria.setExclusiveEndPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row1"))
                        );
        rangeCriteria.setDirection(Direction.BACKWARD);
         
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromDouble(100.1)));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 2);
        VerifyRow(rows.get(0), fixture.get("row3"));
        VerifyRow(rows.get(1), fixture.get("row2"));

        // != 100.1 => row4, row4
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.NOT_EQUAL, ColumnValue.fromDouble(100.1)));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 2);
        VerifyRow(rows.get(0), fixture.get("row5"));
        VerifyRow(rows.get(1), fixture.get("row4"));

        // > 100.1 => row5
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_THAN, ColumnValue.fromDouble(100.1)));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 1);
        VerifyRow(rows.get(0), fixture.get("row5"));

        // >= 100.1 => row5, row3, row2
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_EQUAL, ColumnValue.fromDouble(100.1)));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 3);
        VerifyRow(rows.get(0), fixture.get("row5"));
        VerifyRow(rows.get(1), fixture.get("row3"));
        VerifyRow(rows.get(2), fixture.get("row2"));

        // < 100.1 => row4
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN, ColumnValue.fromDouble(100.1)));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 1);
        VerifyRow(rows.get(0), fixture.get("row4"));

        // <= 100.1 => row4, row3, row2
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_EQUAL, ColumnValue.fromDouble(100.1)));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 3);
        VerifyRow(rows.get(0), fixture.get("row4"));
        VerifyRow(rows.get(1), fixture.get("row3"));
        VerifyRow(rows.get(2), fixture.get("row2"));
    }

    /**
     * | pk   | col1 | col2       |
     * |------+------+------------|
     * | row1 | ABC  | DON'T CARE |
     * | row2 | ABC  | DON'T CARE |
     * | row3 | ABC  | DON'T CARE |
     * | row4 | ABB  | DON'T CARE |
     * | row5 | ABD  | DON'T CARE |
     */
    private HashMap<String, Row> WriteFixture4Case4() {
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("pk", PrimaryKeyType.STRING);

        OTSHelper.createTable(ots, tableMeta);
        Utils.sleepSeconds(SECONDS_UNTIL_TABLE_READY);

        HashMap<String, Row> fixture = new HashMap<String, Row>();
        for (int i = 1; i <= 5; ++i) {
            String rowKey = "row" + i;
            RowPrimaryKey pk = new RowPrimaryKey()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(rowKey));


            String value = "Uninitialized";
            if (i <= 3) {
                value = "ABC";
            } else if (i == 4) {
                value = "ABB";
            } else if (i == 5) {
                value = "ABD";
            }

            Map<String, ColumnValue> puts = new HashMap<String, ColumnValue>();
            puts.put("col1", ColumnValue.fromBinary(value.getBytes()));
            puts.put("col2", ColumnValue.fromString("DON'T CARE"));
            OTSHelper.updateRow(ots, tableName, pk, puts, null);
            Row row = new Row();
            row.addColumn("pk", ColumnValue.fromString(rowKey));
            row.addColumn("col1", ColumnValue.fromBinary(value.getBytes()));
            row.addColumn("col2", ColumnValue.fromString("DON'T CARE"));
            fixture.put(rowKey, row);
        }
        return fixture;
    }

    /**
     * Test relation filter with BINARY type
     * Test filters with the following combinations:
     * ["col1"] x [=, !=, >, >=, <, <=] x ["ABC", "ABB", "ABD"]
     * for GetRow/BatchGetRow/GetRange
     */
    @Test
    public void testCase4() throws UnsupportedEncodingException {
        LOG.info("Start testCase4");

        long ts = (new Date()).getTime();
        LOG.info("Begin time stamp : " + ts);

        HashMap<String, Row> fixture = WriteFixture4Case4();

        RowPrimaryKey pk = new RowPrimaryKey()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row1"));

        // = "ABC" => PASS
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);
        criteria.setPrimaryKey(pk);
         
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL,
                ColumnValue.fromBinary(new String("ABC").getBytes())));

        Row row = OTSHelper.getRow(ots, criteria).getRow();
        VerifyRow(row, fixture.get("row1"));

        // != "ABC" => DROP
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.NOT_EQUAL,
                ColumnValue.fromBinary(new String("ABC").getBytes())));
        row = OTSHelper.getRow(ots, criteria).getRow();
        checkRow(new Row(), row);

        // > "ABC" => DROP
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_THAN,
                ColumnValue.fromBinary(new String("ABC").getBytes())));
        row = OTSHelper.getRow(ots, criteria).getRow();
        checkRow(new Row(), row);

        // >= "ABC" => PASS
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_EQUAL,
                ColumnValue.fromBinary(new String("ABC").getBytes())));
        row = OTSHelper.getRow(ots, criteria).getRow();
        VerifyRow(row, fixture.get("row1"));

        // < "ABC" => DROP
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN,
                ColumnValue.fromBinary(new String("ABC").getBytes())));
        row = OTSHelper.getRow(ots, criteria).getRow();
        checkRow(new Row(), row);

        // <= "ABC" => PASS
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_EQUAL,
                ColumnValue.fromBinary(new String("ABC").getBytes())));
        row = OTSHelper.getRow(ots, criteria).getRow();
        VerifyRow(row, fixture.get("row1"));

        // BatchGetRow (Get row1, row4, row5)
        // = "ABC" => row1
        List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
        MultiRowQueryCriteria c = new MultiRowQueryCriteria(tableName);
         
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL,
                ColumnValue.fromBinary(new String("ABC").getBytes())));
        c.addRow(new RowPrimaryKey()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row1"))
                );
        c.addRow(new RowPrimaryKey()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row4"))
                );
        c.addRow(new RowPrimaryKey()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row5"))
                );
        criterias.add(c);
        List< BatchGetRowResult.RowStatus> result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getTableName(), tableName);
        VerifyRow(result.get(0).getRow(), fixture.get("row1"));
        assertEquals(result.get(1).getRow().getColumns().size(), 0);
        assertEquals(result.get(2).getRow().getColumns().size(), 0);

        // != "ABC" => row4, row5
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.NOT_EQUAL,
                ColumnValue.fromBinary(new String("ABC").getBytes())));
        result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getRow().getColumns().size(), 0);
        assertEquals(result.get(1).getTableName(), tableName);
        VerifyRow(result.get(1).getRow(), fixture.get("row4"));
        assertEquals(result.get(2).getTableName(), tableName);
        VerifyRow(result.get(2).getRow(), fixture.get("row5"));

        // > "ABC" => row5
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_THAN,
                ColumnValue.fromBinary(new String("ABC").getBytes())));
        result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getRow().getColumns().size(), 0);
        assertEquals(result.get(1).getRow().getColumns().size(), 0);
        assertEquals(result.get(2).getTableName(), tableName);
        VerifyRow(result.get(2).getRow(), fixture.get("row5"));

        // >= "ABC" => row1, row5
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_EQUAL,
                ColumnValue.fromBinary(new String("ABC").getBytes())));
        result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getTableName(), tableName);
        VerifyRow(result.get(0).getRow(), fixture.get("row1"));
        assertEquals(result.get(1).getRow().getColumns().size(), 0);
        assertEquals(result.get(2).getTableName(), tableName);
        VerifyRow(result.get(2).getRow(), fixture.get("row5"));

        // < "ABC" => row4
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN,
                ColumnValue.fromBinary(new String("ABC").getBytes())));
        result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getRow().getColumns().size(), 0);
        assertEquals(result.get(1).getTableName(), tableName);
        VerifyRow(result.get(1).getRow(), fixture.get("row4"));
        assertEquals(result.get(2).getRow().getColumns().size(), 0);

        // <= "ABC" => row1, row4
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_EQUAL,
                ColumnValue.fromBinary(new String("ABC").getBytes())));
        result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getTableName(), tableName);
        VerifyRow(result.get(0).getRow(), fixture.get("row1"));
        assertEquals(result.get(1).getTableName(), tableName);
        VerifyRow(result.get(1).getRow(), fixture.get("row4"));
        assertEquals(result.get(2).getRow().getColumns().size(), 0);

        // GetRange [row2, row6)
        // = "ABC" => row2, row3
        RangeRowQueryCriteria rangeCriteria = new RangeRowQueryCriteria(tableName);
        rangeCriteria.setInclusiveStartPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row2"))
                        );
        rangeCriteria.setExclusiveEndPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row6"))
                        );
         
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL,
                ColumnValue.fromBinary(new String("ABC").getBytes())));
        List<Row> rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 2);
        VerifyRow(rows.get(0), fixture.get("row2"));
        VerifyRow(rows.get(1), fixture.get("row3"));

        // != "ABC" => row4, row5
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.NOT_EQUAL,
                ColumnValue.fromBinary(new String("ABC").getBytes())));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 2);
        VerifyRow(rows.get(0), fixture.get("row4"));
        VerifyRow(rows.get(1), fixture.get("row5"));

        // > "ABC" => row5
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_THAN,
                ColumnValue.fromBinary(new String("ABC").getBytes())));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 1);
        VerifyRow(rows.get(0), fixture.get("row5"));

        // >= "ABC" => row2, row3, row5
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_EQUAL,
                ColumnValue.fromBinary(new String("ABC").getBytes())));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 3);
        VerifyRow(rows.get(0), fixture.get("row2"));
        VerifyRow(rows.get(1), fixture.get("row3"));
        VerifyRow(rows.get(2), fixture.get("row5"));

        // < "ABC" => row4
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN,
                ColumnValue.fromBinary(new String("ABC").getBytes())));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 1);
        VerifyRow(rows.get(0), fixture.get("row4"));

        // <= "ABC" => row2, row3, row4
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_EQUAL,
                ColumnValue.fromBinary(new String("ABC").getBytes())));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 3);
        VerifyRow(rows.get(0), fixture.get("row2"));
        VerifyRow(rows.get(1), fixture.get("row3"));
        VerifyRow(rows.get(2), fixture.get("row4"));

        // GetRangeReverse [row5, row1)
        // = "ABC" => row3, row2
        rangeCriteria.setInclusiveStartPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row5"))
                        );
        rangeCriteria.setExclusiveEndPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row1"))
                        );
        rangeCriteria.setDirection(Direction.BACKWARD);
         
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL,
                ColumnValue.fromBinary(new String("ABC").getBytes())));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 2);
        VerifyRow(rows.get(0), fixture.get("row3"));
        VerifyRow(rows.get(1), fixture.get("row2"));

        // != "ABC" => row4, row4
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.NOT_EQUAL,
                ColumnValue.fromBinary(new String("ABC").getBytes())));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 2);
        VerifyRow(rows.get(0), fixture.get("row5"));
        VerifyRow(rows.get(1), fixture.get("row4"));

        // > "ABC" => row5
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_THAN,
                ColumnValue.fromBinary(new String("ABC").getBytes())));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 1);
        VerifyRow(rows.get(0), fixture.get("row5"));

        // >= "ABC" => row5, row3, row2
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_EQUAL,
                ColumnValue.fromBinary(new String("ABC").getBytes())));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 3);
        VerifyRow(rows.get(0), fixture.get("row5"));
        VerifyRow(rows.get(1), fixture.get("row3"));
        VerifyRow(rows.get(2), fixture.get("row2"));

        // < "ABC" => row4
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN,
                ColumnValue.fromBinary(new String("ABC").getBytes())));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 1);
        VerifyRow(rows.get(0), fixture.get("row4"));

        // <= "ABC" => row4, row3, row2
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_EQUAL,
                ColumnValue.fromBinary(new String("ABC").getBytes())));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 3);
        VerifyRow(rows.get(0), fixture.get("row4"));
        VerifyRow(rows.get(1), fixture.get("row3"));
        VerifyRow(rows.get(2), fixture.get("row2"));
    }

    /**
     * | pk   | col1 | col2       |
     * |------+------+------------|
     * | row1 | 杭州 | DON'T CARE |
     * | row2 | 杭州 | DON'T CARE |
     * | row3 | 杭州 | DON'T CARE |
     * | row4 | 中国 | DON'T CARE |
     * | row5 | 浙江 | DON'T CARE |
     */
    private HashMap<String, Row> WriteFixture4Case5() {
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("pk", PrimaryKeyType.STRING);

        OTSHelper.createTable(ots, tableMeta);
        Utils.sleepSeconds(SECONDS_UNTIL_TABLE_READY);

        HashMap<String, Row> fixture = new HashMap<String, Row>();
        for (int i = 1; i <= 5; ++i) {
            String rowKey = "row" + i;
            RowPrimaryKey pk = new RowPrimaryKey()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(rowKey));


            String value = "Uninitialized";
            if (i <= 3) {
                value = "杭州";
            } else if (i == 4) {
                value = "中国";
            } else if (i == 5) {
                value = "浙江";
            }

            Map<String, ColumnValue> puts = new HashMap<String, ColumnValue>();
            puts.put("col1", ColumnValue.fromString(value));
            puts.put("col2", ColumnValue.fromString("DON'T CARE"));
            OTSHelper.updateRow(ots, tableName, pk, puts, null);
            Row row = new Row();
            row.addColumn("pk", ColumnValue.fromString(rowKey));
            row.addColumn("col1", ColumnValue.fromString(value));
            row.addColumn("col2", ColumnValue.fromString("DON'T CARE"));
            fixture.put(rowKey, row);
        }
        return fixture;
    }

    /**
     * Test relation filter with STRING type with Chinese character
     * Test filters with the following combinations:
     * ["col1"] x [=, !=, >, >=, <, <=] x ["杭州", "中国", "浙江"]
     * for GetRow/BatchGetRow/GetRange
     */
    @Test
    public void testCase5() throws UnsupportedEncodingException {
        LOG.info("Start testCase5");

        long ts = (new Date()).getTime();
        LOG.info("Begin time stamp : " + ts);

        HashMap<String, Row> fixture = WriteFixture4Case5();

        RowPrimaryKey pk = new RowPrimaryKey()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row1"));

        // = "杭州" => PASS
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);
        criteria.setPrimaryKey(pk);
         
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL,
                ColumnValue.fromString("杭州")));

        Row row = OTSHelper.getRow(ots, criteria).getRow();
        VerifyRow(row, fixture.get("row1"));

        // != "杭州" => DROP
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.NOT_EQUAL,
                ColumnValue.fromString("杭州")));
        row = OTSHelper.getRow(ots, criteria).getRow();
        checkRow(new Row(), row);

        // > "杭州" => DROP
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_THAN,
                ColumnValue.fromString("杭州")));
        row = OTSHelper.getRow(ots, criteria).getRow();
        checkRow(new Row(), row);

        // >= "杭州" => PASS
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_EQUAL,
                ColumnValue.fromString("杭州")));
        row = OTSHelper.getRow(ots, criteria).getRow();
        VerifyRow(row, fixture.get("row1"));

        // < "杭州" => DROP
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN,
                ColumnValue.fromString("杭州")));
        row = OTSHelper.getRow(ots, criteria).getRow();
        checkRow(new Row(), row);

        // <= "杭州" => PASS
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_EQUAL,
                ColumnValue.fromString("杭州")));
        row = OTSHelper.getRow(ots, criteria).getRow();
        VerifyRow(row, fixture.get("row1"));

        // BatchGetRow (Get row1, row4, row5)
        // = "杭州" => row1
        List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
        MultiRowQueryCriteria c = new MultiRowQueryCriteria(tableName);
         
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL,
                ColumnValue.fromString("杭州")));
        c.addRow(new RowPrimaryKey()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row1"))
                );
        c.addRow(new RowPrimaryKey()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row4"))
                );
        c.addRow(new RowPrimaryKey()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row5"))
                );
        criterias.add(c);
        List< BatchGetRowResult.RowStatus> result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getTableName(), tableName);
        VerifyRow(result.get(0).getRow(), fixture.get("row1"));
        assertEquals(result.get(1).getRow().getColumns().size(), 0);
        assertEquals(result.get(2).getRow().getColumns().size(), 0);

        // != "杭州" => row4, row5
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.NOT_EQUAL,
                ColumnValue.fromString("杭州")));
        result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getRow().getColumns().size(), 0);
        assertEquals(result.get(1).getTableName(), tableName);
        VerifyRow(result.get(1).getRow(), fixture.get("row4"));
        assertEquals(result.get(2).getTableName(), tableName);
        VerifyRow(result.get(2).getRow(), fixture.get("row5"));

        // > "杭州" => row5
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_THAN,
                ColumnValue.fromString("杭州")));
        result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getRow().getColumns().size(), 0);
        assertEquals(result.get(1).getRow().getColumns().size(), 0);
        assertEquals(result.get(2).getTableName(), tableName);
        VerifyRow(result.get(2).getRow(), fixture.get("row5"));

        // >= "杭州" => row1, row5
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_EQUAL,
                ColumnValue.fromString("杭州")));
        result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getTableName(), tableName);
        VerifyRow(result.get(0).getRow(), fixture.get("row1"));
        assertEquals(result.get(1).getRow().getColumns().size(), 0);
        assertEquals(result.get(2).getTableName(), tableName);
        VerifyRow(result.get(2).getRow(), fixture.get("row5"));

        // < "杭州" => row4
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN,
                ColumnValue.fromString("杭州")));
        result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getRow().getColumns().size(), 0);
        assertEquals(result.get(1).getTableName(), tableName);
        VerifyRow(result.get(1).getRow(), fixture.get("row4"));
        assertEquals(result.get(2).getRow().getColumns().size(), 0);

        // <= "杭州" => row1, row4
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_EQUAL,
                ColumnValue.fromString("杭州")));
        result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getTableName(), tableName);
        VerifyRow(result.get(0).getRow(), fixture.get("row1"));
        assertEquals(result.get(1).getTableName(), tableName);
        VerifyRow(result.get(1).getRow(), fixture.get("row4"));
        assertEquals(result.get(2).getRow().getColumns().size(), 0);

        // GetRange [row2, row6)
        // = "杭州" => row2, row3
        RangeRowQueryCriteria rangeCriteria = new RangeRowQueryCriteria(tableName);
        rangeCriteria.setInclusiveStartPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row2"))
                        );
        rangeCriteria.setExclusiveEndPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row6"))
                        );
         
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL,
                ColumnValue.fromString("杭州")));
        List<Row> rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 2);
        VerifyRow(rows.get(0), fixture.get("row2"));
        VerifyRow(rows.get(1), fixture.get("row3"));

        // != "杭州" => row4, row5
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.NOT_EQUAL,
                ColumnValue.fromString("杭州")));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 2);
        VerifyRow(rows.get(0), fixture.get("row4"));
        VerifyRow(rows.get(1), fixture.get("row5"));

        // > "杭州" => row5
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_THAN,
                ColumnValue.fromString("杭州")));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 1);
        VerifyRow(rows.get(0), fixture.get("row5"));

        // >= "杭州" => row2, row3, row5
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_EQUAL,
                ColumnValue.fromString("杭州")));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 3);
        VerifyRow(rows.get(0), fixture.get("row2"));
        VerifyRow(rows.get(1), fixture.get("row3"));
        VerifyRow(rows.get(2), fixture.get("row5"));

        // < "杭州" => row4
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN,
                ColumnValue.fromString("杭州")));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 1);
        VerifyRow(rows.get(0), fixture.get("row4"));

        // <= "杭州" => row2, row3, row4
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_EQUAL,
                ColumnValue.fromString("杭州")));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 3);
        VerifyRow(rows.get(0), fixture.get("row2"));
        VerifyRow(rows.get(1), fixture.get("row3"));
        VerifyRow(rows.get(2), fixture.get("row4"));

        // GetRangeReverse [row5, row1)
        // = "杭州" => row3, row2
        rangeCriteria.setInclusiveStartPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row5"))
                        );
        rangeCriteria.setExclusiveEndPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row1"))
                        );
        rangeCriteria.setDirection(Direction.BACKWARD);
         
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL,
                ColumnValue.fromString("杭州")));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 2);
        VerifyRow(rows.get(0), fixture.get("row3"));
        VerifyRow(rows.get(1), fixture.get("row2"));

        // != "杭州" => row4, row4
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.NOT_EQUAL,
                ColumnValue.fromString("杭州")));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 2);
        VerifyRow(rows.get(0), fixture.get("row5"));
        VerifyRow(rows.get(1), fixture.get("row4"));

        // > "杭州" => row5
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_THAN,
                ColumnValue.fromString("杭州")));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 1);
        VerifyRow(rows.get(0), fixture.get("row5"));

        // >= "杭州" => row5, row3, row2
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_EQUAL,
                ColumnValue.fromString("杭州")));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 3);
        VerifyRow(rows.get(0), fixture.get("row5"));
        VerifyRow(rows.get(1), fixture.get("row3"));
        VerifyRow(rows.get(2), fixture.get("row2"));

        // < "杭州" => row4
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN,
                ColumnValue.fromString("杭州")));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 1);
        VerifyRow(rows.get(0), fixture.get("row4"));

        // <= "杭州" => row4, row3, row2
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_EQUAL,
                ColumnValue.fromString("杭州")));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 3);
        VerifyRow(rows.get(0), fixture.get("row4"));
        VerifyRow(rows.get(1), fixture.get("row3"));
        VerifyRow(rows.get(2), fixture.get("row2"));
    }

    /**
     * | pk   | col1 | col2       |
     * |------+------+------------|
     * | row1 | 杭州 | DON'T CARE |
     * | row2 | 杭州 | DON'T CARE |
     * | row3 | 杭州 | DON'T CARE |
     * | row4 | 中国 | DON'T CARE |
     * | row5 | 浙江 | DON'T CARE |
     */
    private HashMap<String, Row> WriteFixture4Case6() throws UnsupportedEncodingException {
        LOG.info("Start testCase6");

        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("pk", PrimaryKeyType.STRING);

        OTSHelper.createTable(ots, tableMeta);
        Utils.sleepSeconds(SECONDS_UNTIL_TABLE_READY);

        HashMap<String, Row> fixture = new HashMap<String, Row>();
        for (int i = 1; i <= 5; ++i) {
            String rowKey = "row" + i;
            RowPrimaryKey pk = new RowPrimaryKey()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(rowKey));

            String value = "Uninitialized";
            if (i <= 3) {
                value = "杭州";
            } else if (i == 4) {
                value = "中国";
            } else if (i == 5) {
                value = "浙江";
            }

            Map<String, ColumnValue> puts = new HashMap<String, ColumnValue>();
            puts.put("col1", ColumnValue.fromBinary(value.getBytes("utf-8")));
            puts.put("col2", ColumnValue.fromString("DON'T CARE"));
            OTSHelper.updateRow(ots, tableName, pk, puts, null);
            Row row = new Row();
            row.addColumn("pk", ColumnValue.fromString(rowKey));
            row.addColumn("col1", ColumnValue.fromBinary(value.getBytes("utf-8")));
            row.addColumn("col2", ColumnValue.fromString("DON'T CARE"));
            fixture.put(rowKey, row);
        }
        return fixture;
    }

    /**
     * Test relation filter with BINARY type with Chinese character
     * Test filters with the following combinations:
     * ["col1"] x [=, !=, >, >=, <, <=] x ["杭州", "中国", "浙江"]
     * for GetRow/BatchGetRow/GetRange
     */
    @Test
    public void testCase6() throws UnsupportedEncodingException {
        LOG.info("Start testCase6");

        long ts = (new Date()).getTime();
        LOG.info("Begin time stamp : " + ts);

        HashMap<String, Row> fixture = WriteFixture4Case6();

        RowPrimaryKey pk = new RowPrimaryKey()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row1"));

        // = "杭州" => PASS
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);
        criteria.setPrimaryKey(pk);
         
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL,
                ColumnValue.fromBinary(new String("杭州").getBytes())));

        Row row = OTSHelper.getRow(ots, criteria).getRow();
        VerifyRow(row, fixture.get("row1"));

        // != "杭州" => DROP
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.NOT_EQUAL,
                ColumnValue.fromBinary(new String("杭州").getBytes())));
        row = OTSHelper.getRow(ots, criteria).getRow();
        checkRow(new Row(), row);

        // > "杭州" => DROP
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_THAN,
                ColumnValue.fromBinary(new String("杭州").getBytes())));
        row = OTSHelper.getRow(ots, criteria).getRow();
        checkRow(new Row(), row);

        // >= "杭州" => PASS
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_EQUAL,
                ColumnValue.fromBinary(new String("杭州").getBytes())));
        row = OTSHelper.getRow(ots, criteria).getRow();
        VerifyRow(row, fixture.get("row1"));

        // < "杭州" => DROP
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN,
                ColumnValue.fromBinary(new String("杭州").getBytes())));
        row = OTSHelper.getRow(ots, criteria).getRow();
        checkRow(new Row(), row);

        // <= "杭州" => PASS
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_EQUAL,
                ColumnValue.fromBinary(new String("杭州").getBytes())));
        row = OTSHelper.getRow(ots, criteria).getRow();
        VerifyRow(row, fixture.get("row1"));

        // BatchGetRow (Get row1, row4, row5)
        // = "杭州" => row1
        List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
        MultiRowQueryCriteria c = new MultiRowQueryCriteria(tableName);
         
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL,
                ColumnValue.fromBinary(new String("杭州").getBytes())));
        c.addRow(new RowPrimaryKey()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row1"))
                );
        c.addRow(new RowPrimaryKey()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row4"))
                );
        c.addRow(new RowPrimaryKey()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row5"))
                );
        criterias.add(c);
        List< BatchGetRowResult.RowStatus> result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getTableName(), tableName);
        VerifyRow(result.get(0).getRow(), fixture.get("row1"));
        assertEquals(result.get(1).getRow().getColumns().size(), 0);
        assertEquals(result.get(2).getRow().getColumns().size(), 0);

        // != "杭州" => row4, row5
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.NOT_EQUAL,
                ColumnValue.fromBinary(new String("杭州").getBytes())));
        result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getRow().getColumns().size(), 0);
        assertEquals(result.get(1).getTableName(), tableName);
        VerifyRow(result.get(1).getRow(), fixture.get("row4"));
        assertEquals(result.get(2).getTableName(), tableName);
        VerifyRow(result.get(2).getRow(), fixture.get("row5"));

        // > "杭州" => row5
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_THAN,
                ColumnValue.fromBinary(new String("杭州").getBytes())));
        result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getRow().getColumns().size(), 0);
        assertEquals(result.get(1).getRow().getColumns().size(), 0);
        assertEquals(result.get(2).getTableName(), tableName);
        VerifyRow(result.get(2).getRow(), fixture.get("row5"));

        // >= "杭州" => row1, row5
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_EQUAL,
                ColumnValue.fromBinary(new String("杭州").getBytes())));
        result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getTableName(), tableName);
        VerifyRow(result.get(0).getRow(), fixture.get("row1"));
        assertEquals(result.get(1).getRow().getColumns().size(), 0);
        assertEquals(result.get(2).getTableName(), tableName);
        VerifyRow(result.get(2).getRow(), fixture.get("row5"));

        // < "杭州" => row4
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN,
                ColumnValue.fromBinary(new String("杭州").getBytes())));
        result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getRow().getColumns().size(), 0);
        assertEquals(result.get(1).getTableName(), tableName);
        VerifyRow(result.get(1).getRow(), fixture.get("row4"));
        assertEquals(result.get(2).getRow().getColumns().size(), 0);

        // <= "杭州" => row1, row4
        c.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_EQUAL,
                ColumnValue.fromBinary(new String("杭州").getBytes())));
        result = OTSHelper.batchGetRow(ots, criterias).getSucceedRows();
        assertEquals(result.size(), 3);
        assertEquals(result.get(0).getTableName(), tableName);
        VerifyRow(result.get(0).getRow(), fixture.get("row1"));
        assertEquals(result.get(1).getTableName(), tableName);
        VerifyRow(result.get(1).getRow(), fixture.get("row4"));
        assertEquals(result.get(2).getRow().getColumns().size(), 0);

        // GetRange [row2, row6)
        // = "杭州" => row2, row3
        RangeRowQueryCriteria rangeCriteria = new RangeRowQueryCriteria(tableName);
        rangeCriteria.setInclusiveStartPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row2"))
                        );
        rangeCriteria.setExclusiveEndPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row6"))
                        );
         
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL,
                ColumnValue.fromBinary(new String("杭州").getBytes())));
        List<Row> rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 2);
        VerifyRow(rows.get(0), fixture.get("row2"));
        VerifyRow(rows.get(1), fixture.get("row3"));

        // != "杭州" => row4, row5
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.NOT_EQUAL,
                ColumnValue.fromBinary(new String("杭州").getBytes())));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 2);
        VerifyRow(rows.get(0), fixture.get("row4"));
        VerifyRow(rows.get(1), fixture.get("row5"));

        // > "杭州" => row5
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_THAN,
                ColumnValue.fromBinary(new String("杭州").getBytes())));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 1);
        VerifyRow(rows.get(0), fixture.get("row5"));

        // >= "杭州" => row2, row3, row5
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_EQUAL,
                ColumnValue.fromBinary(new String("杭州").getBytes())));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 3);
        VerifyRow(rows.get(0), fixture.get("row2"));
        VerifyRow(rows.get(1), fixture.get("row3"));
        VerifyRow(rows.get(2), fixture.get("row5"));

        // < "杭州" => row4
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN,
                ColumnValue.fromBinary(new String("杭州").getBytes())));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 1);
        VerifyRow(rows.get(0), fixture.get("row4"));

        // <= "杭州" => row2, row3, row4
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_EQUAL,
                ColumnValue.fromBinary(new String("杭州").getBytes())));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 3);
        VerifyRow(rows.get(0), fixture.get("row2"));
        VerifyRow(rows.get(1), fixture.get("row3"));
        VerifyRow(rows.get(2), fixture.get("row4"));

        // GetRangeReverse [row5, row1)
        // = "杭州" => row3, row2
        rangeCriteria.setInclusiveStartPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row5"))
                        );
        rangeCriteria.setExclusiveEndPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row1"))
                        );
        rangeCriteria.setDirection(Direction.BACKWARD);
         
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL,
                ColumnValue.fromBinary(new String("杭州").getBytes())));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 2);
        VerifyRow(rows.get(0), fixture.get("row3"));
        VerifyRow(rows.get(1), fixture.get("row2"));

        // != "杭州" => row4, row4
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.NOT_EQUAL,
                ColumnValue.fromBinary(new String("杭州").getBytes())));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 2);
        VerifyRow(rows.get(0), fixture.get("row5"));
        VerifyRow(rows.get(1), fixture.get("row4"));

        // > "杭州" => row5
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_THAN,
                ColumnValue.fromBinary(new String("杭州").getBytes())));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 1);
        VerifyRow(rows.get(0), fixture.get("row5"));

        // >= "杭州" => row5, row3, row2
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_EQUAL,
                ColumnValue.fromBinary(new String("杭州").getBytes())));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 3);
        VerifyRow(rows.get(0), fixture.get("row5"));
        VerifyRow(rows.get(1), fixture.get("row3"));
        VerifyRow(rows.get(2), fixture.get("row2"));

        // < "杭州" => row4
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN,
                ColumnValue.fromBinary(new String("杭州").getBytes())));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 1);
        VerifyRow(rows.get(0), fixture.get("row4"));

        // <= "杭州" => row4, row3, row2
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_EQUAL,
                ColumnValue.fromBinary(new String("杭州").getBytes())));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 3);
        VerifyRow(rows.get(0), fixture.get("row4"));
        VerifyRow(rows.get(1), fixture.get("row3"));
        VerifyRow(rows.get(2), fixture.get("row2"));
    }

    /**
     * | pk   | col1  | col2       |
     * |------+-------+------------|
     * | row1 | true  | DON'T CARE |
     * | row2 | false | DON'T CARE |
     * | row3 | true  | DON'T CARE |
     */
    private HashMap<String, Row> WriteFixture4Case7() {
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("pk", PrimaryKeyType.STRING);

        OTSHelper.createTable(ots, tableMeta);
        Utils.sleepSeconds(SECONDS_UNTIL_TABLE_READY);

        HashMap<String, Row> fixture = new HashMap<String, Row>();
        for (int i = 1; i <= 3; ++i) {
            String rowKey = "row" + i;
            RowPrimaryKey pk = new RowPrimaryKey()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(rowKey));


            boolean value = false;
            if (i == 1) {
                value = true;
            } else if (i == 2) {
                value = false;
            } else if (i == 3) {
                value = true;
            }

            Map<String, ColumnValue> puts = new HashMap<String, ColumnValue>();
            puts.put("col1", ColumnValue.fromBoolean(value));
            puts.put("col2", ColumnValue.fromString("DON'T CARE"));
            OTSHelper.updateRow(ots, tableName, pk, puts, null);
            Row row = new Row();
            row.addColumn("pk", ColumnValue.fromString(rowKey));
            row.addColumn("col1", ColumnValue.fromBoolean(value));
            row.addColumn("col2", ColumnValue.fromString("DON'T CARE"));
            fixture.put(rowKey, row);
        }
        return fixture;
    }

    /**
     * Test relation filter with BOOLEAN type
     * Test the following comparisons:
     * [true, false] x [=, !=, >, >=, <, <=] x [true, false]
     */
    @Test
    public void testCase7() throws UnsupportedEncodingException {
        LOG.info("Start testCase7");

        long ts = (new Date()).getTime();
        LOG.info("Begin time stamp : " + ts);

        HashMap<String, Row> fixture = WriteFixture4Case7();

        RowPrimaryKey pk = new RowPrimaryKey()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row1"));

        // = true => PASS
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);
        criteria.setPrimaryKey(pk);
         
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromBoolean(true)));

        Row row = OTSHelper.getRow(ots, criteria).getRow();
        VerifyRow(row, fixture.get("row1"));

        // != true => DROP
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.NOT_EQUAL, ColumnValue.fromBoolean(true)));
        row = OTSHelper.getRow(ots, criteria).getRow();
        checkRow(new Row(), row);

        // > true => DROP
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_THAN, ColumnValue.fromBoolean(true)));
        row = OTSHelper.getRow(ots, criteria).getRow();
        checkRow(new Row(), row);

        // >= true => PASS
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_EQUAL, ColumnValue.fromBoolean(true)));
        row = OTSHelper.getRow(ots, criteria).getRow();
        VerifyRow(row, fixture.get("row1"));

        // < true => DROP
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN, ColumnValue.fromBoolean(true)));
        row = OTSHelper.getRow(ots, criteria).getRow();
        checkRow(new Row(), row);

        // <= true => PASS
        criteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_EQUAL, ColumnValue.fromBoolean(true)));
        row = OTSHelper.getRow(ots, criteria).getRow();
        VerifyRow(row, fixture.get("row1"));

        // GetRange [row1, row4)
        // = true => row1, row3
        RangeRowQueryCriteria rangeCriteria = new RangeRowQueryCriteria(tableName);
        rangeCriteria.setInclusiveStartPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row1"))
                        );
        rangeCriteria.setExclusiveEndPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row4"))
                        );
         
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromBoolean(true)));
        List<Row> rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 2);
        VerifyRow(rows.get(0), fixture.get("row1"));
        VerifyRow(rows.get(1), fixture.get("row3"));

        // != true => row2
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.NOT_EQUAL, ColumnValue.fromBoolean(true)));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 1);
        VerifyRow(rows.get(0), fixture.get("row2"));

        // > true => none
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_THAN, ColumnValue.fromBoolean(true)));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 0);

        // >= true => row1, row3
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_EQUAL, ColumnValue.fromBoolean(true)));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 2);
        VerifyRow(rows.get(0), fixture.get("row1"));
        VerifyRow(rows.get(1), fixture.get("row3"));

        // < true => row2
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN, ColumnValue.fromBoolean(true)));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 1);
        VerifyRow(rows.get(0), fixture.get("row2"));

        // <= true => row2
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_EQUAL, ColumnValue.fromBoolean(true)));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 3);
        VerifyRow(rows.get(0), fixture.get("row1"));
        VerifyRow(rows.get(1), fixture.get("row2"));
        VerifyRow(rows.get(2), fixture.get("row3"));

        // GetRangeReverse [row3, row0)
        // = false => row2
        rangeCriteria.setInclusiveStartPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row3"))
                        );
        rangeCriteria.setExclusiveEndPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row0"))
                        );
        rangeCriteria.setDirection(Direction.BACKWARD);
         
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromBoolean(false)));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 1);
        VerifyRow(rows.get(0), fixture.get("row2"));

        // != false => row3, row1
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.NOT_EQUAL, ColumnValue.fromBoolean(false)));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 2);
        VerifyRow(rows.get(0), fixture.get("row3"));
        VerifyRow(rows.get(1), fixture.get("row1"));

        // > false => row3, row1
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_THAN, ColumnValue.fromBoolean(false)));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 2);
        VerifyRow(rows.get(0), fixture.get("row3"));
        VerifyRow(rows.get(1), fixture.get("row1"));

        // >= false => row3, row2, row1
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_EQUAL, ColumnValue.fromBoolean(false)));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 3);
        VerifyRow(rows.get(0), fixture.get("row3"));
        VerifyRow(rows.get(1), fixture.get("row2"));
        VerifyRow(rows.get(2), fixture.get("row1"));

        // < false => none
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN, ColumnValue.fromBoolean(false)));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 0);

        // <= false => row2
        rangeCriteria.setFilter(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_EQUAL, ColumnValue.fromBoolean(false)));
        rows = OTSHelper.getRange(ots, rangeCriteria).getRows();
        assertEquals(rows.size(), 1);
        VerifyRow(rows.get(0), fixture.get("row2"));
    }

    /**
     * | PK   | col1 | col2 | col3       |
     * |------+------+------+------------|
     * | row1 | ABC  | 100  | DON'T CARE |
     * | row2 | ABC  | 200  | DON'T CARE |
     * | row3 | ABC  | 300  | DON'T CARE |
     * | row4 | ABB  | 100  | DON'T CARE |
     * | row5 | ABD  | 100  | DON'T CARE |
     */
    private HashMap<String, Row> WriteFixture4Case8() {
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("pk", PrimaryKeyType.STRING);

        OTSHelper.createTable(ots, tableMeta);
        Utils.sleepSeconds(SECONDS_UNTIL_TABLE_READY);

        HashMap<String, Row> fixture = new HashMap<String, Row>();
        for (int i = 1; i <= 5; ++i) {
            String rowKey = "row" + i;
            RowPrimaryKey pk = new RowPrimaryKey()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(rowKey));


            String value1 = "";
            long value2 = 0;
            if (i <= 3) {
                value1 = "ABC";
                value2 = 100;
            } else if (i == 4) {
                value1 = "ABC";
                value2 = 100;
            } else if (i == 5) {
                value1 = "ABC";
                value2 = 100;
            }

            Map<String, ColumnValue> puts = new HashMap<String, ColumnValue>();
            puts.put("col1", ColumnValue.fromString(value1));
            puts.put("col2", ColumnValue.fromLong(value2));
            puts.put("col3", ColumnValue.fromString("DON'T CARE"));
            OTSHelper.updateRow(ots, tableName, pk, puts, null);
            Row row = new Row();
            row.addColumn("pk", ColumnValue.fromString(rowKey));
            row.addColumn("col1", ColumnValue.fromString(value1));
            row.addColumn("col2", ColumnValue.fromLong(value2));
            row.addColumn("col3", ColumnValue.fromString("DON'T CARE"));
            fixture.put(rowKey, row);
        }
        return fixture;
    }

    /**
     * Test composite filter with AND/OR/NOT
     * Test the following comparisons:
     * [true, false] x [AND/OR] x [true, false]
     * [NOT] x [true, false]
     */
    @Test
    public void testCase8() throws UnsupportedEncodingException {
        LOG.info("Start testCase8");

        long ts = (new Date()).getTime();
        LOG.info("Begin time stamp : " + ts);

        HashMap<String, Row> fixture = WriteFixture4Case8();

        RowPrimaryKey pk = new RowPrimaryKey()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row1"));

        // true AND true
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);
        criteria.setPrimaryKey(pk);
         
        CompositeCondition filter = new CompositeCondition(CompositeCondition.LogicOperator.AND);
        filter.addCondition(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromString("ABC")));
        filter.addCondition(new  RelationalCondition("col2",
                 RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromLong(100)));
        criteria.setFilter(filter);

        Row row = OTSHelper.getRow(ots, criteria).getRow();
        VerifyRow(row, fixture.get("row1"));

        // true AND false
        filter = new CompositeCondition(CompositeCondition.LogicOperator.AND);
        filter.addCondition(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromString("ABC")));
        filter.addCondition(new  RelationalCondition("col2",
                 RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromLong(200)));
        criteria.setFilter(filter);

        row = OTSHelper.getRow(ots, criteria).getRow();
        checkRow(new Row(), row);

        // false AND true
        filter = new CompositeCondition(CompositeCondition.LogicOperator.AND);
        filter.addCondition(new  RelationalCondition("col2",
                 RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromLong(200)));
        filter.addCondition(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromString("ABC")));
        criteria.setFilter(filter);

        row = OTSHelper.getRow(ots, criteria).getRow();
        checkRow(new Row(), row);

        // false AND false
        filter = new CompositeCondition(CompositeCondition.LogicOperator.AND);
        filter.addCondition(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.NOT_EQUAL, ColumnValue.fromString("ABC")));
        filter.addCondition(new  RelationalCondition("col2",
                 RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromLong(200)));
        criteria.setFilter(filter);

        row = OTSHelper.getRow(ots, criteria).getRow();
        checkRow(new Row(), row);

        // true OR true
        filter = new CompositeCondition(CompositeCondition.LogicOperator.OR);
        filter.addCondition(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromString("ABC")));
        filter.addCondition(new  RelationalCondition("col2",
                 RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromLong(100)));
        criteria.setFilter(filter);

        row = OTSHelper.getRow(ots, criteria).getRow();
        VerifyRow(row, fixture.get("row1"));

        // true OR false
        filter = new CompositeCondition(CompositeCondition.LogicOperator.OR);
        filter.addCondition(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromString("ABC")));
        filter.addCondition(new  RelationalCondition("col2",
                 RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromLong(200)));
        criteria.setFilter(filter);

        row = OTSHelper.getRow(ots, criteria).getRow();
        VerifyRow(row, fixture.get("row1"));

        // false OR true
        filter = new CompositeCondition(CompositeCondition.LogicOperator.OR);
        filter.addCondition(new  RelationalCondition("col2",
                 RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromLong(200)));
        filter.addCondition(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromString("ABC")));
        criteria.setFilter(filter);

        row = OTSHelper.getRow(ots, criteria).getRow();
        VerifyRow(row, fixture.get("row1"));

        // false OR false
        filter = new CompositeCondition(CompositeCondition.LogicOperator.OR);
        filter.addCondition(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.NOT_EQUAL, ColumnValue.fromString("ABC")));
        filter.addCondition(new  RelationalCondition("col2",
                 RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromLong(200)));
        criteria.setFilter(filter);

        row = OTSHelper.getRow(ots, criteria).getRow();
        checkRow(new Row(), row);

        // NOT true
        filter = new CompositeCondition(CompositeCondition.LogicOperator.NOT);
        filter.addCondition(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromString("ABC")));
        criteria.setFilter(filter);

        row = OTSHelper.getRow(ots, criteria).getRow();
        checkRow(new Row(), row);

        // NOT false
        filter = new CompositeCondition(CompositeCondition.LogicOperator.NOT);
        filter.addCondition(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.NOT_EQUAL, ColumnValue.fromString("ABC")));
        criteria.setFilter(filter);

        row = OTSHelper.getRow(ots, criteria).getRow();
        VerifyRow(row, fixture.get("row1"));
    }

    /**
     * | PK   | col1      | col2       |
     * |------+-----------+------------|
     * | row1 | BINARY(5) | DON'T CARE |
     * | row2 | BINARY(8) | DON'T CARE |
     * | row3 | BINARY(2) | DON'T CARE |
     * | row4 | BINARY(4) | DON'T CARE |
     */
    private HashMap<String, Row> WriteFixture4Case9() {
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("pk", PrimaryKeyType.STRING);

        OTSHelper.createTable(ots, tableMeta);
        Utils.sleepSeconds(SECONDS_UNTIL_TABLE_READY);

        HashMap<String, Row> fixture = new HashMap<String, Row>();
        for (int i = 1; i <= 4; ++i) {
            String rowKey = "row" + i;
            RowPrimaryKey pk = new RowPrimaryKey()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(rowKey));

            byte[] value = null;
            if (i == 1) {
                value = new byte[]{5};
            } else if (i == 2) {
                value = new byte[]{8};
            } else if (i == 3) {
                value = new byte[]{2};
            } else if (i == 4) {
                value = new byte[]{4};
            }

            Map<String, ColumnValue> puts = new HashMap<String, ColumnValue>();
            puts.put("col1", ColumnValue.fromBinary(value));
            puts.put("col2", ColumnValue.fromString("DON'T CARE"));
            OTSHelper.updateRow(ots, tableName, pk, puts, null);
            Row row = new Row();
            row.addColumn("pk", ColumnValue.fromString(rowKey));
            row.addColumn("col1", ColumnValue.fromBinary(value));
            row.addColumn("col2", ColumnValue.fromString("DON'T CARE"));
            fixture.put(rowKey, row);
        }
        return fixture;
    }

    /**
     * Test composite filter:
     * (>0 AND <3) OR =5 AND (NOT =9 OR <7)
     */
    @Test
    public void testCase9() throws UnsupportedEncodingException {
        LOG.info("Start testCase9");

        long ts = (new Date()).getTime();
        LOG.info("Begin time stamp : " + ts);

        HashMap<String, Row> fixture = WriteFixture4Case9();

        // ReadRange
        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
        criteria.setInclusiveStartPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row1"))
                        );
        criteria.setExclusiveEndPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row5"))
                        );
         

        CompositeCondition f1 = new CompositeCondition(CompositeCondition.LogicOperator.AND);
        f1.addCondition(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_THAN,
                ColumnValue.fromBinary(new byte[]{0})));
        f1.addCondition(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN,
                ColumnValue.fromBinary(new byte[]{3})));
        CompositeCondition f2 = new CompositeCondition(CompositeCondition.LogicOperator.OR);
        f2.addCondition(f1);
        f2.addCondition(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL,
                ColumnValue.fromBinary(new byte[]{5})));
        CompositeCondition f3 = new CompositeCondition(CompositeCondition.LogicOperator.NOT);
        f3.addCondition(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL,
                ColumnValue.fromBinary(new byte[]{9})));
        CompositeCondition f4 = new CompositeCondition(CompositeCondition.LogicOperator.OR);
        f4.addCondition(f3);
        f4.addCondition(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN,
                ColumnValue.fromBinary(new byte[]{7})));

        CompositeCondition filter = new CompositeCondition(CompositeCondition.LogicOperator.AND);
        filter.addCondition(f2);
        filter.addCondition(f4);

        criteria.setFilter(filter);
        List<Row> rows = OTSHelper.getRange(ots, criteria).getRows();
        assertEquals(rows.size(), 2);
        VerifyRow(rows.get(0), fixture.get("row1"));
        VerifyRow(rows.get(1), fixture.get("row3"));

        // ReadRangeReverse
        criteria.setInclusiveStartPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row4"))
                        );
        criteria.setExclusiveEndPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row0"))
                        );
        criteria.setDirection(Direction.BACKWARD);

        rows = OTSHelper.getRange(ots, criteria).getRows();
        assertEquals(rows.size(), 2);
        VerifyRow(rows.get(0), fixture.get("row3"));
        VerifyRow(rows.get(1), fixture.get("row1"));
    }

    /**
     * | PK   | col1         | col2       |
     * |------+--------------+------------|
     * | row1 | BINARY(XXX)  | DON'T CARE |
     * | row2 | BINARY(YYY)  | DON'T CARE |
     * | row3 | BINARY(ABC)  | DON'T CARE |
     */
    private HashMap<String, Row> WriteFixture4Case10() throws UnsupportedEncodingException {
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("pk", PrimaryKeyType.STRING);

        OTSHelper.createTable(ots, tableMeta);
        Utils.sleepSeconds(SECONDS_UNTIL_TABLE_READY);

        HashMap<String, Row> fixture = new HashMap<String, Row>();
        for (int i = 1; i <= 3; ++i) {
            String rowKey = "row" + i;
            RowPrimaryKey pk = new RowPrimaryKey()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(rowKey));


            String value = "Uninitialized";
            if (i == 1) {
                value = "XXX";
            } else if (i == 2) {
                value = "YYY";
            } else if (i == 3) {
                value = "ABC";
            }
            Map<String, ColumnValue> puts = new HashMap<String, ColumnValue>();
            puts.put("col1", ColumnValue.fromBinary(value.getBytes("utf-8")));
            puts.put("col2", ColumnValue.fromString("DON'T CARE"));
            OTSHelper.updateRow(ots, tableName, pk, puts, null);
            Row row = new Row();
            row.addColumn("pk", ColumnValue.fromString(rowKey));
            row.addColumn("col1", ColumnValue.fromBinary(value.getBytes("utf-8")));
            row.addColumn("col2", ColumnValue.fromString("DON'T CARE"));
            fixture.put(rowKey, row);
        }
        return fixture;
    }

    /**
     * Test composite filter:
     * >"" AND >"A" AND >"AB" AND >"ABC" AND >"ABD" AND >"CCC" AND (NOT = "YYY")
     */
    @Test
    public void testCase10() throws UnsupportedEncodingException {
        LOG.info("Start testCase10");

        long ts = (new Date()).getTime();
        LOG.info("Begin time stamp : " + ts);

        HashMap<String, Row> fixture = WriteFixture4Case10();

        // ReadRange
        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
        criteria.setInclusiveStartPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row1"))
                        );
        criteria.setExclusiveEndPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row4"))
                        );
         

        CompositeCondition f1 = new CompositeCondition(CompositeCondition.LogicOperator.AND);
        f1.addCondition(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_THAN,
                ColumnValue.fromBinary(new byte[]{})));
        f1.addCondition(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_THAN,
                ColumnValue.fromBinary(new String("A").getBytes())));
        f1.addCondition(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_THAN,
                ColumnValue.fromBinary(new String("AB").getBytes())));
        f1.addCondition(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_THAN,
                ColumnValue.fromBinary(new String("ABC").getBytes())));
        f1.addCondition(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_THAN,
                ColumnValue.fromBinary(new String("ABD").getBytes())));
        f1.addCondition(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.GREATER_THAN,
                ColumnValue.fromBinary(new String("CCC").getBytes())));

        CompositeCondition f2 = new CompositeCondition(CompositeCondition.LogicOperator.NOT);
        f2.addCondition(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.EQUAL,
                ColumnValue.fromBinary(new String("YYY").getBytes())));

        CompositeCondition filter = new CompositeCondition(CompositeCondition.LogicOperator.AND);
        filter.addCondition(f1);
        filter.addCondition(f2);

        criteria.setFilter(filter);
        List<Row> rows = OTSHelper.getRange(ots, criteria).getRows();
        assertEquals(rows.size(), 1);
        VerifyRow(rows.get(0), fixture.get("row1"));

        // ReadRangeReverse
        criteria.setInclusiveStartPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row3"))
                        );
        criteria.setExclusiveEndPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row0"))
                        );
        criteria.setDirection(Direction.BACKWARD);

        rows = OTSHelper.getRange(ots, criteria).getRows();
        assertEquals(rows.size(), 1);
        VerifyRow(rows.get(0), fixture.get("row1"));
    }

    /**
     * | PK   | col1 | col2       |
     * |------+------+------------|
     * | row1 |   10 | DON'T CARE |
     * | row2 |    2 | DON'T CARE |
     * | row3 |    5 | DON'T CARE |
     * | row4 |    8 | DON'T CARE |
     */
    private HashMap<String, Row> WriteFixture4Case11() {
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("pk", PrimaryKeyType.STRING);

        OTSHelper.createTable(ots, tableMeta);
        Utils.sleepSeconds(SECONDS_UNTIL_TABLE_READY);

        HashMap<String, Row> fixture = new HashMap<String, Row>();
        for (int i = 1; i <= 4; ++i) {
            String rowKey = "row" + i;
            RowPrimaryKey pk = new RowPrimaryKey()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(rowKey));

            long value = 0;
            if (i == 1) {
                value = 10;
            } else if (i == 2) {
                value = 2;
            } else if (i == 3) {
                value = 5;
            } else if (i == 4) {
                value = 8;
            }
            Map<String, ColumnValue> puts = new HashMap<String, ColumnValue>();
            puts.put("col1", ColumnValue.fromLong(value));
            puts.put("col2", ColumnValue.fromString("DON'T CARE"));
            OTSHelper.updateRow(ots, tableName, pk, puts, null);
            Row row = new Row();
            row.addColumn("pk", ColumnValue.fromString(rowKey));
            row.addColumn("col1", ColumnValue.fromLong(value));
            row.addColumn("col2", ColumnValue.fromString("DON'T CARE"));
            fixture.put(rowKey, row);
        }
        return fixture;
    }

    /**
     * Test composite filter:
     * < 0 OR < 1 OR < 2 OR < 3 OR < 4 OR < 5 OR < 6 OR < 7 OR < 8
     */
    @Test
    public void testCase11() throws UnsupportedEncodingException {
        LOG.info("Start testCase11");

        long ts = (new Date()).getTime();
        LOG.info("Begin time stamp : " + ts);

        HashMap<String, Row> fixture = WriteFixture4Case11();

        // ReadRange
        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
        criteria.setInclusiveStartPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row1"))
                        );
        criteria.setExclusiveEndPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row5"))
                        );
         

        CompositeCondition filter = new CompositeCondition(CompositeCondition.LogicOperator.OR);
        filter.addCondition(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN,
                ColumnValue.fromLong(0)));
        filter.addCondition(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN,
                ColumnValue.fromLong(1)));
        filter.addCondition(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN,
                ColumnValue.fromLong(2)));
        filter.addCondition(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN,
                ColumnValue.fromLong(3)));
        filter.addCondition(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN,
                ColumnValue.fromLong(4)));
        filter.addCondition(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN,
                ColumnValue.fromLong(5)));
        filter.addCondition(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN,
                ColumnValue.fromLong(6)));
        filter.addCondition(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN,
                ColumnValue.fromLong(7)));
        filter.addCondition(new  RelationalCondition("col1",
                 RelationalCondition.CompareOperator.LESS_THAN,
                ColumnValue.fromLong(8)));

        criteria.setFilter(filter);
        List<Row> rows = OTSHelper.getRange(ots, criteria).getRows();
        assertEquals(rows.size(), 2);
        VerifyRow(rows.get(0), fixture.get("row2"));
        VerifyRow(rows.get(1), fixture.get("row3"));

        // ReadRangeReverse
        criteria.setInclusiveStartPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row4"))
                        );
        criteria.setExclusiveEndPrimaryKey(
              new RowPrimaryKey()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("row0"))
                        );
        criteria.setDirection(Direction.BACKWARD);

        rows = OTSHelper.getRange(ots, criteria).getRows();
        assertEquals(rows.size(), 2);
        VerifyRow(rows.get(0), fixture.get("row3"));
        VerifyRow(rows.get(1), fixture.get("row2"));
    }
}
