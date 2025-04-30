package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.common.BaseFT;
import com.alicloud.openservices.tablestore.common.OTSHelper;
import com.alicloud.openservices.tablestore.common.Utils;
import com.alicloud.openservices.tablestore.*;
import com.google.gson.JsonSyntaxException;
import org.junit.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;

public class CapacityUnitTest extends BaseFT {
    private static final int MILLISECONDS_UNTIL_TABLE_READY = 10 * 1000;

    private static final String tableName = "CapacityUnitTest";
    private static SyncClientInterface client;
    private static Logger LOG = Logger.getLogger(CapacityUnitTest.class.getName());


    @BeforeClass
    public static void classBefore() throws JsonSyntaxException, IOException {
        client = Utils.getOTSInstance();
    }

    @Before
    public void setup() throws Exception {
        OTSHelper.deleteAllTable(client);
    }

    private void CreateTable(SyncClientInterface otsForPublic, String tableName, Map<String, PrimaryKeyType> pk) throws Exception {
        try {
            OTSHelper.createTable(otsForPublic, tableName, pk);
            Thread.sleep(MILLISECONDS_UNTIL_TABLE_READY);
        } catch (Exception e) {
            LOG.warning("Errors during table creation process: " + e.getMessage());
        }
    }

    public static PrimaryKeyValue getPKColumnValue(PrimaryKeyType type, String value) throws UnsupportedEncodingException {
        switch (type) {
            case INTEGER:
                return PrimaryKeyValue.fromLong(Long.valueOf(value));
            case STRING:
                return PrimaryKeyValue.fromString(value);
            case BINARY:
                return PrimaryKeyValue.fromBinary(value.getBytes("UTF-8"));
            default:
                throw new RuntimeException("Bug: not support : " + type);
        }
    }

    void assertCapacityUnitEqual(CapacityUnit ca, CapacityUnit cb) {
        assertEquals(cb.getReadCapacityUnit(), ca.getReadCapacityUnit());
        assertEquals(cb.getWriteCapacityUnit(), ca.getWriteCapacityUnit());
    }

    String fixedSizeString(char c, long size) {
        StringBuffer sb = new StringBuffer();
        for (long i = 0; i < size; ++i) {
            sb.append(c);
        }
        return sb.toString();
    }

    /**
     * Create a table without writing data, expecting GetRow to return CU as 1.
     *
     * @throws Exception
     */
    @Test
    public void testGetRowNonexistent() throws Exception {
        Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("PK1", PrimaryKeyType.STRING);
        String modifiedTableName = tableName + System.currentTimeMillis();
        CreateTable(client, modifiedTableName, pks);

        List<PrimaryKeyColumn> pk = new ArrayList<PrimaryKeyColumn>();
        pk.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "x")));

        GetRowResponse result = OTSHelper.getRowForAll(client, modifiedTableName, new PrimaryKey(pk));
        CapacityUnit cu = result.getConsumedCapacity().getCapacityUnit();
        assertCapacityUnitEqual(cu, new CapacityUnit(1, 0));
    }

    /**
     * Create a table, write a row "x" with a value size of 5K, read the row "x", expect GetRow to return CU as 2.
     *
     * @throws Exception
     */
    @Test
    public void testGetRowExistent() throws Exception {
        Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("PK1", PrimaryKeyType.STRING);
        String modifiedTableName = tableName + System.currentTimeMillis();
        CreateTable(client, modifiedTableName, pks);

        List<PrimaryKeyColumn> pk = new ArrayList<PrimaryKeyColumn>();
        pk.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "x")));

        List<Column> columns = new ArrayList<Column>();
        columns.add(new Column("a", ColumnValue.fromString(fixedSizeString('a', 5000))));

        OTSHelper.putRow(client, modifiedTableName, new PrimaryKey(pk), columns);

        GetRowResponse result = OTSHelper.getRowForAll(client, modifiedTableName, new PrimaryKey(pk));
        CapacityUnit cu = result.getConsumedCapacity().getCapacityUnit();
        assertCapacityUnitEqual(cu, new CapacityUnit(2, 0));

        OTSHelper.deleteTable(client, modifiedTableName);
    }

    /**
     * Create a table, expecting GetRow('a') to return CU as 1
     *
     * @throws Exception
     */
    @Test
    public void testGetRowNonexistentSelect() throws Exception {
        Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("PK1", PrimaryKeyType.STRING);
        String modifiedTableName = tableName + System.currentTimeMillis();
        CreateTable(client, modifiedTableName, pks);
        List<PrimaryKeyColumn> pk = new ArrayList<PrimaryKeyColumn>();
        pk.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "x")));

        List<String> columns = new ArrayList<String>();
        columns.add("a");

        GetRowResponse result = OTSHelper.getRow(client, modifiedTableName, new PrimaryKey(pk), null, 1, columns);
        CapacityUnit cu = result.getConsumedCapacity().getCapacityUnit();
        assertCapacityUnitEqual(cu, new CapacityUnit(1, 0));

        OTSHelper.deleteTable(client, modifiedTableName);
    }

    /**
     * Create a table, write a row "x", with columns "a", "b", "c", "d", "e", each column value size is 2K, read row "x", expect GetRow("a", "b") to return CU as 2
     *
     * @throws Exception
     */
    @Test
    public void testGetRowSelectAllExistent() throws Exception {
        Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("PK1", PrimaryKeyType.STRING);

        String modifiedTableName = tableName + System.currentTimeMillis();
        CreateTable(client, modifiedTableName, pks);

        List<PrimaryKeyColumn> pk = new ArrayList<PrimaryKeyColumn>();
        pk.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "x")));

        {
            List<Column> columns = new ArrayList<Column>();
            columns.add(new Column("a", ColumnValue.fromString(fixedSizeString('a', 2048))));
            columns.add(new Column("b", ColumnValue.fromString(fixedSizeString('a', 2048))));
            columns.add(new Column("c", ColumnValue.fromString(fixedSizeString('a', 2048))));
            columns.add(new Column("d", ColumnValue.fromString(fixedSizeString('a', 2048))));
            columns.add(new Column("e", ColumnValue.fromString(fixedSizeString('a', 2048))));

            OTSHelper.putRow(client, modifiedTableName, new PrimaryKey(pk), columns);
        }
        {
            List<String> columns = new ArrayList<String>();
            columns.add("a");
            columns.add("b");

            GetRowResponse result = OTSHelper.getRow(client, modifiedTableName, new PrimaryKey(pk), null, 1, columns);
            CapacityUnit cu = result.getConsumedCapacity().getCapacityUnit();
            assertCapacityUnitEqual(cu, new CapacityUnit(2, 0));
        }
        OTSHelper.deleteTable(client, modifiedTableName);
    }

    /**
     * Create a table, write a row "x" with columns "a", "c", "d", each column value size is 2K, read row "x", expect GetRow("a", "b") to return CU as 1.
     *
     * @throws Exception
     */
    @Test
    public void testGetRowSelectPartialExistent() throws Exception {
        Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("PK1", PrimaryKeyType.STRING);

        String modifiedTableName = tableName + System.currentTimeMillis();
        CreateTable(client, modifiedTableName, pks);

        List<PrimaryKeyColumn> pk = new ArrayList<PrimaryKeyColumn>();
        pk.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "x")));

        {
            List<Column> columns = new ArrayList<Column>();
            columns.add(new Column("a", ColumnValue.fromString(fixedSizeString('a', 2048))));
            columns.add(new Column("c", ColumnValue.fromString(fixedSizeString('a', 2048))));
            columns.add(new Column("d", ColumnValue.fromString(fixedSizeString('a', 2048))));

            OTSHelper.putRow(client, modifiedTableName, new PrimaryKey(pk), columns);
        }
        {
            List<String> columns = new ArrayList<String>();
            columns.add("a");
            columns.add("b");

            GetRowResponse result = OTSHelper.getRow(client, modifiedTableName, new PrimaryKey(pk), null, 1, columns);
            CapacityUnit cu = result.getConsumedCapacity().getCapacityUnit();
            assertCapacityUnitEqual(cu, new CapacityUnit(1, 0));
        }
        OTSHelper.deleteTable(client, modifiedTableName);
    }

    /**
     * Create a table, write a row "x", with columns "a", "b", "e", each column value size is 4K, read row "x", expect GetRow("c", "d") to return CU as 1
     *
     * @throws Exception
     */
    @Test
    public void testGetRowSelectAllNonxistent() throws Exception {
        Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("PK1", PrimaryKeyType.STRING);

        String modifiedTableName = tableName + System.currentTimeMillis();
        CreateTable(client, modifiedTableName, pks);

        List<PrimaryKeyColumn> pk = new ArrayList<PrimaryKeyColumn>();
        pk.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "x")));

        {
            List<Column> columns = new ArrayList<Column>();
            columns.add(new Column("a", ColumnValue.fromString(fixedSizeString('a', 4096))));
            columns.add(new Column("b", ColumnValue.fromString(fixedSizeString('a', 4096))));
            columns.add(new Column("e", ColumnValue.fromString(fixedSizeString('a', 4096))));

            OTSHelper.putRow(client, modifiedTableName, new PrimaryKey(pk), columns);
        }
        {
            List<String> columns = new ArrayList<String>();
            columns.add("c");
            columns.add("d");

            GetRowResponse result = OTSHelper.getRow(client, modifiedTableName, new PrimaryKey(pk), null, 1, columns);
            CapacityUnit cu = result.getConsumedCapacity().getCapacityUnit();
            assertCapacityUnitEqual(cu, new CapacityUnit(1, 0));
        }
        OTSHelper.deleteTable(client, modifiedTableName);
    }

    /**
     * Create a table, write rows "x", "y", "z", columns "a", "b", "c", "d", "e", with each column value size being 2K, read row "x", expect BatchGetRow("x", "y", "z") to return CU of 3 per row.
     *
     * @throws Exception
     */
    @Test
    public void testBatchGetRow() throws Exception {
        Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("PK1", PrimaryKeyType.STRING);

        String modifiedTableName = tableName + System.currentTimeMillis();
        CreateTable(client, modifiedTableName, pks);

        List<Column> columns = new ArrayList<Column>();
        columns.add(new Column("a", ColumnValue.fromString(fixedSizeString('a', 2048))));
        columns.add(new Column("b", ColumnValue.fromString(fixedSizeString('a', 2048))));
        columns.add(new Column("c", ColumnValue.fromString(fixedSizeString('a', 2048))));
        columns.add(new Column("d", ColumnValue.fromString(fixedSizeString('a', 2048))));
        columns.add(new Column("e", ColumnValue.fromString(fixedSizeString('a', 2048))));

        String[] pkList = {"x", "y", "z"};
        for (String pk : pkList) {
            List<PrimaryKeyColumn> pkc = new ArrayList<PrimaryKeyColumn>();
            pkc.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, pk)));

            OTSHelper.putRow(client, modifiedTableName, new PrimaryKey(pkc), columns);
        }

        MultiRowQueryCriteria multiRows = new MultiRowQueryCriteria(modifiedTableName);
        for (String pk : pkList) {
            List<PrimaryKeyColumn> pkc = new ArrayList<PrimaryKeyColumn>();
            pkc.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, pk)));

            multiRows.addRow(new PrimaryKey(pkc));
        }
        multiRows.setMaxVersions(1);
        List<MultiRowQueryCriteria> request = new ArrayList<MultiRowQueryCriteria>();
        request.add(multiRows);
        BatchGetRowResponse result = OTSHelper.batchGetRow(client, request);
        List<BatchGetRowResponse.RowResult> rowRets = result.getBatchGetRowResult(modifiedTableName);

        for (BatchGetRowResponse.RowResult ret : rowRets) {
            CapacityUnit cu = ret.getConsumedCapacity().getCapacityUnit();
            assertCapacityUnitEqual(cu, new CapacityUnit(3, 0));
        }

        OTSHelper.deleteTable(client, modifiedTableName);
    }

    /**
     * Create a table, expecting GetRange("a", "b") to return CU as 1.
     *
     * @throws Exception
     */
    @Test
    public void testGetRangeEmptyRange() throws Exception {
        Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("PK1", PrimaryKeyType.STRING);

        String modifiedTableName = tableName + System.currentTimeMillis();
        CreateTable(client, modifiedTableName, pks);

        List<PrimaryKeyColumn> startPK = new ArrayList<PrimaryKeyColumn>();
        startPK.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "a")));

        List<PrimaryKeyColumn> endPK = new ArrayList<PrimaryKeyColumn>();
        endPK.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "b")));

        RangeRowQueryCriteria query = new RangeRowQueryCriteria(modifiedTableName);
        query.setInclusiveStartPrimaryKey(new PrimaryKey(startPK));
        query.setExclusiveEndPrimaryKey(new PrimaryKey(endPK));
        query.setMaxVersions(1);

        GetRangeResponse result = OTSHelper.getRange(client, query);
        CapacityUnit cu = result.getConsumedCapacity().getCapacityUnit();
        assertCapacityUnitEqual(cu, new CapacityUnit(1, 0));

        OTSHelper.deleteTable(client, modifiedTableName);
    }

    /**
     * Create a table, write row "a" with columns "a", "b", "c", each column value size is 4K, expect GetRange("a", "b") to return CU as 4
     *
     * @throws Exception
     */
    @Test
    public void testGetRange() throws Exception {
        Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("PK1", PrimaryKeyType.STRING);

        String modifiedTableName = tableName + System.currentTimeMillis();
        CreateTable(client, modifiedTableName, pks);

        {
            List<PrimaryKeyColumn> pk = new ArrayList<PrimaryKeyColumn>();
            pk.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "a")));

            List<Column> columns = new ArrayList<Column>();
            columns.add(new Column("a", ColumnValue.fromString(fixedSizeString('a', 4096))));
            columns.add(new Column("b", ColumnValue.fromString(fixedSizeString('a', 4096))));
            columns.add(new Column("c", ColumnValue.fromString(fixedSizeString('a', 4096))));

            OTSHelper.putRow(client, modifiedTableName, new PrimaryKey(pk), columns);
        }

        {
            List<PrimaryKeyColumn> startPK = new ArrayList<PrimaryKeyColumn>();
            startPK.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "a")));

            List<PrimaryKeyColumn> endPK = new ArrayList<PrimaryKeyColumn>();
            endPK.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "b")));

            RangeRowQueryCriteria query = new RangeRowQueryCriteria(modifiedTableName);
            query.setInclusiveStartPrimaryKey(new PrimaryKey(startPK));
            query.setExclusiveEndPrimaryKey(new PrimaryKey(endPK));
            query.setMaxVersions(1);

            GetRangeResponse result = OTSHelper.getRange(client, query);
            CapacityUnit cu = result.getConsumedCapacity().getCapacityUnit();
            assertCapacityUnitEqual(cu, new CapacityUnit(4, 0));
        }

        OTSHelper.deleteTable(client, modifiedTableName);
    }

    /**
     * Create a table, write rows "a" and "b", columns "a", "b", "c", with each column value size being 4K, expecting GetRange("a", "c") to return CU as 7.
     *
     * @throws Exception
     */
    @Test
    public void testGetRangeMultiRows() throws Exception {
        Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("PK1", PrimaryKeyType.STRING);

        String modifiedTableName = tableName + System.currentTimeMillis();
        CreateTable(client, modifiedTableName, pks);

        {
            List<PrimaryKeyColumn> pk = new ArrayList<PrimaryKeyColumn>();
            pk.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "a")));

            List<Column> columns = new ArrayList<Column>();
            columns.add(new Column("a", ColumnValue.fromString(fixedSizeString('a', 4096))));
            columns.add(new Column("b", ColumnValue.fromString(fixedSizeString('a', 4096))));
            columns.add(new Column("c", ColumnValue.fromString(fixedSizeString('a', 4096))));

            OTSHelper.putRow(client, modifiedTableName, new PrimaryKey(pk), columns);
        }

        {
            List<PrimaryKeyColumn> pk = new ArrayList<PrimaryKeyColumn>();
            pk.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "b")));

            List<Column> columns = new ArrayList<Column>();
            columns.add(new Column("a", ColumnValue.fromString(fixedSizeString('a', 4096))));
            columns.add(new Column("b", ColumnValue.fromString(fixedSizeString('a', 4096))));
            columns.add(new Column("c", ColumnValue.fromString(fixedSizeString('a', 4096))));

            OTSHelper.putRow(client, modifiedTableName, new PrimaryKey(pk), columns);
        }

        {
            List<PrimaryKeyColumn> startPK = new ArrayList<PrimaryKeyColumn>();
            startPK.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "a")));

            List<PrimaryKeyColumn> endPK = new ArrayList<PrimaryKeyColumn>();
            endPK.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "c")));

            RangeRowQueryCriteria query = new RangeRowQueryCriteria(modifiedTableName);
            query.setInclusiveStartPrimaryKey(new PrimaryKey(startPK));
            query.setExclusiveEndPrimaryKey(new PrimaryKey(endPK));
            query.setMaxVersions(1);

            GetRangeResponse result = OTSHelper.getRange(client, query);
            CapacityUnit cu = result.getConsumedCapacity().getCapacityUnit();
            assertCapacityUnitEqual(cu, new CapacityUnit(7, 0));
        }

        OTSHelper.deleteTable(client, modifiedTableName);
    }

    /**
     * Create a table, expecting GetRange("a", "b")("a") to return CU as 1.
     *
     * @throws Exception
     */
    @Test
    public void testGetRangeEmptyRangeSelect() throws Exception {
        Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("PK1", PrimaryKeyType.STRING);

        String modifiedTableName = tableName + System.currentTimeMillis();
        CreateTable(client, modifiedTableName, pks);

        List<PrimaryKeyColumn> startPK = new ArrayList<PrimaryKeyColumn>();
        startPK.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "a")));

        List<PrimaryKeyColumn> endPK = new ArrayList<PrimaryKeyColumn>();
        endPK.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "b")));

        RangeRowQueryCriteria query = new RangeRowQueryCriteria(modifiedTableName);
        query.setInclusiveStartPrimaryKey(new PrimaryKey(startPK));
        query.setExclusiveEndPrimaryKey(new PrimaryKey(endPK));
        query.setMaxVersions(1);
        query.addColumnsToGet("a");

        GetRangeResponse result = OTSHelper.getRange(client, query);
        CapacityUnit cu = result.getConsumedCapacity().getCapacityUnit();
        assertCapacityUnitEqual(cu, new CapacityUnit(1, 0));

        OTSHelper.deleteTable(client, modifiedTableName);
    }

    /**
     * Create a table, write rows "a" and "b", with columns "a", "b", "c". Each value in column "a" is 4K in size. Expect GetRange("a", "c")("a", "b") to return CU as 5.
     *
     * @throws Exception
     */
    @Test
    public void testGetRangeSelectAllExistent() throws Exception {
        Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("PK1", PrimaryKeyType.STRING);

        String modifiedTableName = tableName + System.currentTimeMillis();
        CreateTable(client, modifiedTableName, pks);

        {
            List<PrimaryKeyColumn> pk = new ArrayList<PrimaryKeyColumn>();
            pk.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "a")));

            List<Column> columns = new ArrayList<Column>();
            columns.add(new Column("a", ColumnValue.fromString(fixedSizeString('a', 4096))));
            columns.add(new Column("b", ColumnValue.fromString(fixedSizeString('a', 4096))));
            columns.add(new Column("c", ColumnValue.fromString(fixedSizeString('a', 4096))));

            OTSHelper.putRow(client, modifiedTableName, new PrimaryKey(pk), columns);
        }

        {
            List<PrimaryKeyColumn> pk = new ArrayList<PrimaryKeyColumn>();
            pk.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "b")));

            List<Column> columns = new ArrayList<Column>();
            columns.add(new Column("a", ColumnValue.fromString(fixedSizeString('a', 4096))));
            columns.add(new Column("b", ColumnValue.fromString(fixedSizeString('a', 4096))));
            columns.add(new Column("c", ColumnValue.fromString(fixedSizeString('a', 4096))));

            OTSHelper.putRow(client, modifiedTableName, new PrimaryKey(pk), columns);
        }

        {
            List<PrimaryKeyColumn> startPK = new ArrayList<PrimaryKeyColumn>();
            startPK.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "a")));

            List<PrimaryKeyColumn> endPK = new ArrayList<PrimaryKeyColumn>();
            endPK.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "c")));

            RangeRowQueryCriteria query = new RangeRowQueryCriteria(modifiedTableName);
            query.setInclusiveStartPrimaryKey(new PrimaryKey(startPK));
            query.setExclusiveEndPrimaryKey(new PrimaryKey(endPK));
            query.setMaxVersions(1);
            query.addColumnsToGet("a");
            query.addColumnsToGet("b");

            GetRangeResponse result = OTSHelper.getRange(client, query);
            CapacityUnit cu = result.getConsumedCapacity().getCapacityUnit();
            assertCapacityUnitEqual(cu, new CapacityUnit(5, 0));
        }

        OTSHelper.deleteTable(client, modifiedTableName);
    }

    /**
     * Create a table, write rows "a" and "b", with columns "a", "b", "c". Each value in column "a" is 4K in size. Expect GetRange("a", "c")("d", "e") to return CU as 1.
     *
     * @throws Exception
     */
    @Test
    public void testGetRangeSelectAllNonexistent() throws Exception {
        Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("PK1", PrimaryKeyType.STRING);

        String modifiedTableName = tableName + System.currentTimeMillis();
        CreateTable(client, modifiedTableName, pks);

        {
            List<PrimaryKeyColumn> pk = new ArrayList<PrimaryKeyColumn>();
            pk.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "a")));

            List<Column> columns = new ArrayList<Column>();
            columns.add(new Column("a", ColumnValue.fromString(fixedSizeString('a', 4096))));
            columns.add(new Column("b", ColumnValue.fromString(fixedSizeString('a', 4096))));
            columns.add(new Column("c", ColumnValue.fromString(fixedSizeString('a', 4096))));

            OTSHelper.putRow(client, modifiedTableName, new PrimaryKey(pk), columns);
        }

        {
            List<PrimaryKeyColumn> pk = new ArrayList<PrimaryKeyColumn>();
            pk.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "b")));

            List<Column> columns = new ArrayList<Column>();
            columns.add(new Column("a", ColumnValue.fromString(fixedSizeString('a', 4096))));
            columns.add(new Column("b", ColumnValue.fromString(fixedSizeString('a', 4096))));
            columns.add(new Column("c", ColumnValue.fromString(fixedSizeString('a', 4096))));

            OTSHelper.putRow(client, modifiedTableName, new PrimaryKey(pk), columns);
        }

        {
            List<PrimaryKeyColumn> startPK = new ArrayList<PrimaryKeyColumn>();
            startPK.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "a")));

            List<PrimaryKeyColumn> endPK = new ArrayList<PrimaryKeyColumn>();
            endPK.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "c")));

            RangeRowQueryCriteria query = new RangeRowQueryCriteria(modifiedTableName);
            query.setInclusiveStartPrimaryKey(new PrimaryKey(startPK));
            query.setExclusiveEndPrimaryKey(new PrimaryKey(endPK));
            query.setMaxVersions(1);
            query.addColumnsToGet("d");
            query.addColumnsToGet("e");

            GetRangeResponse result = OTSHelper.getRange(client, query);
            CapacityUnit cu = result.getConsumedCapacity().getCapacityUnit();
            assertCapacityUnitEqual(cu, new CapacityUnit(1, 0));
        }

        OTSHelper.deleteTable(client, modifiedTableName);
    }

    /**
     * Create a table, write row "a" with columns "a", "b", "c", and row "b" with columns "a", "c", "e". Each column value size is 4K. Expect GetRange("a", "c")("b", "e") to return CU as 3.
     *
     * @throws Exception
     */
    @Test
    public void testGetRangeSelectPartialExistent() throws Exception {
        Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("PK1", PrimaryKeyType.STRING);

        String modifiedTableName = tableName + System.currentTimeMillis();
        CreateTable(client, modifiedTableName, pks);

        {
            List<PrimaryKeyColumn> pk = new ArrayList<PrimaryKeyColumn>();
            pk.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "a")));

            List<Column> columns = new ArrayList<Column>();
            columns.add(new Column("a", ColumnValue.fromString(fixedSizeString('a', 4096))));
            columns.add(new Column("b", ColumnValue.fromString(fixedSizeString('a', 4096))));
            columns.add(new Column("c", ColumnValue.fromString(fixedSizeString('a', 4096))));

            OTSHelper.putRow(client, modifiedTableName, new PrimaryKey(pk), columns);
        }

        {
            List<PrimaryKeyColumn> pk = new ArrayList<PrimaryKeyColumn>();
            pk.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "b")));

            List<Column> columns = new ArrayList<Column>();
            columns.add(new Column("a", ColumnValue.fromString(fixedSizeString('a', 4096))));
            columns.add(new Column("c", ColumnValue.fromString(fixedSizeString('a', 4096))));
            columns.add(new Column("e", ColumnValue.fromString(fixedSizeString('a', 4096))));

            OTSHelper.putRow(client, modifiedTableName, new PrimaryKey(pk), columns);
        }

        {
            List<PrimaryKeyColumn> startPK = new ArrayList<PrimaryKeyColumn>();
            startPK.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "a")));

            List<PrimaryKeyColumn> endPK = new ArrayList<PrimaryKeyColumn>();
            endPK.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "c")));

            RangeRowQueryCriteria query = new RangeRowQueryCriteria(modifiedTableName);
            query.setInclusiveStartPrimaryKey(new PrimaryKey(startPK));
            query.setExclusiveEndPrimaryKey(new PrimaryKey(endPK));
            query.setMaxVersions(1);
            query.addColumnsToGet("b");
            query.addColumnsToGet("e");

            GetRangeResponse result = OTSHelper.getRange(client, query);
            CapacityUnit cu = result.getConsumedCapacity().getCapacityUnit();
            assertCapacityUnitEqual(cu, new CapacityUnit(3, 0));
        }

        OTSHelper.deleteTable(client, modifiedTableName);
    }

    /**
     * Create a table, write row "a", columns "a", "b", "c", with each column value size being 4K. Expect PutRow("a")("b": 4096, "e": 4096) to return CU as 3.
     *
     * @throws Exception
     */
    @Test
    public void testPutRow() throws Exception {
        Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("PK1", PrimaryKeyType.STRING);

        String modifiedTableName = tableName + System.currentTimeMillis();
        CreateTable(client, modifiedTableName, pks);

        {
            List<PrimaryKeyColumn> pk = new ArrayList<PrimaryKeyColumn>();
            pk.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "a")));

            List<Column> columns = new ArrayList<Column>();
            columns.add(new Column("a", ColumnValue.fromString(fixedSizeString('a', 4096))));
            columns.add(new Column("b", ColumnValue.fromString(fixedSizeString('a', 4096))));
            columns.add(new Column("c", ColumnValue.fromString(fixedSizeString('a', 4096))));

            OTSHelper.putRow(client, modifiedTableName, new PrimaryKey(pk), columns);
        }

        {
            List<PrimaryKeyColumn> pk = new ArrayList<PrimaryKeyColumn>();
            pk.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "a")));

            List<Column> columns = new ArrayList<Column>();
            columns.add(new Column("b", ColumnValue.fromString(fixedSizeString('a', 4096))));
            columns.add(new Column("e", ColumnValue.fromString(fixedSizeString('a', 4096))));

            PutRowResponse result = OTSHelper.putRow(client, modifiedTableName, new PrimaryKey(pk), columns);
            CapacityUnit cu = result.getConsumedCapacity().getCapacityUnit();
            assertCapacityUnitEqual(cu, new CapacityUnit(0, 3));
        }

        OTSHelper.deleteTable(client, modifiedTableName);
    }

    @Test
    public void testPutRowExpectExistOrNotExist() throws Exception {
        Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("PK1", PrimaryKeyType.STRING);

        String modifiedTableName = tableName + System.currentTimeMillis();
        CreateTable(client, modifiedTableName, pks);
        {
            List<PrimaryKeyColumn> pk = new ArrayList<PrimaryKeyColumn>();
            pk.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "a")));

            List<Column> columns = new ArrayList<Column>();
            columns.add(new Column("b", ColumnValue.fromString(fixedSizeString('a', 4096))));
            columns.add(new Column("e", ColumnValue.fromString(fixedSizeString('a', 4096))));

            RowPutChange rowChange = new RowPutChange(modifiedTableName, new PrimaryKey(pk));
            for (Column col : columns) {
                rowChange.addColumn(col);
            }
            rowChange.setCondition(new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST));
            PutRowResponse result = OTSHelper.putRow(client, rowChange);
            CapacityUnit cu = result.getConsumedCapacity().getCapacityUnit();
            assertCapacityUnitEqual(cu, new CapacityUnit(1, 3));

            rowChange.setCondition(new Condition(RowExistenceExpectation.EXPECT_EXIST));
            result = OTSHelper.putRow(client, rowChange);
            cu = result.getConsumedCapacity().getCapacityUnit();
            assertCapacityUnitEqual(cu, new CapacityUnit(1, 3));
        }

        OTSHelper.deleteTable(client, modifiedTableName);
    }

    /**
     * Create a table, write a row "a" with columns "a", "b", "c", where the value size of each column is 4K. Expect UpdateRow("a")("b": 4096, "e": 4096) to return CU as 3.
     *
     * @throws Exception
     */
    @Test
    public void testUpdateRow() throws Exception {
        Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("PK1", PrimaryKeyType.STRING);

        String modifiedTableName = tableName + System.currentTimeMillis();
        CreateTable(client, modifiedTableName, pks);

        {
            List<PrimaryKeyColumn> pk = new ArrayList<PrimaryKeyColumn>();
            pk.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "a")));

            List<Column> columns = new ArrayList<Column>();
            columns.add(new Column("a", ColumnValue.fromString(fixedSizeString('a', 4096))));
            columns.add(new Column("b", ColumnValue.fromString(fixedSizeString('a', 4096))));
            columns.add(new Column("c", ColumnValue.fromString(fixedSizeString('a', 4096))));

            OTSHelper.putRow(client, modifiedTableName, new PrimaryKey(pk), columns);
        }

        {
            List<PrimaryKeyColumn> pk = new ArrayList<PrimaryKeyColumn>();
            pk.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "a")));

            RowUpdateChange row = new RowUpdateChange(modifiedTableName, new PrimaryKey(pk));
            row.put(new Column("b", ColumnValue.fromString(fixedSizeString('a', 4096))));
            row.put(new Column("e", ColumnValue.fromString(fixedSizeString('a', 4096))));

            UpdateRowResponse result = OTSHelper.updateRow(client, row);
            CapacityUnit cu = result.getConsumedCapacity().getCapacityUnit();
            assertCapacityUnitEqual(cu, new CapacityUnit(0, 3));
        }

        OTSHelper.deleteTable(client, modifiedTableName);
    }

    @Test
    public void testUpdateRowExpectExist() throws Exception {
        Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("PK1", PrimaryKeyType.STRING);

        String modifiedTableName = tableName + System.currentTimeMillis();
        CreateTable(client, modifiedTableName, pks);

        {
            List<PrimaryKeyColumn> pk = new ArrayList<PrimaryKeyColumn>();
            pk.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "a")));

            List<Column> columns = new ArrayList<Column>();
            columns.add(new Column("a", ColumnValue.fromString(fixedSizeString('a', 4096))));
            columns.add(new Column("b", ColumnValue.fromString(fixedSizeString('a', 4096))));
            columns.add(new Column("c", ColumnValue.fromString(fixedSizeString('a', 4096))));

            OTSHelper.putRow(client, modifiedTableName, new PrimaryKey(pk), columns);
        }

        {
            List<PrimaryKeyColumn> pk = new ArrayList<PrimaryKeyColumn>();
            pk.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "a")));

            RowUpdateChange row = new RowUpdateChange(modifiedTableName, new PrimaryKey(pk));
            row.put(new Column("b", ColumnValue.fromString(fixedSizeString('a', 4096))));
            row.put(new Column("e", ColumnValue.fromString(fixedSizeString('a', 4096))));
            row.deleteColumns("a");
            row.deleteColumns("c");
            row.setCondition(new Condition(RowExistenceExpectation.EXPECT_EXIST));
            UpdateRowResponse result = OTSHelper.updateRow(client, row);
            CapacityUnit cu = result.getConsumedCapacity().getCapacityUnit();
            assertCapacityUnitEqual(cu, new CapacityUnit(1, 3));
        }
        OTSHelper.deleteTable(client, modifiedTableName);
    }

    /**
     * Create a table, write row "a", columns "a", "b", "c", with each column value size being 4K, expecting DeleteRow("a") to return CU as 1.
     *
     * @throws Exception
     */
    @Test
    public void testDeleteRow() throws Exception {
        Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("PK1", PrimaryKeyType.STRING);

        String modifiedTableName = tableName + System.currentTimeMillis();
        CreateTable(client, modifiedTableName, pks);

        {
            List<PrimaryKeyColumn> pk = new ArrayList<PrimaryKeyColumn>();
            pk.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "a")));

            List<Column> columns = new ArrayList<Column>();
            columns.add(new Column("a", ColumnValue.fromString(fixedSizeString('a', 4096))));
            columns.add(new Column("b", ColumnValue.fromString(fixedSizeString('a', 4096))));
            columns.add(new Column("c", ColumnValue.fromString(fixedSizeString('a', 4096))));
            try {
                OTSHelper.putRow(client, modifiedTableName, new PrimaryKey(pk), columns);
            } catch (Exception e) {
                LOG.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        {
            List<PrimaryKeyColumn> pk = new ArrayList<PrimaryKeyColumn>();
            pk.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, "a")));

            RowDeleteChange row = new RowDeleteChange(modifiedTableName, new PrimaryKey(pk));

            DeleteRowResponse result = OTSHelper.deleteRow(client, row);
            CapacityUnit cu = result.getConsumedCapacity().getCapacityUnit();
            assertCapacityUnitEqual(cu, new CapacityUnit(0, 1));

            row.setCondition(new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST));
            result = OTSHelper.deleteRow(client, row);
            cu = result.getConsumedCapacity().getCapacityUnit();
            assertCapacityUnitEqual(cu, new CapacityUnit(1, 1));
        }

        OTSHelper.deleteTable(client, modifiedTableName);
    }

    /**
     * Create a table, write rows "a", "b", "c", columns "a", "b", "c", with each column value size being 4K.
     * Use BatchWriteRow to perform PutRow("a", "b", "c") where ("b": 4096, "e": 4096).
     * Expect the returned CUs to be 3, 3, 3 respectively.
     *
     * @throws Exception
     */
    @Test
    public void testBatchPutRow() throws Exception {
        Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("PK1", PrimaryKeyType.STRING);

        String modifiedTableName = tableName + System.currentTimeMillis();
        CreateTable(client, modifiedTableName, pks);

        List<Column> columns = new ArrayList<Column>();
        columns.add(new Column("a", ColumnValue.fromString(fixedSizeString('a', 4096))));
        columns.add(new Column("b", ColumnValue.fromString(fixedSizeString('a', 4096))));
        columns.add(new Column("c", ColumnValue.fromString(fixedSizeString('a', 4096))));

        String[] pkList = {"a", "b", "c"};
        for (String pk : pkList) {
            List<PrimaryKeyColumn> pkc = new ArrayList<PrimaryKeyColumn>();
            pkc.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, pk)));

            OTSHelper.putRow(client, modifiedTableName, new PrimaryKey(pkc), columns);
        }

        List<RowPutChange> putRows = new ArrayList<RowPutChange>();
        for (String pk : pkList) {
            List<PrimaryKeyColumn> pkc = new ArrayList<PrimaryKeyColumn>();
            pkc.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, pk)));
            RowPutChange putRow = new RowPutChange(modifiedTableName, new PrimaryKey(pkc));
            putRow.addColumn("b", ColumnValue.fromString(fixedSizeString('a', 4096)));
            putRow.addColumn("e", ColumnValue.fromString(fixedSizeString('a', 4096)));
            putRows.add(putRow);
        }

        BatchWriteRowResponse result = OTSHelper.batchWriteRow(client, putRows, null, null);

        List<BatchWriteRowResponse.RowResult> putRets = result.getRowStatus(modifiedTableName);
        for (BatchWriteRowResponse.RowResult ret : putRets) {
            assertCapacityUnitEqual(ret.getConsumedCapacity().getCapacityUnit(), new CapacityUnit(0, 3));
        }

        OTSHelper.deleteTable(client, modifiedTableName);
    }

    /**
     * Create a table, write rows "a", "b", "c", columns "a", "b", "c", with each column value size being 4K,
     * Use BatchWriteRow to respectively UpdateRow("a", "b", "c")("b": 4096, "e": 4096),
     * Expect the returned CUs to be 3, 3, 3 respectively.
     *
     * @throws Exception
     */
    @Test
    public void testBatchUpdateRow() throws Exception {
        Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("PK1", PrimaryKeyType.STRING);

        String modifiedTableName = tableName + System.currentTimeMillis();
        CreateTable(client, modifiedTableName, pks);

        List<Column> columns = new ArrayList<Column>();
        columns.add(new Column("a", ColumnValue.fromString(fixedSizeString('a', 4096))));
        columns.add(new Column("b", ColumnValue.fromString(fixedSizeString('a', 4096))));
        columns.add(new Column("c", ColumnValue.fromString(fixedSizeString('a', 4096))));

        String[] pkList = {"a", "b", "c"};
        for (String pk : pkList) {
            List<PrimaryKeyColumn> pkc = new ArrayList<PrimaryKeyColumn>();
            pkc.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, pk)));

            OTSHelper.putRow(client, modifiedTableName, new PrimaryKey(pkc), columns);
        }

        List<RowUpdateChange> updateRows = new ArrayList<RowUpdateChange>();
        for (String pk : pkList) {
            List<PrimaryKeyColumn> pkc = new ArrayList<PrimaryKeyColumn>();
            pkc.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, pk)));
            RowUpdateChange updateRow = new RowUpdateChange(modifiedTableName, new PrimaryKey(pkc));
            updateRow.put("b", ColumnValue.fromString(fixedSizeString('a', 4096)));
            updateRow.put("e", ColumnValue.fromString(fixedSizeString('a', 4096)));
            updateRows.add(updateRow);
        }

        BatchWriteRowResponse result = OTSHelper.batchWriteRow(client, null, updateRows, null);

        List<BatchWriteRowResponse.RowResult> updateRets = result.getRowStatus(modifiedTableName);
        for (BatchWriteRowResponse.RowResult ret : updateRets) {
            assertCapacityUnitEqual(ret.getConsumedCapacity().getCapacityUnit(), new CapacityUnit(0, 3));
        }

        OTSHelper.deleteTable(client, modifiedTableName);
    }

    /**
     * Create a table, write rows "a", "b", "c", columns "a", "b", "c", with each column value size being 4K,
     * use BatchWriteRow to respectively delete rows ("a", "b", "c"),
     * expect the returned CUs to be 1, 1, 1 respectively.
     *
     * @throws Exception
     */
    @Test
    public void testBatchDeleteRow() throws Exception {
        Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("PK1", PrimaryKeyType.STRING);

        String modifiedmodifiedTableName = tableName + System.currentTimeMillis();
        CreateTable(client, modifiedmodifiedTableName, pks);

        List<Column> columns = new ArrayList<Column>();
        columns.add(new Column("a", ColumnValue.fromString(fixedSizeString('a', 4096))));
        columns.add(new Column("b", ColumnValue.fromString(fixedSizeString('a', 4096))));
        columns.add(new Column("c", ColumnValue.fromString(fixedSizeString('a', 4096))));

        String[] pkList = {"a", "b", "c"};
        for (String pk : pkList) {
            List<PrimaryKeyColumn> pkc = new ArrayList<PrimaryKeyColumn>();
            pkc.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, pk)));

            OTSHelper.putRow(client, modifiedmodifiedTableName, new PrimaryKey(pkc), columns);
        }

        List<RowDeleteChange> deleteRows = new ArrayList<RowDeleteChange>();
        for (String pk : pkList) {
            List<PrimaryKeyColumn> pkc = new ArrayList<PrimaryKeyColumn>();
            pkc.add(new PrimaryKeyColumn("PK1", getPKColumnValue(PrimaryKeyType.STRING, pk)));
            RowDeleteChange deleteRow = new RowDeleteChange(modifiedmodifiedTableName, new PrimaryKey(pkc));
            deleteRows.add(deleteRow);
        }

        BatchWriteRowResponse result = OTSHelper.batchWriteRow(client, null, null, deleteRows);

        List<BatchWriteRowResponse.RowResult> deleteRets = result.getRowStatus(modifiedmodifiedTableName);
        for (BatchWriteRowResponse.RowResult ret : deleteRets) {
            assertCapacityUnitEqual(ret.getConsumedCapacity().getCapacityUnit(), new CapacityUnit(0, 1));
        }

        OTSHelper.deleteTable(client, modifiedmodifiedTableName);
    }
}
