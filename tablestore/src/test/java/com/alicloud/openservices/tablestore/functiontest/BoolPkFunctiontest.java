package com.alicloud.openservices.tablestore.functiontest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.common.BaseFT;
import com.alicloud.openservices.tablestore.common.OTSHelper;
import com.alicloud.openservices.tablestore.common.Utils;
import com.alicloud.openservices.tablestore.model.BatchGetRowRequest;
import com.alicloud.openservices.tablestore.model.BatchGetRowResponse;
import com.alicloud.openservices.tablestore.model.BatchWriteRowRequest;
import com.alicloud.openservices.tablestore.model.BatchWriteRowResponse;
import com.alicloud.openservices.tablestore.model.CapacityUnit;
import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.CreateTableRequest;
import com.alicloud.openservices.tablestore.model.DefinedColumnSchema;
import com.alicloud.openservices.tablestore.model.DefinedColumnType;
import com.alicloud.openservices.tablestore.model.DeleteTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeTableResponse;
import com.alicloud.openservices.tablestore.model.Direction;
import com.alicloud.openservices.tablestore.model.GetRangeRequest;
import com.alicloud.openservices.tablestore.model.GetRangeResponse;
import com.alicloud.openservices.tablestore.model.GetRowRequest;
import com.alicloud.openservices.tablestore.model.GetRowResponse;
import com.alicloud.openservices.tablestore.model.MultiRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.PutRowRequest;
import com.alicloud.openservices.tablestore.model.PutRowResponse;
import com.alicloud.openservices.tablestore.model.RangeRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.ReservedThroughput;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.alicloud.openservices.tablestore.model.SingleRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.model.TableOptions;

/**
 * End-to-end tests for BOOLEAN as a non-first primary key column.
 *
 * Mirrors the C++ SDK test_bool_pk.cpp (commit 099d842). Server-side support
 * comes from the same unified-model backend. Cases:
 *   - CreateTable with PK1=STRING, PK2=BOOLEAN and a DCT_BOOLEAN defined column
 *   - CreateTable rejection when BOOLEAN is the first PK column
 *   - PutRow / GetRow with bool PK and bool attribute column
 *   - BatchWriteRow / BatchGetRow
 *   - GetRange with INF_MIN/INF_MAX endpoints and with concrete true/false
 *     endpoints (inclusive start, exclusive end)
 */
public class BoolPkFunctiontest extends BaseFT {

    private static final Logger LOG = LoggerFactory.getLogger(BoolPkFunctiontest.class);
    // unique per run: a fixed name would let two concurrent gate runs drop each other's table
    private static final String TABLE = OTSHelper.generateUniqueTableName("bool_pk_ft");

    private static SyncClientInterface ots;

    @BeforeClass
    public static void classBefore() throws IOException {
        ots = Utils.getOTSInstance();
        dropTableIfExists(TABLE);
    }

    @AfterClass
    public static void classAfter() {
        ots.shutdown();
    }

    @Before
    public void setup() throws Exception {
        // NOTE: instance-wide OTSHelper.deleteAllTable was removed on this branch for
        // parallel-test safety; this test only needs its own table, which classBefore
        // and teardown already drop via dropTableIfExists(TABLE).
        dropTableIfExists(TABLE);
        createBoolTable(TABLE);
        // allow server to settle
        Thread.sleep(3000);
    }

    @After
    public void teardown() {
        dropTableIfExists(TABLE);
    }

    private static void dropTableIfExists(String name) {
        try {
            ots.deleteTable(new DeleteTableRequest(name));
        } catch (TableStoreException e) {
            // only swallow "not exist"; any other delete failure must surface instead of
            // masquerading as a create conflict in the next test
            if (!com.alicloud.openservices.tablestore.core.ErrorCode.OBJECT_NOT_EXIST.equals(e.getErrorCode())) {
                throw e;
            }
        }
    }

    private static void createBoolTable(String name) {
        TableMeta meta = new TableMeta(name);
        meta.addPrimaryKeyColumn("pk1", PrimaryKeyType.STRING);
        meta.addPrimaryKeyColumn("pk2", PrimaryKeyType.BOOLEAN);
        meta.addDefinedColumn(new DefinedColumnSchema("col_bool", DefinedColumnType.BOOLEAN));

        TableOptions options = new TableOptions();
        options.setTimeToLive(-1);
        options.setMaxVersions(1);

        CreateTableRequest req = new CreateTableRequest(meta, options, new ReservedThroughput(new CapacityUnit(0, 0)));
        ots.createTable(req);
    }

    private static PrimaryKey makePk(String pk1, boolean pk2) {
        return new PrimaryKey(new PrimaryKeyColumn[]{
                new PrimaryKeyColumn("pk1", PrimaryKeyValue.fromString(pk1)),
                new PrimaryKeyColumn("pk2", PrimaryKeyValue.fromBoolean(pk2))
        });
    }

    private static void putRow(String pk1, boolean pk2, boolean col) {
        RowPutChange change = new RowPutChange(TABLE, makePk(pk1, pk2));
        change.addColumn("col_bool", ColumnValue.fromBoolean(col));
        PutRowResponse resp = ots.putRow(new PutRowRequest(change));
        assertNotNull(resp);
    }

    @Test
    public void testCreateTable_AndDescribe() {
        DescribeTableResponse resp = ots.describeTable(new DescribeTableRequest(TABLE));
        TableMeta meta = resp.getTableMeta();

        List<PrimaryKeySchema> pk = meta.getPrimaryKeyList();
        assertEquals(2, pk.size());
        assertEquals("pk1", pk.get(0).getName());
        assertEquals(PrimaryKeyType.STRING, pk.get(0).getType());
        assertEquals("pk2", pk.get(1).getName());
        assertEquals(PrimaryKeyType.BOOLEAN, pk.get(1).getType());

        List<DefinedColumnSchema> def = meta.getDefinedColumnsList();
        assertEquals(1, def.size());
        assertEquals("col_bool", def.get(0).getName());
        assertEquals(DefinedColumnType.BOOLEAN, def.get(0).getType());
    }

    @Test
    public void testFirstColumnBoolean_Rejected() throws InterruptedException {
        String table = TABLE + "_first";
        dropTableIfExists(table);
        Thread.sleep(1000);

        TableMeta meta = new TableMeta(table);
        meta.addPrimaryKeyColumn("pk1", PrimaryKeyType.BOOLEAN);
        TableOptions options = new TableOptions();
        options.setTimeToLive(-1);
        options.setMaxVersions(1);

        try {
            ots.createTable(new CreateTableRequest(meta, options,
                    new ReservedThroughput(new CapacityUnit(0, 0))));
            fail("Expected TableStoreException for BOOLEAN as first PK column");
        } catch (TableStoreException e) {
            LOG.info("Got expected exception: {}", e.toString());
            // Server returns OTSParameterInvalid; we don't pin the exact code/message
            // to keep the test robust against minor wording changes.
            assertTrue("Expected error code OTSParameterInvalid, got: " + e.getErrorCode(),
                    "OTSParameterInvalid".equals(e.getErrorCode())
                            || (e.getMessage() != null && e.getMessage().toLowerCase().contains("boolean")));
        } finally {
            dropTableIfExists(table);
        }
    }

    @Test
    public void testPutRow_GetRow() {
        putRow("row_true", true, true);
        putRow("row_false", false, false);

        // read back row_true
        {
            SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(TABLE, makePk("row_true", true));
            criteria.setMaxVersions(1);
            GetRowResponse resp = ots.getRow(new GetRowRequest(criteria));
            Row row = resp.getRow();
            assertNotNull(row);

            PrimaryKey pk = row.getPrimaryKey();
            assertEquals(2, pk.getPrimaryKeyColumns().length);
            assertEquals("row_true", pk.getPrimaryKeyColumns()[0].getValue().asString());
            assertEquals(PrimaryKeyType.BOOLEAN, pk.getPrimaryKeyColumns()[1].getValue().getType());
            assertTrue(pk.getPrimaryKeyColumns()[1].getValue().asBoolean());

            Column[] cols = row.getColumns();
            assertEquals(1, cols.length);
            assertEquals("col_bool", cols[0].getName());
            assertEquals(ColumnType.BOOLEAN, cols[0].getValue().getType());
            assertTrue(cols[0].getValue().asBoolean());
        }

        // read back row_false
        {
            SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(TABLE, makePk("row_false", false));
            criteria.setMaxVersions(1);
            GetRowResponse resp = ots.getRow(new GetRowRequest(criteria));
            Row row = resp.getRow();
            assertNotNull(row);

            PrimaryKeyColumn[] pkCols = row.getPrimaryKey().getPrimaryKeyColumns();
            assertEquals("pk2", pkCols[1].getName());
            assertEquals(PrimaryKeyType.BOOLEAN, pkCols[1].getValue().getType());
            assertFalse(pkCols[1].getValue().asBoolean());
        }
    }

    @Test
    public void testBatchWriteRow_BatchGetRow() {
        // Seed 4 rows
        BatchWriteRowRequest writeReq = new BatchWriteRowRequest();
        for (int i = 0; i < 4; i++) {
            RowPutChange change = new RowPutChange(TABLE, makePk("batch_" + i, (i % 2 == 0)));
            change.addColumn("col_bool", ColumnValue.fromBoolean(i % 2 == 0));
            writeReq.addRowChange(change);
        }
        BatchWriteRowResponse writeResp = ots.batchWriteRow(writeReq);
        assertEquals(4, writeResp.getRowStatus(TABLE).size());
        for (BatchWriteRowResponse.RowResult rr : writeResp.getRowStatus(TABLE)) {
            assertTrue(rr.isSucceed());
        }

        // Batch get
        BatchGetRowRequest getReq = new BatchGetRowRequest();
        MultiRowQueryCriteria criteria = new MultiRowQueryCriteria(TABLE);
        for (int i = 0; i < 4; i++) {
            criteria.addRow(makePk("batch_" + i, (i % 2 == 0)));
        }
        criteria.setMaxVersions(1);
        getReq.addMultiRowQueryCriteria(criteria);
        BatchGetRowResponse getResp = ots.batchGetRow(getReq);
        List<BatchGetRowResponse.RowResult> results = getResp.getBatchGetRowResult(TABLE);
        assertEquals(4, results.size());
        for (BatchGetRowResponse.RowResult rr : results) {
            assertTrue("row failed: " + rr.getError(), rr.isSucceed());
        }
    }

    @Test
    public void testGetRange_InfMinInfMax() {
        putRow("range_a", false, false);
        putRow("range_a", true, true);

        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(TABLE);
        PrimaryKey start = new PrimaryKey(new PrimaryKeyColumn[]{
                new PrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("range_a")),
                new PrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN)
        });
        PrimaryKey end = new PrimaryKey(new PrimaryKeyColumn[]{
                new PrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("range_a")),
                new PrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MAX)
        });
        criteria.setInclusiveStartPrimaryKey(start);
        criteria.setExclusiveEndPrimaryKey(end);
        criteria.setDirection(Direction.FORWARD);
        criteria.setMaxVersions(1);

        GetRangeResponse resp = ots.getRange(new GetRangeRequest(criteria));
        List<Row> rows = resp.getRows();
        assertEquals(2, rows.size());
        // false < true
        assertFalse(rows.get(0).getPrimaryKey().getPrimaryKeyColumns()[1].getValue().asBoolean());
        assertTrue(rows.get(1).getPrimaryKey().getPrimaryKeyColumns()[1].getValue().asBoolean());
    }

    @Test
    public void testGetRange_ConcreteBoolEndpoints() {
        putRow("range_b", false, false);
        putRow("range_b", true, true);

        // [false, true) -> only false (true excluded by exclusive end)
        {
            RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(TABLE);
            criteria.setInclusiveStartPrimaryKey(new PrimaryKey(new PrimaryKeyColumn[]{
                    new PrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("range_b")),
                    new PrimaryKeyColumn("pk2", PrimaryKeyValue.fromBoolean(false))
            }));
            criteria.setExclusiveEndPrimaryKey(new PrimaryKey(new PrimaryKeyColumn[]{
                    new PrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("range_b")),
                    new PrimaryKeyColumn("pk2", PrimaryKeyValue.fromBoolean(true))
            }));
            criteria.setDirection(Direction.FORWARD);
            criteria.setMaxVersions(1);
            GetRangeResponse resp = ots.getRange(new GetRangeRequest(criteria));
            assertEquals(1, resp.getRows().size());
            assertFalse(resp.getRows().get(0).getPrimaryKey().getPrimaryKeyColumns()[1].getValue().asBoolean());
        }

        // [true, +inf) -> only true
        {
            RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(TABLE);
            criteria.setInclusiveStartPrimaryKey(new PrimaryKey(new PrimaryKeyColumn[]{
                    new PrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("range_b")),
                    new PrimaryKeyColumn("pk2", PrimaryKeyValue.fromBoolean(true))
            }));
            criteria.setExclusiveEndPrimaryKey(new PrimaryKey(new PrimaryKeyColumn[]{
                    new PrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("range_b")),
                    new PrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MAX)
            }));
            criteria.setDirection(Direction.FORWARD);
            criteria.setMaxVersions(1);
            GetRangeResponse resp = ots.getRange(new GetRangeRequest(criteria));
            assertEquals(1, resp.getRows().size());
            assertTrue(resp.getRows().get(0).getPrimaryKey().getPrimaryKeyColumns()[1].getValue().asBoolean());
        }

        // [false, +inf) -> both rows, false then true
        {
            RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(TABLE);
            criteria.setInclusiveStartPrimaryKey(new PrimaryKey(new PrimaryKeyColumn[]{
                    new PrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("range_b")),
                    new PrimaryKeyColumn("pk2", PrimaryKeyValue.fromBoolean(false))
            }));
            criteria.setExclusiveEndPrimaryKey(new PrimaryKey(new PrimaryKeyColumn[]{
                    new PrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("range_b")),
                    new PrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MAX)
            }));
            criteria.setDirection(Direction.FORWARD);
            criteria.setMaxVersions(1);
            GetRangeResponse resp = ots.getRange(new GetRangeRequest(criteria));
            assertEquals(2, resp.getRows().size());
        }
    }

}
