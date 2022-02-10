package com.alicloud.openservices.tablestore.functiontest;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
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
import com.alicloud.openservices.tablestore.common.*;
import com.google.gson.JsonSyntaxException;

public class RestrictedItemTest extends BaseFT {
    
    private static String tableName = "RestrictedItemFunctiontest";
    private static SyncClientInterface ots;
    private static final Logger LOG = LoggerFactory.getLogger(RestrictedItemTest.class);
    
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
    public void teardown() {}
    
    public static DescribeTableResponse buildDescribeTableResult(Table t) {
    	DescribeTableResponse r = new DescribeTableResponse(new Response());
        r.setTableMeta(t.getMeta());
        r.setReservedThroughputDetails(
                new ReservedThroughputDetails(
                        t.getReservedThroughput().getCapacityUnit(),
                        1,
                        1
                        ));
        r.setTableOptions(t.getTableOptions());
        return r;
    }

    /**
     * 设定MaxVersion=3, 对于不同类型的属性列（INTEGER/BINARY/STRING/BOOLEAN/DOUBLE），
     * PutRow/UpdateRow/BatchWriteRow的put或者update操作，同一个属性列并且带有3个version，
     * GetRow读出，并校验；
     * @throws UnsupportedEncodingException
     */
    @Test
    public void testCase3() throws UnsupportedEncodingException {
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("pk", PrimaryKeyType.INTEGER));
        
        OTSHelper.createTable(ots, tableName, scheme, -1, 3);
        
        Utils.waitForPartitionLoad(tableName);
        
        long ts = (new Date()).getTime();
        
        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(0))
                .build();
        
        {// put row
            List<Column> columns = new ArrayList<Column>();
            for (int i = 0; i < 3; i++) {
                columns.add(new Column("attr_0", ColumnValue.fromLong(i), ts + i));
                columns.add(new Column("attr_1", ColumnValue.fromString(String.valueOf(i)), ts + i));
                columns.add(new Column("attr_2", ColumnValue.fromDouble(i), ts + i));
                columns.add(new Column("attr_3", ColumnValue.fromBoolean(true), ts + i));
                columns.add(new Column("attr_4", ColumnValue.fromBinary((String.valueOf(i)).getBytes("UTF-8")), ts + i));
            }
            OTSHelper.putRow(ots, tableName, pk, columns);
            
            //
            Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
            
            assertTrue(null != row);
            assertEquals(pk, row.getPrimaryKey());
            
            Column[] cols = row.getColumns();
            assertEquals(15, cols.length); // 5(column) * 3(version)
            
            // attr 0
            assertEquals("attr_0", cols[0].getName());
            assertEquals(2, cols[0].getValue().asLong());
            assertEquals(ts + 2, cols[0].getTimestamp());
            
            assertEquals("attr_0", cols[1].getName());
            assertEquals(1, cols[1].getValue().asLong());
            assertEquals(ts + 1, cols[1].getTimestamp());
            
            assertEquals("attr_0", cols[2].getName());
            assertEquals(0, cols[2].getValue().asLong());
            assertEquals(ts + 0, cols[2].getTimestamp());
            
            // attr 1
            assertEquals("attr_1", cols[3].getName());
            assertEquals("2", cols[3].getValue().asString());
            assertEquals(ts + 2, cols[3].getTimestamp());
            
            assertEquals("attr_1", cols[4].getName());
            assertEquals("1", cols[4].getValue().asString());
            assertEquals(ts + 1, cols[4].getTimestamp());
            
            assertEquals("attr_1", cols[5].getName());
            assertEquals("0", cols[5].getValue().asString());
            assertEquals(ts + 0, cols[5].getTimestamp());
            
            // attr 2
            assertEquals("attr_2", cols[6].getName());
            assertEquals(true, 2 == cols[6].getValue().asDouble());
            assertEquals(ts + 2, cols[6].getTimestamp());
            
            assertEquals("attr_2", cols[7].getName());
            assertEquals(true, 1 == cols[7].getValue().asDouble());
            assertEquals(ts + 1, cols[7].getTimestamp());
            
            assertEquals("attr_2", cols[8].getName());
            assertEquals(true, 0 == cols[8].getValue().asDouble());
            assertEquals(ts + 0, cols[8].getTimestamp());
            
            // attr 3
            assertEquals("attr_3", cols[9].getName());
            assertEquals(true, cols[9].getValue().asBoolean());
            assertEquals(ts + 2, cols[9].getTimestamp());
            
            assertEquals("attr_3", cols[10].getName());
            assertEquals(true, cols[10].getValue().asBoolean());
            assertEquals(ts + 1, cols[10].getTimestamp());
            
            assertEquals("attr_3", cols[11].getName());
            assertEquals(true, cols[11].getValue().asBoolean());
            assertEquals(ts + 0, cols[11].getTimestamp());
            
            // attr 4
            assertEquals("attr_4", cols[12].getName());
            assertEquals("2", new String(cols[12].getValue().asBinary(), "UTF-8"));
            assertEquals(ts + 2, cols[12].getTimestamp());
            
            assertEquals("attr_4", cols[13].getName());
            assertEquals("1", new String(cols[13].getValue().asBinary(), "UTF-8"));
            assertEquals(ts + 1, cols[13].getTimestamp());
            
            assertEquals("attr_4", cols[14].getName());
            assertEquals("0", new String(cols[14].getValue().asBinary(), "UTF-8"));
            assertEquals(ts + 0, cols[14].getTimestamp());
        }
        
        {// Update row
            
            List<Column> columns = new ArrayList<Column>();
            for (int i = 0; i < 3; i++) {
                columns.add(new Column("attr_0", ColumnValue.fromLong(i + 1), ts + i));
                columns.add(new Column("attr_1", ColumnValue.fromString(String.valueOf(i + 1)), ts + i));
                columns.add(new Column("attr_2", ColumnValue.fromDouble(i + 1), ts + i));
                columns.add(new Column("attr_3", ColumnValue.fromBoolean(false), ts + i));
                columns.add(new Column("attr_4", ColumnValue.fromBinary((String.valueOf(i + 1)).getBytes("UTF-8")), ts + i));
            }
            
            OTSHelper.updateRow(ots, tableName, pk, columns, null, null);
            //
            Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
            
            assertTrue(null != row);
            assertEquals(pk, row.getPrimaryKey());
            
            Column[] cols = row.getColumns();
            assertEquals(15, cols.length); // 5(column) * 3(version)
            
            // attr 0
            assertEquals("attr_0", cols[0].getName());
            assertEquals(3, cols[0].getValue().asLong());
            assertEquals(ts + 2, cols[0].getTimestamp());
            
            assertEquals("attr_0", cols[1].getName());
            assertEquals(2, cols[1].getValue().asLong());
            assertEquals(ts + 1, cols[1].getTimestamp());
            
            assertEquals("attr_0", cols[2].getName());
            assertEquals(1, cols[2].getValue().asLong());
            assertEquals(ts + 0, cols[2].getTimestamp());
            
            // attr 1
            assertEquals("attr_1", cols[3].getName());
            assertEquals("3", cols[3].getValue().asString());
            assertEquals(ts + 2, cols[3].getTimestamp());
            
            assertEquals("attr_1", cols[4].getName());
            assertEquals("2", cols[4].getValue().asString());
            assertEquals(ts + 1, cols[4].getTimestamp());
            
            assertEquals("attr_1", cols[5].getName());
            assertEquals("1", cols[5].getValue().asString());
            assertEquals(ts + 0, cols[5].getTimestamp());
            
            // attr 2
            assertEquals("attr_2", cols[6].getName());
            assertEquals(true, 3 == cols[6].getValue().asDouble());
            assertEquals(ts + 2, cols[6].getTimestamp());
            
            assertEquals("attr_2", cols[7].getName());
            assertEquals(true, 2 == cols[7].getValue().asDouble());
            assertEquals(ts + 1, cols[7].getTimestamp());
            
            assertEquals("attr_2", cols[8].getName());
            assertEquals(true, 1 == cols[8].getValue().asDouble());
            assertEquals(ts + 0, cols[8].getTimestamp());
            
            // attr 3
            assertEquals("attr_3", cols[9].getName());
            assertEquals(false, cols[9].getValue().asBoolean());
            assertEquals(ts + 2, cols[9].getTimestamp());
            
            assertEquals("attr_3", cols[10].getName());
            assertEquals(false, cols[10].getValue().asBoolean());
            assertEquals(ts + 1, cols[10].getTimestamp());
            
            assertEquals("attr_3", cols[11].getName());
            assertEquals(false, cols[11].getValue().asBoolean());
            assertEquals(ts + 0, cols[11].getTimestamp());
            
            // attr 4
            assertEquals("attr_4", cols[12].getName());
            assertEquals("3", new String(cols[12].getValue().asBinary(), "UTF-8"));
            assertEquals(ts + 2, cols[12].getTimestamp());
            
            assertEquals("attr_4", cols[13].getName());
            assertEquals("2", new String(cols[13].getValue().asBinary(), "UTF-8"));
            assertEquals(ts + 1, cols[13].getTimestamp());
            
            assertEquals("attr_4", cols[14].getName());
            assertEquals("1", new String(cols[14].getValue().asBinary(), "UTF-8"));
            assertEquals(ts + 0, cols[14].getTimestamp());
        }
        
        {// Batch write row
            List<RowPutChange> puts = new ArrayList<RowPutChange>();
            RowPutChange put = new RowPutChange(tableName, pk);
            for (int i = 0; i < 3; i++) {
                put.addColumn(new Column("attr_0", ColumnValue.fromLong(i), ts + i));
                put.addColumn(new Column("attr_1", ColumnValue.fromString(String.valueOf(i)), ts + i));
                put.addColumn(new Column("attr_2", ColumnValue.fromDouble(i), ts + i));
                put.addColumn(new Column("attr_3", ColumnValue.fromBoolean(true), ts + i));
                put.addColumn(new Column("attr_4", ColumnValue.fromBinary(String.valueOf(i).getBytes("UTF-8")), ts + i));
            }
            puts.add(put);
            OTSHelper.batchWriteRowNoLimit(ots, puts, null, null);
            
            //
            Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
            
            assertTrue(null != row);
            assertEquals(pk, row.getPrimaryKey());
            
            Column[] cols = row.getColumns();
            assertEquals(15, cols.length); // 5(column) * 3(version)
            
            // attr 0
            assertEquals("attr_0", cols[0].getName());
            assertEquals(2, cols[0].getValue().asLong());
            assertEquals(ts + 2, cols[0].getTimestamp());
            
            assertEquals("attr_0", cols[1].getName());
            assertEquals(1, cols[1].getValue().asLong());
            assertEquals(ts + 1, cols[1].getTimestamp());
            
            assertEquals("attr_0", cols[2].getName());
            assertEquals(0, cols[2].getValue().asLong());
            assertEquals(ts + 0, cols[2].getTimestamp());
            
            // attr 1
            assertEquals("attr_1", cols[3].getName());
            assertEquals("2", cols[3].getValue().asString());
            assertEquals(ts + 2, cols[3].getTimestamp());
            
            assertEquals("attr_1", cols[4].getName());
            assertEquals("1", cols[4].getValue().asString());
            assertEquals(ts + 1, cols[4].getTimestamp());
            
            assertEquals("attr_1", cols[5].getName());
            assertEquals("0", cols[5].getValue().asString());
            assertEquals(ts + 0, cols[5].getTimestamp());
            
            // attr 2
            assertEquals("attr_2", cols[6].getName());
            assertEquals(true, 2 == cols[6].getValue().asDouble());
            assertEquals(ts + 2, cols[6].getTimestamp());
            
            assertEquals("attr_2", cols[7].getName());
            assertEquals(true, 1 == cols[7].getValue().asDouble());
            assertEquals(ts + 1, cols[7].getTimestamp());
            
            assertEquals("attr_2", cols[8].getName());
            assertEquals(true, 0 == cols[8].getValue().asDouble());
            assertEquals(ts + 0, cols[8].getTimestamp());
            
            // attr 3
            assertEquals("attr_3", cols[9].getName());
            assertEquals(true, cols[9].getValue().asBoolean());
            assertEquals(ts + 2, cols[9].getTimestamp());
            
            assertEquals("attr_3", cols[10].getName());
            assertEquals(true, cols[10].getValue().asBoolean());
            assertEquals(ts + 1, cols[10].getTimestamp());
            
            assertEquals("attr_3", cols[11].getName());
            assertEquals(true, cols[11].getValue().asBoolean());
            assertEquals(ts + 0, cols[11].getTimestamp());
            
            // attr 4
            assertEquals("attr_4", cols[12].getName());
            assertEquals("2", new String(cols[12].getValue().asBinary(), "UTF-8"));
            assertEquals(ts + 2, cols[12].getTimestamp());
            
            assertEquals("attr_4", cols[13].getName());
            assertEquals("1", new String(cols[13].getValue().asBinary(), "UTF-8"));
            assertEquals(ts + 1, cols[13].getTimestamp());
            
            assertEquals("attr_4", cols[14].getName());
            assertEquals("0", new String(cols[14].getValue().asBinary(), "UTF-8"));
            assertEquals(ts + 0, cols[14].getTimestamp());
        }
        {
            List<RowUpdateChange> updates = new ArrayList<RowUpdateChange>();
            RowUpdateChange update = new RowUpdateChange(tableName, pk);
            for (int i = 0; i < 3; i++) {
                update.put(new Column("attr_0", ColumnValue.fromLong(i + 1), ts + i));
                update.put(new Column("attr_1", ColumnValue.fromString(String.valueOf(i + 1)), ts + i));
                update.put(new Column("attr_2", ColumnValue.fromDouble(i + 1), ts + i));
                update.put(new Column("attr_3", ColumnValue.fromBoolean(false), ts + i));
                update.put(new Column("attr_4", ColumnValue.fromBinary((String.valueOf(i + 1)).getBytes("UTF-8")), ts + i));
            }
            updates.add(update);
            OTSHelper.batchWriteRowNoLimit(ots, null, updates, null);
            
            //
            Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
            
            assertTrue(null != row);
            assertEquals(pk, row.getPrimaryKey());
            
            Column[] cols = row.getColumns();
            assertEquals(15, cols.length); // 5(column) * 3(version)
            
            // attr 0
            assertEquals("attr_0", cols[0].getName());
            assertEquals(3, cols[0].getValue().asLong());
            assertEquals(ts + 2, cols[0].getTimestamp());
            
            assertEquals("attr_0", cols[1].getName());
            assertEquals(2, cols[1].getValue().asLong());
            assertEquals(ts + 1, cols[1].getTimestamp());
            
            assertEquals("attr_0", cols[2].getName());
            assertEquals(1, cols[2].getValue().asLong());
            assertEquals(ts + 0, cols[2].getTimestamp());
            
            // attr 1
            assertEquals("attr_1", cols[3].getName());
            assertEquals("3", cols[3].getValue().asString());
            assertEquals(ts + 2, cols[3].getTimestamp());
            
            assertEquals("attr_1", cols[4].getName());
            assertEquals("2", cols[4].getValue().asString());
            assertEquals(ts + 1, cols[4].getTimestamp());
            
            assertEquals("attr_1", cols[5].getName());
            assertEquals("1", cols[5].getValue().asString());
            assertEquals(ts + 0, cols[5].getTimestamp());
            
            // attr 2
            assertEquals("attr_2", cols[6].getName());
            assertEquals(true, 3 == cols[6].getValue().asDouble());
            assertEquals(ts + 2, cols[6].getTimestamp());
            
            assertEquals("attr_2", cols[7].getName());
            assertEquals(true, 2 == cols[7].getValue().asDouble());
            assertEquals(ts + 1, cols[7].getTimestamp());
            
            assertEquals("attr_2", cols[8].getName());
            assertEquals(true, 1 == cols[8].getValue().asDouble());
            assertEquals(ts + 0, cols[8].getTimestamp());
            
            // attr 3
            assertEquals("attr_3", cols[9].getName());
            assertEquals(false, cols[9].getValue().asBoolean());
            assertEquals(ts + 2, cols[9].getTimestamp());
            
            assertEquals("attr_3", cols[10].getName());
            assertEquals(false, cols[10].getValue().asBoolean());
            assertEquals(ts + 1, cols[10].getTimestamp());
            
            assertEquals("attr_3", cols[11].getName());
            assertEquals(false, cols[11].getValue().asBoolean());
            assertEquals(ts + 0, cols[11].getTimestamp());
            
            // attr 4
            assertEquals("attr_4", cols[12].getName());
            assertEquals("3", new String(cols[12].getValue().asBinary(), "UTF-8"));
            assertEquals(ts + 2, cols[12].getTimestamp());
            
            assertEquals("attr_4", cols[13].getName());
            assertEquals("2", new String(cols[13].getValue().asBinary(), "UTF-8"));
            assertEquals(ts + 1, cols[13].getTimestamp());
            
            assertEquals("attr_4", cols[14].getName());
            assertEquals("1", new String(cols[14].getValue().asBinary(), "UTF-8"));
            assertEquals(ts + 0, cols[14].getTimestamp());
        }
    }
    
    /**
     * 设定MaxVersion=3, 对于不同类型的属性列（INTEGER/BINARY/STRING/BOOLEAN/DOUBLE），
     * UpdateRow或者BatchWriteRow的update操作 同一个属性列重复3次，每次写入同一个属性列并不指定
     * timestamp。GetRow读出，并校验；
     * @throws UnsupportedEncodingException
     */
    @Test
    public void testCase4() throws UnsupportedEncodingException {
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("pk", PrimaryKeyType.INTEGER));
        
        OTSHelper.createTable(ots, tableName, scheme, -1, 3);
        
        Utils.waitForPartitionLoad(tableName);
        
        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(0))
                .build();
        
        {// UpdateRow
            
            long ts = System.currentTimeMillis();
            for (int i = 0; i < 3; i++) {
                List<Column> columns = new ArrayList<Column>();
                columns.add(new Column("attr_0", ColumnValue.fromLong(i + 1)));
                columns.add(new Column("attr_1", ColumnValue.fromString(String.valueOf(i + 1))));
                columns.add(new Column("attr_2", ColumnValue.fromDouble(i + 1)));
                columns.add(new Column("attr_3", ColumnValue.fromBoolean(false)));
                columns.add(new Column("attr_4", ColumnValue.fromBinary((String.valueOf(i + 1)).getBytes("UTF-8"))));
                OTSHelper.updateRow(ots, tableName, pk, columns, null, null);
            }
            
            //
            Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
            
            assertTrue(null != row);
            assertEquals(pk, row.getPrimaryKey());
            
            Column[] cols = row.getColumns();
            assertEquals(15, cols.length); // 5(column) * 3(version)
            
            // attr 0
            assertEquals("attr_0", cols[0].getName());
            assertEquals(3, cols[0].getValue().asLong());
            checkTimestampWithDeviation(ts, cols[0].getTimestamp());
            
            assertEquals("attr_0", cols[1].getName());
            assertEquals(2, cols[1].getValue().asLong());
            checkTimestampWithDeviation(ts, cols[1].getTimestamp());
            
            assertEquals("attr_0", cols[2].getName());
            assertEquals(1, cols[2].getValue().asLong());
            checkTimestampWithDeviation(ts, cols[2].getTimestamp());
            
            // attr 1
            assertEquals("attr_1", cols[3].getName());
            assertEquals("3", cols[3].getValue().asString());
            checkTimestampWithDeviation(ts, cols[3].getTimestamp());
            
            assertEquals("attr_1", cols[4].getName());
            assertEquals("2", cols[4].getValue().asString());
            checkTimestampWithDeviation(ts, cols[4].getTimestamp());
            
            assertEquals("attr_1", cols[5].getName());
            assertEquals("1", cols[5].getValue().asString());
            checkTimestampWithDeviation(ts, cols[5].getTimestamp());
            
            // attr 2
            assertEquals("attr_2", cols[6].getName());
            assertEquals(true, 3 == cols[6].getValue().asDouble());
            checkTimestampWithDeviation(ts, cols[6].getTimestamp());
            
            assertEquals("attr_2", cols[7].getName());
            assertEquals(true, 2 == cols[7].getValue().asDouble());
            checkTimestampWithDeviation(ts, cols[7].getTimestamp());
            
            assertEquals("attr_2", cols[8].getName());
            assertEquals(true, 1 == cols[8].getValue().asDouble());
            checkTimestampWithDeviation(ts, cols[8].getTimestamp());
            
            // attr 3
            assertEquals("attr_3", cols[9].getName());
            assertEquals(false, cols[9].getValue().asBoolean());
            checkTimestampWithDeviation(ts, cols[9].getTimestamp());
            
            assertEquals("attr_3", cols[10].getName());
            assertEquals(false, cols[10].getValue().asBoolean());
            checkTimestampWithDeviation(ts, cols[10].getTimestamp());
            
            assertEquals("attr_3", cols[11].getName());
            assertEquals(false, cols[11].getValue().asBoolean());
            checkTimestampWithDeviation(ts, cols[11].getTimestamp());
            
            // attr 4
            assertEquals("attr_4", cols[12].getName());
            assertEquals("3", new String(cols[12].getValue().asBinary(), "UTF-8"));
            checkTimestampWithDeviation(ts, cols[12].getTimestamp());
            
            assertEquals("attr_4", cols[13].getName());
            assertEquals("2", new String(cols[13].getValue().asBinary(), "UTF-8"));
            checkTimestampWithDeviation(ts, cols[13].getTimestamp());
            
            assertEquals("attr_4", cols[14].getName());
            assertEquals("1", new String(cols[14].getValue().asBinary(), "UTF-8"));
            checkTimestampWithDeviation(ts, cols[14].getTimestamp());
        }
        
        OTSHelper.deleteRow(ots, tableName, pk);
        {
            long ts = System.currentTimeMillis();
            
            for (int i = 0; i < 3; i++) {
                List<RowUpdateChange> updates = new ArrayList<RowUpdateChange>();
                RowUpdateChange update = new RowUpdateChange(tableName, pk);
                update.put(new Column("attr_0", ColumnValue.fromLong(i + 1)));
                update.put(new Column("attr_1", ColumnValue.fromString(String.valueOf(i + 1))));
                update.put(new Column("attr_2", ColumnValue.fromDouble(i + 1)));
                update.put(new Column("attr_3", ColumnValue.fromBoolean(false)));
                update.put(new Column("attr_4", ColumnValue.fromBinary((String.valueOf(i + 1)).getBytes("UTF-8"))));
                updates.add(update);
                OTSHelper.batchWriteRowNoLimit(ots, null, updates, null);
            }
            
            //
            Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
            
            assertTrue(null != row);
            assertEquals(pk, row.getPrimaryKey());
            
            Column[] cols = row.getColumns();
            
            assertEquals(15, cols.length); // 5(column) * 3(version)
            
            // attr 0
            assertEquals("attr_0", cols[0].getName());
            assertEquals(3, cols[0].getValue().asLong());
            checkTimestampWithDeviation(ts, cols[0].getTimestamp());
            
            assertEquals("attr_0", cols[1].getName());
            assertEquals(2, cols[1].getValue().asLong());
            checkTimestampWithDeviation(ts, cols[1].getTimestamp());
            
            assertEquals("attr_0", cols[2].getName());
            assertEquals(1, cols[2].getValue().asLong());
            checkTimestampWithDeviation(ts, cols[2].getTimestamp());
            
            // attr 1
            assertEquals("attr_1", cols[3].getName());
            assertEquals("3", cols[3].getValue().asString());
            checkTimestampWithDeviation(ts, cols[3].getTimestamp());
            
            assertEquals("attr_1", cols[4].getName());
            assertEquals("2", cols[4].getValue().asString());
            checkTimestampWithDeviation(ts, cols[4].getTimestamp());
            
            assertEquals("attr_1", cols[5].getName());
            assertEquals("1", cols[5].getValue().asString());
            checkTimestampWithDeviation(ts, cols[5].getTimestamp());
            
            // attr 2
            assertEquals("attr_2", cols[6].getName());
            assertEquals(true, 3 == cols[6].getValue().asDouble());
            checkTimestampWithDeviation(ts, cols[6].getTimestamp());
            
            assertEquals("attr_2", cols[7].getName());
            assertEquals(true, 2 == cols[7].getValue().asDouble());
            checkTimestampWithDeviation(ts, cols[7].getTimestamp());
            
            assertEquals("attr_2", cols[8].getName());
            assertEquals(true, 1 == cols[8].getValue().asDouble());
            checkTimestampWithDeviation(ts, cols[8].getTimestamp());
            
            // attr 3
            assertEquals("attr_3", cols[9].getName());
            assertEquals(false, cols[9].getValue().asBoolean());
            checkTimestampWithDeviation(ts, cols[9].getTimestamp());
            
            assertEquals("attr_3", cols[10].getName());
            assertEquals(false, cols[10].getValue().asBoolean());
            checkTimestampWithDeviation(ts, cols[10].getTimestamp());
            
            assertEquals("attr_3", cols[11].getName());
            assertEquals(false, cols[11].getValue().asBoolean());
            checkTimestampWithDeviation(ts, cols[11].getTimestamp());
            
            // attr 4
            assertEquals("attr_4", cols[12].getName());
            assertEquals("3", new String(cols[12].getValue().asBinary(), "UTF-8"));
            checkTimestampWithDeviation(ts, cols[12].getTimestamp());
            
            assertEquals("attr_4", cols[13].getName());
            assertEquals("2", new String(cols[13].getValue().asBinary(), "UTF-8"));
            checkTimestampWithDeviation(ts, cols[13].getTimestamp());
            
            assertEquals("attr_4", cols[14].getName());
            assertEquals("1", new String(cols[14].getValue().asBinary(), "UTF-8"));
            checkTimestampWithDeviation(ts, cols[14].getTimestamp());
        }
    }
    
    /**
     * 测试MaxVersions的最大值为INT32_MAX和最小值为1，调用所有读写接口，期望可以正常读写，验证数据正确性。
     */
    @Test
    public void testCase5() {
        
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("pk_0", PrimaryKeyType.INTEGER));
        
        TableMeta meta = new TableMeta(tableName);
        meta.addPrimaryKeyColumns(scheme);
        
        // max version = 1
        {
            TableOptions tableOptions = new TableOptions();
            tableOptions.setMaxVersions(1);
            tableOptions.setTimeToLive(-1);
            
            OTSHelper.createTable(ots, meta, new CapacityUnit(0, 0), tableOptions);
            Utils.waitForPartitionLoad(tableName);
            
            long ts = System.currentTimeMillis();
            // put row
            {
                // data
                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(0))
                        .build();
                
                List<Column> columns = new ArrayList<Column>();
                columns.add(new Column("attr", ColumnValue.fromLong(0))) ;
                
                OTSHelper.putRow(ots, tableName, pk, columns);
                
                // get data
                Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
                
                assertTrue(null != row);
                
                assertEquals(pk, row.getPrimaryKey());
                
                Column[] cols = row.getColumns();
                
                assertEquals(1, cols.length);
                assertEquals("attr", cols[0].getName());
                assertEquals(0, cols[0].getValue().asLong());
                checkTimestampWithDeviation(ts, cols[0].getTimestamp());
            }
            // update row
            {
                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(0))
                        .build();
                List<Column> puts = new ArrayList<Column>();
                puts.add(new Column("attr", ColumnValue.fromString("0"))) ;
                
                OTSHelper.updateRow(ots, tableName, pk, puts, null, null);
                
                // get data
                Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
                
                assertTrue(null != row);
                
                assertEquals(pk, row.getPrimaryKey());
                
                Column[] cols = row.getColumns();
                
                assertEquals(1, cols.length);
                assertEquals("attr", cols[0].getName());
                assertEquals("0", cols[0].getValue().asString());
                checkTimestampWithDeviation(ts, cols[0].getTimestamp());
            }
            // batch write row
            {
                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(0))
                        .build();
                
                List<RowPutChange> puts = new ArrayList<RowPutChange>();
                RowPutChange put = new RowPutChange(tableName, pk);
                put.addColumn("attr", ColumnValue.fromLong(1));
                puts.add(put);
                
                OTSHelper.batchWriteRowNoLimit(ots, puts, null, null);
                
                // get data
                Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
                
                assertTrue(null != row);
                
                assertEquals(pk, row.getPrimaryKey());
                
                Column[] cols = row.getColumns();
                
                assertEquals(1, cols.length);
                assertEquals("attr", cols[0].getName());
                assertEquals(1, cols[0].getValue().asLong());
                checkTimestampWithDeviation(ts, cols[0].getTimestamp());
            }
        }
        
        OTSHelper.deleteTable(ots, tableName);
        
        // max version = INT32_MAX
        {
            TableOptions tableOptions = new TableOptions();
            tableOptions.setMaxVersions(Integer.MAX_VALUE);
            tableOptions.setTimeToLive(-1);
            
            OTSHelper.createTable(ots, meta, new CapacityUnit(0, 0), tableOptions);
            Utils.waitForPartitionLoad(tableName);
            long ts = System.currentTimeMillis();
            // put row
            {
                // data
                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(0))
                        .build();
                
                List<Column> columns = new ArrayList<Column>();
                columns.add(new Column("attr", ColumnValue.fromLong(0))) ;
                
                OTSHelper.putRow(ots, tableName, pk, columns);
                
                // get data
                Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
                
                assertTrue(null != row);
                
                assertEquals(pk, row.getPrimaryKey());
                
                Column[] cols = row.getColumns();
                
                assertEquals(1, cols.length);
                assertEquals("attr", cols[0].getName());
                assertEquals(0, cols[0].getValue().asLong());
                checkTimestampWithDeviation(ts, cols[0].getTimestamp());
            }
            // update row
            {
                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(0))
                        .build();
                List<Column> puts = new ArrayList<Column>();
                puts.add(new Column("attr", ColumnValue.fromString("0"))) ;
                
                OTSHelper.updateRow(ots, tableName, pk, puts, null, null);
                
                // get data
                Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
                
                assertTrue(null != row);
                
                assertEquals(pk, row.getPrimaryKey());
                
                Column[] cols = row.getColumns();
                
                assertEquals(2, cols.length);
                assertEquals("attr", cols[0].getName());
                assertEquals("attr", cols[1].getName());
                assertEquals("0", cols[0].getValue().asString());
                assertEquals(0, cols[1].getValue().asLong());
                checkTimestampWithDeviation(ts, cols[0].getTimestamp());
                checkTimestampWithDeviation(ts, cols[1].getTimestamp());
            }
            // batch write row
            {
                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(0))
                        .build();
                
                List<RowPutChange> puts = new ArrayList<RowPutChange>();
                RowPutChange put = new RowPutChange(tableName, pk);
                put.addColumn("attr", ColumnValue.fromLong(1));
                puts.add(put);
                OTSHelper.batchWriteRowNoLimit(ots, puts, null, null);
                
                // get data
                Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
                
                assertTrue(null != row);
                
                assertEquals(pk, row.getPrimaryKey());
                
                Column[] cols = row.getColumns();
                
                assertEquals(1, cols.length);
                assertEquals("attr", cols[0].getName());
                assertEquals(1, cols[0].getValue().asLong());
                checkTimestampWithDeviation(ts, cols[0].getTimestamp());
            }
        }
    }
    
    /**
     * 测试TTL的最大值为INT64_MAX和最小值为1，调用所有读写接口，期望可以正常读写，验证数据正确性。
     */
    @Test
    public void testCase6() {
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("pk_0", PrimaryKeyType.INTEGER));
        
        TableMeta meta = new TableMeta(tableName);
        meta.addPrimaryKeyColumns(scheme);

        // TTL = INT64_MAX
        {
            TableOptions tableOptions = new TableOptions();
            tableOptions.setTimeToLive(-1);
            tableOptions.setMaxVersions(Integer.MAX_VALUE);
            
            OTSHelper.createTable(ots, meta, new CapacityUnit(0, 0), tableOptions);
            Utils.waitForPartitionLoad(tableName);
            // put row
            {
                // data
                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(0))
                        .build();
                
                List<Column> columns = new ArrayList<Column>();
                columns.add(new Column("attr", ColumnValue.fromLong(0))) ;
                
                OTSHelper.putRow(ots, tableName, pk, columns);
                
                // get data
                Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
                
                assertTrue(null != row);
                
                assertEquals(pk, row.getPrimaryKey());
                
                Column[] cols = row.getColumns();
                
                assertEquals(1, cols.length);
                
                assertEquals("attr", cols[0].getName());
                assertEquals(0, cols[0].getValue().asLong());
                checkTimestampWithDeviation(System.currentTimeMillis(), cols[0].getTimestamp());
            }
            Utils.sleepSeconds(2);
            // update row
            {
                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(0))
                        .build();
                List<Column> puts = new ArrayList<Column>();
                puts.add(new Column("attr", ColumnValue.fromString("0"))) ;
                
                OTSHelper.updateRow(ots, tableName, pk, puts, null, null);
                
                // get data
                Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
                
                assertTrue(null != row);
                
                assertEquals(pk, row.getPrimaryKey());
                
                Column[] cols = row.getColumns();
                
                assertEquals(2, cols.length);
                
                assertEquals("attr", cols[0].getName());
                assertEquals("attr", cols[1].getName());
                assertEquals("0", cols[0].getValue().asString());
                assertEquals(0, cols[1].getValue().asLong());
                checkTimestampWithDeviation(System.currentTimeMillis(), cols[0].getTimestamp());
                checkTimestampWithDeviation(System.currentTimeMillis(), cols[1].getTimestamp());
            }
            Utils.sleepSeconds(2);
            // batch write row
            {
                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(0))
                        .build();
                
                List<RowPutChange> puts = new ArrayList<RowPutChange>();
                RowPutChange put = new RowPutChange(tableName, pk);
                put.addColumn("attr", ColumnValue.fromLong(1));
                puts.add(put);
                OTSHelper.batchWriteRowNoLimit(ots, puts, null, null);
                
                // get data
                Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
                
                assertTrue(null != row);
                
                assertEquals(pk, row.getPrimaryKey());
                
                Column[] cols = row.getColumns();
                
                assertEquals(1, cols.length);
                
                assertEquals("attr", cols[0].getName());
                assertEquals(1, cols[0].getValue().asLong());
                checkTimestampWithDeviation(System.currentTimeMillis(), cols[0].getTimestamp());
            }
        }
    }
    
    /**
     * 测试版本的最大值为INT64_MAX和最小值为0，调用所有读写接口，期望可以正常读写，验证数据正确性。
     */
    @Test
    public void testCase7() {
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("pk_0", PrimaryKeyType.INTEGER));
        
        TableMeta meta = new TableMeta(tableName);
        meta.addPrimaryKeyColumns(scheme);
        
        TableOptions tableOptions = new TableOptions();
        tableOptions.setMaxVersions(Integer.MAX_VALUE);
        tableOptions.setTimeToLive(-1);
        tableOptions.setMaxTimeDeviation(Long.MAX_VALUE / 1000000);
        
        // TS = 0
        {
            OTSHelper.createTable(ots, meta, new CapacityUnit(0, 0), tableOptions);
            Utils.waitForPartitionLoad(tableName);
            
            // put row
            {
                // data
                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(0))
                        .build();
                
                List<Column> columns = new ArrayList<Column>();
                columns.add(new Column("attr", ColumnValue.fromLong(0), 0)) ;
                
                OTSHelper.putRow(ots, tableName, pk, columns);
                
                // get data
                Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
                
                assertTrue(null != row);
                
                assertEquals(pk, row.getPrimaryKey());
                
                Column[] cols = row.getColumns();
                
                assertEquals(1, cols.length);
                assertEquals("attr", cols[0].getName());
                assertEquals(0, cols[0].getValue().asLong());
                assertEquals(0, cols[0].getTimestamp());
            }
            // update row
            {
                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(0))
                        .build();
                List<Column> puts = new ArrayList<Column>();
                puts.add(new Column("attr", ColumnValue.fromString("0"), 0)) ;
                
                OTSHelper.updateRow(ots, tableName, pk, puts, null, null);
                
                // get data
                Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
                
                assertTrue(null != row);
                
                assertEquals(pk, row.getPrimaryKey());
                
                Column[] cols = row.getColumns();
                
                assertEquals(1, cols.length);
                assertEquals("attr", cols[0].getName());
                assertEquals("0", cols[0].getValue().asString());
                assertEquals(0, cols[0].getTimestamp());
            }
            // batch write row
            {
                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(0))
                        .build();
                
                List<RowPutChange> puts = new ArrayList<RowPutChange>();
                RowPutChange put = new RowPutChange(tableName, pk);
                put.addColumn("attr", ColumnValue.fromLong(1), 0);
                puts.add(put);
                OTSHelper.batchWriteRowNoLimit(ots, puts, null, null);
                
                // get data
                Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
                
                assertTrue(null != row);
                
                assertEquals(pk, row.getPrimaryKey());
                
                Column[] cols = row.getColumns();
                
                assertEquals(1, cols.length);
                assertEquals("attr", cols[0].getName());
                assertEquals(1, cols[0].getValue().asLong());
                assertEquals(0, cols[0].getTimestamp());
            }
        }
        
        OTSHelper.deleteTable(ots, tableName);
        OTSHelper.createTable(ots, meta, new CapacityUnit(0, 0), tableOptions);
        Utils.waitForPartitionLoad(tableName);
        // TS = INT64_MAX
        {
            // put row
            {
                // data
                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(0))
                        .build();
                
                List<Column> columns = new ArrayList<Column>();
                columns.add(new Column("attr", ColumnValue.fromLong(0), Long.MAX_VALUE/1000 - 1)) ;
                
                OTSHelper.putRow(ots, tableName, pk, columns);
                
                // get data
                Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
                
                assertTrue(null != row);
                
                assertEquals(pk, row.getPrimaryKey());
                
                Column[] cols = row.getColumns();
                
                assertEquals(1, cols.length);
                assertEquals("attr", cols[0].getName());
                assertEquals(0, cols[0].getValue().asLong());
                assertEquals(Long.MAX_VALUE/1000 - 1, cols[0].getTimestamp());
            }
            // update row
            {
                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(0))
                        .build();
                List<Column> puts = new ArrayList<Column>();
                puts.add(new Column("attr", ColumnValue.fromString("0"), Long.MAX_VALUE/1000 - 1)) ;
                
                OTSHelper.updateRow(ots, tableName, pk, puts, null, null);
                
                // get data
                Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
                
                assertTrue(null != row);
                
                assertEquals(pk, row.getPrimaryKey());
                
                Column[] cols = row.getColumns();
                
                assertEquals(1, cols.length);
                assertEquals("attr", cols[0].getName());
                assertEquals("0", cols[0].getValue().asString());
                assertEquals(Long.MAX_VALUE/1000 - 1, cols[0].getTimestamp());
            }
            // batch write row
            {
                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(0))
                        .build();
                
                List<RowPutChange> puts = new ArrayList<RowPutChange>();
                RowPutChange put = new RowPutChange(tableName, pk);
                put.addColumn("attr", ColumnValue.fromLong(1), Long.MAX_VALUE/1000 - 1);
                puts.add(put);
                OTSHelper.batchWriteRowNoLimit(ots, puts, null, null);
                
                // get data
                Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
                
                assertTrue(null != row);
                
                assertEquals(pk, row.getPrimaryKey());
                
                Column[] cols = row.getColumns();
                
                assertEquals(1, cols.length);
                assertEquals("attr", cols[0].getName());
                assertEquals(1, cols[0].getValue().asLong());
                assertEquals(Long.MAX_VALUE/1000 - 1, cols[0].getTimestamp());
            }
        }
    }

    /**
     * 对于每个参数包含TableName的API，TableName长度为 MaxLength + 1，期望返回ErrorCode: 
     * @throws Exception
     */
    @Test
    public void testCase10() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < OTSRestrictedItemConst.TABLE_NAME_LENGTH_MAX + 1; i++) {
            sb.append("a");
        }
        String tableName = sb.toString();
        
        // create table
        {
            List<PrimaryKeySchema> pk = new ArrayList<PrimaryKeySchema>();
            pk.add(new PrimaryKeySchema("pk_0", PrimaryKeyType.STRING));
            try {
                OTSHelper.createTable(ots, tableName, pk);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(
                        ErrorCode.INVALID_PARAMETER,
                        String.format("Invalid table name: '%s'.", tableName),
                        400,
                        e);
            }
        }
        // UpdateTable
        {
            ReservedThroughput reservedThroughputForUpdate = new ReservedThroughput();
            TableOptions tableOptionsForUpdate = new TableOptions();
            try {
                OTSHelper.updateTable(ots, tableName, reservedThroughputForUpdate, tableOptionsForUpdate);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(
                        ErrorCode.INVALID_PARAMETER,
                        String.format("Invalid table name: '%s'.", tableName),
                        400,
                        e);
            }
        }
        // DescribeTable
        {
            try {
                OTSHelper.describeTable(ots, tableName);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(
                        ErrorCode.INVALID_PARAMETER,
                        String.format("Invalid table name: '%s'.", tableName),
                        400,
                        e);
            }
        }
        // delete table
        {
            List<PrimaryKeySchema> pk = new ArrayList<PrimaryKeySchema>();
            pk.add(new PrimaryKeySchema("pk_0", PrimaryKeyType.STRING));
            try {
                OTSHelper.deleteTable(ots, tableName);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(
                        ErrorCode.INVALID_PARAMETER,
                        String.format("Invalid table name: '%s'.", tableName),
                        400,
                        e);
            }
        }
        // put row
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(1))
                    .build();
            try {
                OTSHelper.putRow(ots, tableName, pk, Collections.<Column> emptyList());
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(
                        ErrorCode.INVALID_PARAMETER,
                        String.format("Invalid table name: '%s'.", tableName),
                        400,
                        e);
            }
        }
        // get row
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(1))
                    .build();
            try {
                OTSHelper.getRowForAll(ots, tableName, pk);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(
                        ErrorCode.INVALID_PARAMETER,
                        String.format("Invalid table name: '%s'.", tableName),
                        400,
                        e);
            }
        }
        // update row
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(1))
                    .build();
            
            List<Column> puts = new ArrayList<Column>();
            puts.add(new Column("attr", ColumnValue.fromLong(1)));
            try {
                OTSHelper.updateRow(ots, tableName, pk, puts, null, null);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(
                        ErrorCode.INVALID_PARAMETER,
                        String.format("Invalid table name: '%s'.", tableName),
                        400,
                        e);
            }
        }
        // delete row
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(1))
                    .build();
            
            try {
                OTSHelper.deleteRow(ots, tableName, pk);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(
                        ErrorCode.INVALID_PARAMETER,
                        String.format("Invalid table name: '%s'.", tableName),
                        400,
                        e);
            }
        }
        // batch get row
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(1))
                    .build();
            List<PrimaryKey> primaryKeys = new ArrayList<PrimaryKey>();
            primaryKeys.add(pk);
            List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
            MultiRowQueryCriteria c = new MultiRowQueryCriteria(tableName);
            c.setRowKeys(primaryKeys);
            criterias.add(c);
            try {
                OTSHelper.batchGetRow(ots, criterias);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(
                        ErrorCode.INVALID_PARAMETER,
                        String.format("Invalid table name: '%s'.", tableName),
                        400,
                        e);
            }
        }
        // batch write row
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(1))
                    .build();
            
            List<RowPutChange> puts = new ArrayList<RowPutChange>();
            RowPutChange put = new RowPutChange(tableName, pk);
            puts.add(put);
            
            try {
                OTSHelper.batchWriteRow(ots, puts, Collections.<RowUpdateChange> emptyList(), Collections.<RowDeleteChange> emptyList());
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(
                        ErrorCode.INVALID_PARAMETER,
                        String.format("Invalid table name: '%s'.", tableName),
                        400,
                        e);
            }
        }
        // get range
        {
            {
                PrimaryKey begin = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(1))
                        .build();
                
                PrimaryKey end = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(1))
                        .build();
                
                RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
                rangeRowQueryCriteria.setInclusiveStartPrimaryKey(begin);
                rangeRowQueryCriteria.setExclusiveEndPrimaryKey(end);
                rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
                try {
                    OTSHelper.getRange(ots, rangeRowQueryCriteria);
                    fail();
                } catch (TableStoreException e) {
                    assertTableStoreException(
                            ErrorCode.INVALID_PARAMETER,
                            String.format("Invalid table name: '%s'.", tableName),
                            400,
                            e);
                }
            }
        }
    }
    
    /**
     * 对于每个参数包含TableName的API，TableName长度为 MaxLength ，期望正常
     * @throws Exception
     */
    @Test
    public void testCase11() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < OTSRestrictedItemConst.TABLE_NAME_LENGTH_MAX; i++) {
            sb.append("a");
        }
        String tableName = sb.toString();
        
        List<Column> columns = new ArrayList<Column>();
        columns.add(new Column("attr", ColumnValue.fromString("hello")));
        
        List<PrimaryKeySchema> pkMeta = new ArrayList<PrimaryKeySchema>();
        pkMeta.add(new PrimaryKeySchema("pk", PrimaryKeyType.INTEGER));

        // create table
        {
            OTSHelper.createTable(ots, tableName, pkMeta);
            Utils.waitForPartitionLoad(tableName);
        }
        
        // DescribeTable
        {
            DescribeTableResponse result = OTSHelper.describeTable(ots, tableName);
            
            Table t = OTSTableBuilder.newInstance()
                    .setTableMeta(OTSHelper.getTableMeta(tableName, pkMeta))
                    .setTableOptions(OTSHelper.getDefaultTableOptions())
                    .toTable();
            t.getTableOptions().setTimeToLive(10);
            checkDescribeTableResult(buildDescribeTableResult(t), result);
        }
        long ts = System.currentTimeMillis();
        // put row
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(1))
                    .build();
            
            OTSHelper.putRow(ots, tableName, pk, columns);
            
            // get data
            Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
            assertTrue(null != row);
            assertEquals(pk, row.getPrimaryKey());
            Column[] cols = row.getColumns();
            assertEquals(1, cols.length);
            assertEquals("attr", cols[0].getName());
            assertEquals("hello", cols[0].getValue().asString());
            checkTimestampWithDeviation(ts, cols[0].getTimestamp());
        }
        // get row
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(1))
                    .build();
            
            // get data
            Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
            assertTrue(null != row);
            assertEquals(pk, row.getPrimaryKey());
            Column[] cols = row.getColumns();
            assertEquals(1, cols.length);
            assertEquals("attr", cols[0].getName());
            assertEquals("hello", cols[0].getValue().asString());
            checkTimestampWithDeviation(ts, cols[0].getTimestamp());
        }
        // update row
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(1))
                    .build();
            
            List<Column> puts = new ArrayList<Column>();
            puts.add(new Column("attr", ColumnValue.fromLong(1)));
            
            OTSHelper.updateRow(ots, tableName, pk, puts, null, null);
            
            // get data
            Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
            assertTrue(null != row);
            assertEquals(pk, row.getPrimaryKey());
            Column[] cols = row.getColumns();
            assertEquals(2, cols.length);
            assertEquals("attr", cols[0].getName());
            assertEquals("attr", cols[1].getName());
            assertEquals(1, cols[0].getValue().asLong());
            assertEquals("hello", cols[1].getValue().asString());
            checkTimestampWithDeviation(ts, cols[0].getTimestamp());
            checkTimestampWithDeviation(ts, cols[1].getTimestamp());
            
        }
        // delete row
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(1))
                    .build();
            
            OTSHelper.deleteRow(ots, tableName, pk);
            
            Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
            assertTrue(null == row);
            
        }
        // batch get row
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(1))
                    .build();
            List<PrimaryKey> primaryKeys = new ArrayList<PrimaryKey>();
            primaryKeys.add(pk);
            List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
            MultiRowQueryCriteria c = new MultiRowQueryCriteria(tableName);
            c.setRowKeys(primaryKeys);
            c.setMaxVersions(Integer.MAX_VALUE);
            criterias.add(c);
            
            BatchGetRowResponse r = OTSHelper.batchGetRow(ots, criterias);
            List<RowResult> results = r.getBatchGetRowResult(tableName);
            
            assertEquals(1, results.size());
            
            assertTrue(null == results.get(0).getRow());
            
        }
        // batch write row
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(1))
                    .build();
            
            
            List<RowPutChange> puts = new ArrayList<RowPutChange>();
            RowPutChange put = new RowPutChange(tableName, pk);
            put.addColumn("attr", ColumnValue.fromString("world"));
            puts.add(put);
            
            OTSHelper.batchWriteRowNoLimit(ots, puts, Collections.<RowUpdateChange> emptyList(), Collections.<RowDeleteChange> emptyList());
            
            // get data
            Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
            assertTrue(null != row);
            assertEquals(pk, row.getPrimaryKey());
            Column[] cols = row.getColumns();
            assertEquals(1, cols.length);
            assertEquals("attr", cols[0].getName());
            assertEquals("world", cols[0].getValue().asString());
            checkTimestampWithDeviation(ts, cols[0].getTimestamp());
        }
        // get range
        {
            
            PrimaryKey begin = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(1))
                    .build();
            
            PrimaryKey end = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(100))
                    .build();
            
            RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(begin);
            rangeRowQueryCriteria.setExclusiveEndPrimaryKey(end);
            rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
            
            GetRangeResponse r = OTSHelper.getRange(ots, rangeRowQueryCriteria);
            
            List<Row> rows = r.getRows();
            
            assertEquals(1, rows.size());
            
            Row row = rows.get(0);
            assertEquals(begin, row.getPrimaryKey());
            Column[] cols = row.getColumns();
            assertEquals(1, cols.length);
            assertEquals("attr", cols[0].getName());
            assertEquals("world", cols[0].getValue().asString());
            checkTimestampWithDeviation(ts, cols[0].getTimestamp());
        }
        
        // delete table
        {
            List<PrimaryKeySchema> pk = new ArrayList<PrimaryKeySchema>();
            pk.add(new PrimaryKeySchema("pk_0", PrimaryKeyType.STRING));
            
            OTSHelper.deleteTable(ots, tableName);
        }
    }
    
    /**
     * 对于每个参数包含column name的API，column name长度为 MaxLength + 1
     * 期望返回ErrorCode: OTSParameterInvalid
     * @throws Exception
     */
    @Test
    public void testCase12() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < OTSRestrictedItemConst.COLUMN_NAME_LENGTH_MAX + 1; i++) {
            sb.append("a");
        }
        
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("pk_0", PrimaryKeyType.STRING));
        OTSHelper.createTable(ots, tableName, scheme);
        Utils.waitForPartitionLoad(tableName);
        String columnName = sb.toString();
        
        // put row
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(1))
                    .build();
            
            List<Column> cols = new ArrayList<Column>();
            cols.add(new Column(columnName, ColumnValue.fromLong(100)));
            try {
                OTSHelper.putRow(ots, tableName, pk, cols);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(
                        ErrorCode.INVALID_PARAMETER,
                        String.format("Invalid column name: '%s'.", columnName),
                        400,
                        e);
            }
        }
        // get row
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(1))
                    .build();
            SingleRowQueryCriteria rowQueryCriteria = new SingleRowQueryCriteria(
                    tableName, pk);
            rowQueryCriteria.addColumnsToGet(columnName);
            try {
                OTSHelper.getRow(ots, rowQueryCriteria);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(
                        ErrorCode.INVALID_PARAMETER,
                        String.format("Invalid column name: '%s'.", columnName),
                        400,
                        e);
            }
        }
        // update row
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(1))
                    .build();
            
            List<Column> puts = new ArrayList<Column>();
            puts.add(new Column(columnName, ColumnValue.fromLong(1)));
            try {
                OTSHelper.updateRow(ots, tableName, pk, puts, null, null);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(
                        ErrorCode.INVALID_PARAMETER,
                        String.format("Invalid column name: '%s'.", columnName),
                        400,
                        e);
            }
        }
        // batch get row
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(1))
                    .build();
            List<PrimaryKey> primaryKeys = new ArrayList<PrimaryKey>();
            primaryKeys.add(pk);
            List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
            MultiRowQueryCriteria c = new MultiRowQueryCriteria(tableName);
            c.addColumnsToGet(columnName);
            c.setRowKeys(primaryKeys);
            criterias.add(c);
            try {
                
                OTSHelper.batchGetRow(ots, criterias);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(
                        ErrorCode.INVALID_PARAMETER,
                        String.format("Invalid column name: '%s'.", columnName),
                        400,
                        e);
            }
        }
        // batch write row
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(1))
                    .build();
            
            
            List<RowPutChange> puts = new ArrayList<RowPutChange>();
            RowPutChange put = new RowPutChange(tableName, pk);
            put.addColumn(columnName, ColumnValue.fromLong(1000));
            puts.add(put);
            
            try {
                OTSHelper.batchWriteRow(ots, puts, Collections.<RowUpdateChange> emptyList(), Collections.<RowDeleteChange> emptyList());
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(
                        ErrorCode.INVALID_PARAMETER,
                        String.format("Invalid column name: '%s'.", columnName),
                        400,
                        e);
            }
        }
        // get range
        {
            PrimaryKey begin = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(1))
                    .build();
            
            PrimaryKey end = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(1))
                    .build();
            
            RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(begin);
            rangeRowQueryCriteria.setExclusiveEndPrimaryKey(end);
            rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
            rangeRowQueryCriteria.addColumnsToGet(columnName);
            try {
                OTSHelper.getRange(ots, rangeRowQueryCriteria);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(
                        ErrorCode.INVALID_PARAMETER,
                        String.format("Invalid column name: '%s'.", columnName),
                        400,
                        e);
            }
        }
    }
    
    /**
     * 对于每个参数包含column name的API，column name长度为 MaxLength + 1，
     * 期望返回ErrorCode: OTSParameterInvalid
     * @throws Exception
     */
    @Test
    public void testCase13() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < OTSRestrictedItemConst.COLUMN_NAME_LENGTH_MAX; i++) {
            sb.append("a");
        }

        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("pk", PrimaryKeyType.INTEGER));
        OTSHelper.createTable(ots, tableName, scheme);
        Utils.waitForPartitionLoad(tableName);
        
        String columnName = sb.toString();
        
        List<Column> columns = new ArrayList<Column>();
        columns.add(new Column(columnName, ColumnValue.fromLong(100)));
        
        long ts = System.currentTimeMillis();
        
        // put row
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(1))
                    .build();
            
            
            OTSHelper.putRow(ots, tableName, pk, columns);
            
            // get data
            Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
            assertTrue(null != row);
            assertEquals(pk, row.getPrimaryKey());
            Column[] cols = row.getColumns();
            assertEquals(1, cols.length);
            assertEquals(columnName, cols[0].getName());
            assertEquals(100, cols[0].getValue().asLong());
            
            checkTimestampWithDeviation(ts, cols[0].getTimestamp());
            
        }
        // get row
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(1))
                    .build();
            SingleRowQueryCriteria rowQueryCriteria = new SingleRowQueryCriteria(
                    tableName, pk);
            rowQueryCriteria.addColumnsToGet(columnName);
            rowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
            
            Row row = OTSHelper.getRow(ots, rowQueryCriteria).getRow();
            
            assertTrue(null != row);
            
            assertEquals(pk, row.getPrimaryKey());
            
            Column[] cols = row.getColumns();
            
            assertEquals(1, cols.length);
            
            assertEquals(columnName, cols[0].getName());
            assertEquals(100, cols[0].getValue().asLong());
            checkTimestampWithDeviation(ts, cols[0].getTimestamp());
        }
        // update row
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(1))
                    .build();
            
            List<Column> puts = new ArrayList<Column>();
            puts.add(new Column(columnName, ColumnValue.fromLong(1)));
            
            OTSHelper.updateRow(ots, tableName, pk, puts, null, null);
            
            Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
            assertTrue(null != row);
            assertEquals(pk, row.getPrimaryKey());
            Column[] cols = row.getColumns();
            assertEquals(2, cols.length);
            assertEquals(columnName, cols[0].getName());
            assertEquals(columnName, cols[1].getName());
            assertEquals(1, cols[0].getValue().asLong());
            assertEquals(100, cols[1].getValue().asLong());
            checkTimestampWithDeviation(ts, cols[0].getTimestamp());
            checkTimestampWithDeviation(ts, cols[0].getTimestamp());
        }
        // batch get row
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(1))
                    .build();
            List<PrimaryKey> primaryKeys = new ArrayList<PrimaryKey>();
            primaryKeys.add(pk);
            List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
            MultiRowQueryCriteria c = new MultiRowQueryCriteria(tableName);
            c.addColumnsToGet(columnName);
            c.setRowKeys(primaryKeys);
            c.setMaxVersions(Integer.MAX_VALUE);
            criterias.add(c);
            
            BatchGetRowResponse r = OTSHelper.batchGetRow(ots, criterias);
            List<RowResult> results = r.getBatchGetRowResult(tableName);
            assertEquals(1, results.size());
            
            Row row = results.get(0).getRow();
            
            assertTrue(null != row);
            
            assertEquals(pk, row.getPrimaryKey());
            Column[] cols = row.getColumns();
            assertEquals(2, cols.length);
            assertEquals(columnName, cols[0].getName());
            assertEquals(columnName, cols[1].getName());
            assertEquals(1, cols[0].getValue().asLong());
            assertEquals(100, cols[1].getValue().asLong());
            checkTimestampWithDeviation(ts, cols[0].getTimestamp());
            checkTimestampWithDeviation(ts, cols[0].getTimestamp());
            
        }
        // batch write row
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(1))
                    .build();
            
            List<RowPutChange> puts = new ArrayList<RowPutChange>();
            RowPutChange put = new RowPutChange(tableName, pk);
            put.addColumn(columnName, ColumnValue.fromLong(1000));
            puts.add(put);
            
            OTSHelper.batchWriteRowNoLimit(ots, puts, null, null);
            
            Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
            assertTrue(null != row);
            assertEquals(pk, row.getPrimaryKey());
            Column[] cols = row.getColumns();
            assertEquals(1, cols.length);
            assertEquals(columnName, cols[0].getName());
            assertEquals(1000, cols[0].getValue().asLong());
            checkTimestampWithDeviation(ts, cols[0].getTimestamp());
        }
        // get range
        {
            PrimaryKey begin = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(1))
                    .build();
            
            PrimaryKey end = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(100))
                    .build();
            
            RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(begin);
            rangeRowQueryCriteria.setExclusiveEndPrimaryKey(end);
            rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
            rangeRowQueryCriteria.addColumnsToGet(columnName);
            
            GetRangeResponse r = OTSHelper.getRange(ots, rangeRowQueryCriteria);
            
            List<Row> rows = r.getRows();
            
            assertEquals(1, rows.size());
            
            Row row = rows.get(0);
            assertEquals(begin, row.getPrimaryKey());
            Column[] cols = row.getColumns();
            assertEquals(1, cols.length);
            assertEquals(columnName, cols[0].getName());
            assertEquals(1000, cols[0].getValue().asLong());
            checkTimestampWithDeviation(ts, cols[0].getTimestamp());
        }
    }
    
    /**
     * 一个instance有max个table，创建一个表期望失败，删除一个表后，再创建一个表期望成功
     * @throws Exception
     */
    @Test
    public void testCase15() throws Exception {
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("pk_0", PrimaryKeyType.STRING));
        
        for (int i = 0; i < OTSRestrictedItemConst.TABLE_NUMBER_MAX; i++) {
            OTSHelper.createTable(ots, tableName + i, scheme);
        }
        
        try {
            OTSHelper.createTable(ots, tableName + OTSRestrictedItemConst.TABLE_NUMBER_MAX, scheme);
            fail();
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.QUOTA_EXHAUSTED, "Number of tables exceeds the quota:" + OTSRestrictedItemConst.TABLE_NUMBER_MAX + ".", 403, e);
        }
        
        OTSHelper.deleteTable(ots, tableName + 0);
        
        OTSHelper.createTable(ots, tableName + OTSRestrictedItemConst.TABLE_NUMBER_MAX, scheme);
        
        assertTrue(Utils.checkNameExiste(OTSHelper.listTable(ots), tableName + OTSRestrictedItemConst.TABLE_NUMBER_MAX));
    }
    
    /**
     * 对于每个参数包含PrimaryKeys的API，PrimaryKeys的个数为max + 1，期望返回ErrorCode: OTSParameterInvalid
     * @throws Exception
     */
    @Test
    public void testCase16() throws Exception {
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        List<PrimaryKeyColumn> primaryKeyColumn = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> begin = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> end = new ArrayList<PrimaryKeyColumn>();
        
        for (int i = 0; i < OTSRestrictedItemConst.PRIMARY_KEY_COLUMN_NUMBER_MAX + 1; i++) {
            scheme.add(new PrimaryKeySchema("pk_" + i, PrimaryKeyType.STRING));
            primaryKeyColumn.add(new PrimaryKeyColumn("pk_" + i, PrimaryKeyValue.fromString("" + i)));
            begin.add(new PrimaryKeyColumn("pk_" + i, PrimaryKeyValue.INF_MIN));
            end.add(new PrimaryKeyColumn("pk_" + i, PrimaryKeyValue.INF_MIN));
        }
        
        List<Column> columns = new ArrayList<Column>();
        columns.add(new Column("attr", ColumnValue.fromString("hello")));
        
        { // create table
            try {
                OTSHelper.createTable(ots, tableName, scheme);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The number of primary key columns must be in range: [1, 4].", 400, e);
            }
        }
        { // put row
            try {
                OTSHelper.putRow(ots, tableName, new PrimaryKey(primaryKeyColumn), columns);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The number of primary key columns must be in range: [1, 4].", 400, e);
            }
        }

        // should create table now to avoid "Request table not exist" error when to get row.
        OTSHelper.createTable(ots, tableName, scheme.subList(0, OTSRestrictedItemConst.PRIMARY_KEY_COLUMN_NUMBER_MAX));

        { // get row
            try {
                OTSHelper.getRowForAll(ots, tableName, new PrimaryKey(primaryKeyColumn));
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The number of primary key columns must be in range: [1, 4].", 400, e);
            }
        }
        { // delete row
            try {
                OTSHelper.deleteRow(ots, tableName, new PrimaryKey(primaryKeyColumn));
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The number of primary key columns must be in range: [1, 4].", 400, e);
            }
        }
        { // update row
            try {
                OTSHelper.updateRow(ots, tableName, new PrimaryKey(primaryKeyColumn), columns, null, null);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The number of primary key columns must be in range: [1, 4].", 400, e);
            }
        }
        { // batch get row
            List<PrimaryKey> primaryKeys = new ArrayList<PrimaryKey>();
            primaryKeys.add(new PrimaryKey(primaryKeyColumn));
            List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
            MultiRowQueryCriteria c = new MultiRowQueryCriteria(tableName);
            c.setRowKeys(primaryKeys);
            c.setMaxVersions(Integer.MAX_VALUE);
            criterias.add(c);
            try {
                OTSHelper.batchGetRow(ots, criterias);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The number of primary key columns must be in range: [1, 4].", 400, e);
            }
        }
        { // batch write row
            List<RowPutChange> puts = new ArrayList<RowPutChange>();
            RowPutChange put = new RowPutChange(tableName, new PrimaryKey(primaryKeyColumn));
            put.addColumn("attr", ColumnValue.fromLong(1));
            puts.add(put);
            try {
                OTSHelper.batchWriteRow(ots, puts, null, null);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The number of primary key columns must be in range: [1, 4].", 400, e);
            }
        }
        { // get range
            
            RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(new PrimaryKey(begin));
            rangeRowQueryCriteria.setExclusiveEndPrimaryKey(new PrimaryKey(end));
            rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
            try {
                OTSHelper.getRange(ots, rangeRowQueryCriteria);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The number of primary key columns must be in range: [1, 4].", 400, e);
            }
        }
    }
    
    /**
     *  对于每个参数包含PrimaryKeys的API，PrimaryKeys的个数为max，期望正常
     * @throws Exception
     */
    @Test
    public void testCase17() throws Exception {
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        List<PrimaryKeyColumn> primaryKeyColumn = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> begin = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> end = new ArrayList<PrimaryKeyColumn>();
        
        for (int i = 0; i < OTSRestrictedItemConst.PRIMARY_KEY_COLUMN_NUMBER_MAX; i++) {
            scheme.add(new PrimaryKeySchema("pk_" + i, PrimaryKeyType.STRING));
            primaryKeyColumn.add(new PrimaryKeyColumn("pk_" + i, PrimaryKeyValue.fromString("" + i)));
            begin.add(new PrimaryKeyColumn("pk_" + i, PrimaryKeyValue.INF_MIN));
            end.add(new PrimaryKeyColumn("pk_" + i, PrimaryKeyValue.INF_MAX));
        }
        
        List<Column> columns = new ArrayList<Column>();
        columns.add(new Column("attr", ColumnValue.fromString("hello")));

        { // create table
            OTSHelper.createTable(ots, tableName, scheme);
            
            assertTrue(Utils.checkNameExiste(OTSHelper.listTable(ots), tableName));
            
            Table t = OTSTableBuilder.newInstance()
                    .setTableMeta(OTSHelper.getTableMeta(tableName, scheme))
                    .setTableOptions(OTSHelper.getDefaultTableOptions())
                    .toTable();
            
            checkDescribeTableResult(buildDescribeTableResult(t), OTSHelper.describeTable(ots, tableName));
        }
        Utils.waitForPartitionLoad(tableName);
        long ts = System.currentTimeMillis();
        
        { // put row
            
            OTSHelper.putRow(ots, tableName, new PrimaryKey(primaryKeyColumn), columns);
            
            Row row = OTSHelper.getRowForAll(ots, tableName, new PrimaryKey(primaryKeyColumn)).getRow();
            assertTrue(null != row);
            assertEquals(new PrimaryKey(primaryKeyColumn), row.getPrimaryKey());
            assertEquals(1, row.getColumns().length);
            assertEquals("attr", row.getColumns()[0].getName());
            assertEquals("hello", row.getColumns()[0].getValue().asString());
            checkTimestampWithDeviation(ts, row.getColumns()[0].getTimestamp());
        }
        { // get row
            
            Row row = OTSHelper.getRowForAll(ots, tableName, new PrimaryKey(primaryKeyColumn)).getRow();
            
            assertTrue(null != row);
            assertEquals(new PrimaryKey(primaryKeyColumn), row.getPrimaryKey());
            Column[] cols = row.getColumns();
            assertEquals(1, cols.length);
            assertEquals("attr", row.getColumns()[0].getName());
            assertEquals("hello", row.getColumns()[0].getValue().asString());
            checkTimestampWithDeviation(ts, cols[0].getTimestamp());
        }
        { // delete row
            OTSHelper.deleteRow(ots, tableName, new PrimaryKey(primaryKeyColumn));
            Row row = OTSHelper.getRowForAll(ots, tableName, new PrimaryKey(primaryKeyColumn)).getRow();
            
            assertTrue(null == row);
        }
        { // update row
            
            OTSHelper.updateRow(ots, tableName, new PrimaryKey(primaryKeyColumn), columns, null, null);
            Row row = OTSHelper.getRowForAll(ots, tableName, new PrimaryKey(primaryKeyColumn)).getRow();
            
            assertTrue(null != row);
            assertEquals(new PrimaryKey(primaryKeyColumn), row.getPrimaryKey());
            Column[] cols = row.getColumns();
            assertEquals(1, cols.length);
            assertEquals("attr", row.getColumns()[0].getName());
            assertEquals("hello", row.getColumns()[0].getValue().asString());
            checkTimestampWithDeviation(ts, cols[0].getTimestamp());
        }
        { // batch get row
            List<PrimaryKey> primaryKeys = new ArrayList<PrimaryKey>();
            primaryKeys.add(new PrimaryKey(primaryKeyColumn));
            List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
            MultiRowQueryCriteria c = new MultiRowQueryCriteria(tableName);
            c.setRowKeys(primaryKeys);
            c.setMaxVersions(Integer.MAX_VALUE);
            criterias.add(c);
            
            BatchGetRowResponse r = OTSHelper.batchGetRow(ots, criterias);
            List<RowResult> results = r.getBatchGetRowResult(tableName);
            assertEquals(1, results.size());
            
            Row row = results.get(0).getRow();
            
            assertTrue(null != row);
            
            assertEquals(new PrimaryKey(primaryKeyColumn), row.getPrimaryKey());
            Column[] cols = row.getColumns();
            assertEquals(1, cols.length);
            assertEquals("attr", row.getColumns()[0].getName());
            assertEquals("hello", row.getColumns()[0].getValue().asString());
            checkTimestampWithDeviation(ts, cols[0].getTimestamp());
            
        }
        { // batch write row
            List<RowPutChange> puts = new ArrayList<RowPutChange>();
            RowPutChange put = new RowPutChange(tableName, new PrimaryKey(primaryKeyColumn));
            put.addColumn("attr", ColumnValue.fromLong(1));
            puts.add(put);
            
            OTSHelper.batchWriteRowNoLimit(ots, puts, null, null);
            
            Row row = OTSHelper.getRowForAll(ots, tableName, new PrimaryKey(primaryKeyColumn)).getRow();
            
            assertTrue(null != row);
            assertEquals(new PrimaryKey(primaryKeyColumn), row.getPrimaryKey());
            Column[] cols = row.getColumns();
            assertEquals(1, cols.length);
            assertEquals("attr", row.getColumns()[0].getName());
            assertEquals(1, row.getColumns()[0].getValue().asLong());
            checkTimestampWithDeviation(ts, cols[0].getTimestamp());
        }
        { // get range
            
            RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(new PrimaryKey(begin));
            rangeRowQueryCriteria.setExclusiveEndPrimaryKey(new PrimaryKey(end));
            rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
            
            GetRangeResponse r = OTSHelper.getRange(ots, rangeRowQueryCriteria);
            
            List<Row> rows = r.getRows();
            
            assertEquals(1, rows.size());
            
            Row row = rows.get(0);
            Column[] cols = row.getColumns();
            assertEquals(1, cols.length);
            assertEquals("attr", row.getColumns()[0].getName());
            assertEquals(1, row.getColumns()[0].getValue().asLong());
            checkTimestampWithDeviation(ts, cols[0].getTimestamp());
        }
    }
    
    /**
     * 对于每个参数包含PrimaryKeys的API，PrimaryKeys的个数为0，期望返回IllegalArgumentException
     * @throws Exception
     */
    @Test
    public void testCase18() throws Exception {
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        List<PrimaryKeyColumn> primaryKeyColumn = new ArrayList<PrimaryKeyColumn>();
        
        List<Column> columns = new ArrayList<Column>();
        columns.add(new Column("attr", ColumnValue.fromString("hello")));
        
        { // create table
            try {
                OTSHelper.createTable(ots, tableName, scheme);
                fail();
            } catch (IllegalArgumentException e) {
                assertEquals("The primary key schema should not be null or empty.", e.getMessage());
            }
        }
    }
    
    /**
     * 对于每个参数包含PrimaryKeys的API，PK的string value长度为max + 1，期望返回ErrorCode: OTSParameterInvalid
     */
    @Test
    public void testCase19() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < OTSRestrictedItemConst.PRIMARY_KEY_VALUE_STRING_LENGTH_MAX + 1; i++) {
            sb.append("a");
        }
        String value = sb.toString();
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        List<PrimaryKeyColumn> primaryKeyColumn = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> begin = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> end = new ArrayList<PrimaryKeyColumn>();
        
        scheme.add(new PrimaryKeySchema("pk_0", PrimaryKeyType.STRING));
        primaryKeyColumn.add(new PrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString(value)));
        
        begin = primaryKeyColumn;
        end.add(new PrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MAX));
        
        List<Column> columns = new ArrayList<Column>();
        columns.add(new Column("attr", ColumnValue.fromString("hello")));
        
        OTSHelper.createTable(ots, tableName, scheme);
        
        Utils.waitForPartitionLoad(tableName);
        
        { // put row
            try {
                OTSHelper.putRow(ots, tableName, new PrimaryKey(primaryKeyColumn), columns);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The length of primary key column: 'pk_0' exceeds the MaxLength:1024 with CurrentLength:1025.", 400, e);
            }
        }
        { // get row
            try {
                OTSHelper.getRowForAll(ots, tableName, new PrimaryKey(primaryKeyColumn));
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The length of primary key column: 'pk_0' exceeds the MaxLength:1024 with CurrentLength:1025.", 400, e);
            }
        }
        { // delete row
            try {
                OTSHelper.deleteRow(ots, tableName, new PrimaryKey(primaryKeyColumn));
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The length of primary key column: 'pk_0' exceeds the MaxLength:1024 with CurrentLength:1025.", 400, e);
            }
        }
        { // update row
            try {
                OTSHelper.updateRow(ots, tableName, new PrimaryKey(primaryKeyColumn), columns, null, null);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The length of primary key column: 'pk_0' exceeds the MaxLength:1024 with CurrentLength:1025.", 400, e);
            }
        }
        { // batch get row
            List<PrimaryKey> primaryKeys = new ArrayList<PrimaryKey>();
            primaryKeys.add(new PrimaryKey(primaryKeyColumn));
            List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
            MultiRowQueryCriteria c = new MultiRowQueryCriteria(tableName);
            c.setRowKeys(primaryKeys);
            c.setMaxVersions(Integer.MAX_VALUE);
            criterias.add(c);
            try {
                OTSHelper.batchGetRow(ots, criterias);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The length of primary key column: 'pk_0' exceeds the MaxLength:1024 with CurrentLength:1025.", 400, e);
            }
        }
        { // batch write row
            List<RowPutChange> puts = new ArrayList<RowPutChange>();
            RowPutChange put = new RowPutChange(tableName, new PrimaryKey(primaryKeyColumn));
            put.addColumn("attr", ColumnValue.fromLong(1));
            puts.add(put);
            try {
                OTSHelper.batchWriteRow(ots, puts, null, null);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The length of primary key column: 'pk_0' exceeds the MaxLength:1024 with CurrentLength:1025.", 400, e);
            }
        }
        { // get range
            
            RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(new PrimaryKey(begin));
            rangeRowQueryCriteria.setExclusiveEndPrimaryKey(new PrimaryKey(end));
            rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
            try {
                OTSHelper.getRange(ots, rangeRowQueryCriteria);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The length of primary key column: 'pk_0' exceeds the MaxLength:1024 with CurrentLength:1025.", 400, e);
            }
        }
    }
    
    /**
     * 对于每个参数包含PrimaryKeys的API，PK的string value长度为max，期望正常
     */
    @Test
    public void testCase20() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < OTSRestrictedItemConst.PRIMARY_KEY_VALUE_STRING_LENGTH_MAX; i++) {
            sb.append("a");
        }
        String value = sb.toString();
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        List<PrimaryKeyColumn> primaryKeyColumn = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> begin = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> end = new ArrayList<PrimaryKeyColumn>();
        
        scheme.add(new PrimaryKeySchema("pk_0", PrimaryKeyType.STRING));
        primaryKeyColumn.add(new PrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString(value)));
        begin = primaryKeyColumn;
        end.add(new PrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MAX));
        
        List<Column> columns = new ArrayList<Column>();
        columns.add(new Column("attr", ColumnValue.fromString("hello")));
        
        OTSHelper.createTable(ots, tableName, scheme);
        
        Utils.waitForPartitionLoad(tableName);
        
        long ts = System.currentTimeMillis();
        
        { // put row
            OTSHelper.putRow(ots, tableName, new PrimaryKey(primaryKeyColumn), columns);
            
            Row row = OTSHelper.getRowForAll(ots, tableName, new PrimaryKey(primaryKeyColumn)).getRow();
            assertTrue(null != row);
            assertEquals(new PrimaryKey(primaryKeyColumn), row.getPrimaryKey());
            assertEquals(1, row.getColumns().length);
            assertEquals("attr", row.getColumns()[0].getName());
            assertEquals("hello", row.getColumns()[0].getValue().asString());
            checkTimestampWithDeviation(ts, row.getColumns()[0].getTimestamp());
        }
        { // get row
            Row row = OTSHelper.getRowForAll(ots, tableName, new PrimaryKey(primaryKeyColumn)).getRow();
            assertTrue(null != row);
            assertEquals(new PrimaryKey(primaryKeyColumn), row.getPrimaryKey());
            assertEquals(1, row.getColumns().length);
            assertEquals("attr", row.getColumns()[0].getName());
            assertEquals("hello", row.getColumns()[0].getValue().asString());
            checkTimestampWithDeviation(ts, row.getColumns()[0].getTimestamp());
        }
        { // delete row
            OTSHelper.deleteRow(ots, tableName, new PrimaryKey(primaryKeyColumn));
            Row row = OTSHelper.getRowForAll(ots, tableName, new PrimaryKey(primaryKeyColumn)).getRow();
            assertTrue(null == row);
        }
        { // update row
            OTSHelper.updateRow(ots, tableName, new PrimaryKey(primaryKeyColumn), columns, null, null);
            
            Row row = OTSHelper.getRowForAll(ots, tableName, new PrimaryKey(primaryKeyColumn)).getRow();
            assertTrue(null != row);
            assertEquals(new PrimaryKey(primaryKeyColumn), row.getPrimaryKey());
            assertEquals(1, row.getColumns().length);
            assertEquals("attr", row.getColumns()[0].getName());
            assertEquals("hello", row.getColumns()[0].getValue().asString());
            checkTimestampWithDeviation(ts, row.getColumns()[0].getTimestamp());
        }
        { // batch get row
            List<PrimaryKey> primaryKeys = new ArrayList<PrimaryKey>();
            primaryKeys.add(new PrimaryKey(primaryKeyColumn));
            List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
            MultiRowQueryCriteria c = new MultiRowQueryCriteria(tableName);
            c.setRowKeys(primaryKeys);
            c.setMaxVersions(Integer.MAX_VALUE);
            criterias.add(c);
            
            BatchGetRowResponse r = OTSHelper.batchGetRow(ots, criterias);
            List<RowResult> results = r.getBatchGetRowResult(tableName);
            assertEquals(1, results.size());
            
            Row row = results.get(0).getRow();
            
            assertTrue(null != row);
            
            assertEquals(new PrimaryKey(primaryKeyColumn), row.getPrimaryKey());
            Column[] cols = row.getColumns();
            assertEquals(1, cols.length);
            assertEquals("attr", row.getColumns()[0].getName());
            assertEquals("hello", row.getColumns()[0].getValue().asString());
            checkTimestampWithDeviation(ts, row.getColumns()[0].getTimestamp());
        }
        { // batch write row
            List<RowPutChange> puts = new ArrayList<RowPutChange>();
            RowPutChange put = new RowPutChange(tableName, new PrimaryKey(primaryKeyColumn));
            put.addColumn("attr", ColumnValue.fromLong(1));
            puts.add(put);
            OTSHelper.batchWriteRowNoLimit(ots, puts, null, null);
            
            Row row = OTSHelper.getRowForAll(ots, tableName, new PrimaryKey(primaryKeyColumn)).getRow();
            
            assertTrue(null != row);
            assertEquals(new PrimaryKey(primaryKeyColumn), row.getPrimaryKey());
            Column[] cols = row.getColumns();
            assertEquals(1, cols.length);
            assertEquals("attr", row.getColumns()[0].getName());
            assertEquals(1, row.getColumns()[0].getValue().asLong());
            checkTimestampWithDeviation(ts, cols[0].getTimestamp());
        }
        { // get range
            
            RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(new PrimaryKey(begin));
            rangeRowQueryCriteria.setExclusiveEndPrimaryKey(new PrimaryKey(end));
            rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
            
            GetRangeResponse r = OTSHelper.getRange(ots, rangeRowQueryCriteria);
            
            List<Row> rows = r.getRows();
            
            assertEquals(1, rows.size());
            
            Row row = rows.get(0);
            Column[] cols = row.getColumns();
            assertEquals(1, cols.length);
            assertEquals("attr", row.getColumns()[0].getName());
            assertEquals(1, row.getColumns()[0].getValue().asLong());
            checkTimestampWithDeviation(ts, cols[0].getTimestamp());
        }
    }
    
    /**
     * 对于每个参数包含PrimaryKeys的API，PK的binary value长度为max + 1，期望返回ErrorCode: OTSParameterInvalid
     * @throws UnsupportedEncodingException
     */
    @Test
    public void testCase21() throws UnsupportedEncodingException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < OTSRestrictedItemConst.PRIMARY_KEY_VALUE_BINARY_LENGTH_MAX + 1; i++) {
            sb.append("a");
        }
        String value = sb.toString();
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        List<PrimaryKeyColumn> primaryKeyColumn = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> begin = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> end = new ArrayList<PrimaryKeyColumn>();
        
        scheme.add(new PrimaryKeySchema("pk_0", PrimaryKeyType.BINARY));
        primaryKeyColumn.add(new PrimaryKeyColumn("pk_0", PrimaryKeyValue.fromBinary(value.getBytes("UTF-8"))));
        begin = primaryKeyColumn;
        end.add(new PrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MAX));
        
        List<Column> columns = new ArrayList<Column>();
        columns.add(new Column("attr", ColumnValue.fromString("hello")));
        
        OTSHelper.createTable(ots, tableName, scheme);
        
        Utils.waitForPartitionLoad(tableName);
        
        { // put row
            try {
                OTSHelper.putRow(ots, tableName, new PrimaryKey(primaryKeyColumn), columns);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The length of primary key column: 'pk_0' exceeds the MaxLength:1024 with CurrentLength:1025.", 400, e);
            }
        }
        { // get row
            try {
                OTSHelper.getRowForAll(ots, tableName, new PrimaryKey(primaryKeyColumn));
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The length of primary key column: 'pk_0' exceeds the MaxLength:1024 with CurrentLength:1025.", 400, e);
            }
        }
        { // delete row
            try {
                OTSHelper.deleteRow(ots, tableName, new PrimaryKey(primaryKeyColumn));
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The length of primary key column: 'pk_0' exceeds the MaxLength:1024 with CurrentLength:1025.", 400, e);
            }
        }
        { // update row
            try {
                OTSHelper.updateRow(ots, tableName, new PrimaryKey(primaryKeyColumn), columns, null, null);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The length of primary key column: 'pk_0' exceeds the MaxLength:1024 with CurrentLength:1025.", 400, e);
            }
        }
        { // batch get row
            List<PrimaryKey> primaryKeys = new ArrayList<PrimaryKey>();
            primaryKeys.add(new PrimaryKey(primaryKeyColumn));
            List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
            MultiRowQueryCriteria c = new MultiRowQueryCriteria(tableName);
            c.setRowKeys(primaryKeys);
            c.setMaxVersions(Integer.MAX_VALUE);
            criterias.add(c);
            try {
                OTSHelper.batchGetRow(ots, criterias);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The length of primary key column: 'pk_0' exceeds the MaxLength:1024 with CurrentLength:1025.", 400, e);
            }
        }
        { // batch write row
            List<RowPutChange> puts = new ArrayList<RowPutChange>();
            RowPutChange put = new RowPutChange(tableName, new PrimaryKey(primaryKeyColumn));
            put.addColumn("attr", ColumnValue.fromLong(1));
            puts.add(put);
            try {
                OTSHelper.batchWriteRow(ots, puts, null, null);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The length of primary key column: 'pk_0' exceeds the MaxLength:1024 with CurrentLength:1025.", 400, e);
            }
        }
        { // get range
            
            RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(new PrimaryKey(begin));
            rangeRowQueryCriteria.setExclusiveEndPrimaryKey(new PrimaryKey(end));
            rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
            try {
                OTSHelper.getRange(ots, rangeRowQueryCriteria);
                fail();
            } catch (TableStoreException e) {
                System.out.println(e.toString());
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The length of primary key column: 'pk_0' exceeds the MaxLength:1024 with CurrentLength:1025.", 400, e);
            }
        }
    }
    
    /**
     * 对于每个参数包含PrimaryKeys的API，PK的binary value长度为max，期望正常
     * @throws UnsupportedEncodingException
     */
    @Test
    public void testCase22() throws UnsupportedEncodingException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < OTSRestrictedItemConst.PRIMARY_KEY_VALUE_BINARY_LENGTH_MAX; i++) {
            sb.append("a");
        }
        String value = sb.toString();
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        List<PrimaryKeyColumn> primaryKeyColumn = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> begin = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> end = new ArrayList<PrimaryKeyColumn>();
        
        scheme.add(new PrimaryKeySchema("pk_0", PrimaryKeyType.BINARY));
        primaryKeyColumn.add(new PrimaryKeyColumn("pk_0", PrimaryKeyValue.fromBinary(value.getBytes("UTF-8"))));
        begin = primaryKeyColumn;
        end.add(new PrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MAX));
        
        List<Column> columns = new ArrayList<Column>();
        columns.add(new Column("attr", ColumnValue.fromString("hello")));
        
        OTSHelper.createTable(ots, tableName, scheme);
        
        Utils.waitForPartitionLoad(tableName);
        
        long ts = System.currentTimeMillis();
        
        { // put row
            OTSHelper.putRow(ots, tableName, new PrimaryKey(primaryKeyColumn), columns);
            
            Row row = OTSHelper.getRowForAll(ots, tableName, new PrimaryKey(primaryKeyColumn)).getRow();
            assertTrue(null != row);
            assertEquals(new PrimaryKey(primaryKeyColumn), row.getPrimaryKey());
            assertEquals(1, row.getColumns().length);
            assertEquals("attr", row.getColumns()[0].getName());
            assertEquals("hello", row.getColumns()[0].getValue().asString());
            checkTimestampWithDeviation(ts, row.getColumns()[0].getTimestamp());
        }
        { // get row
            Row row = OTSHelper.getRowForAll(ots, tableName, new PrimaryKey(primaryKeyColumn)).getRow();
            assertTrue(null != row);
            assertEquals(new PrimaryKey(primaryKeyColumn), row.getPrimaryKey());
            assertEquals(1, row.getColumns().length);
            assertEquals("attr", row.getColumns()[0].getName());
            assertEquals("hello", row.getColumns()[0].getValue().asString());
            checkTimestampWithDeviation(ts, row.getColumns()[0].getTimestamp());
        }
        { // delete row
            OTSHelper.deleteRow(ots, tableName, new PrimaryKey(primaryKeyColumn));
            Row row = OTSHelper.getRowForAll(ots, tableName, new PrimaryKey(primaryKeyColumn)).getRow();
            assertTrue(null == row);
        }
        { // update row
            OTSHelper.updateRow(ots, tableName, new PrimaryKey(primaryKeyColumn), columns, null, null);
            Row row = OTSHelper.getRowForAll(ots, tableName, new PrimaryKey(primaryKeyColumn)).getRow();
            assertTrue(null != row);
            assertEquals(new PrimaryKey(primaryKeyColumn), row.getPrimaryKey());
            assertEquals(1, row.getColumns().length);
            assertEquals("attr", row.getColumns()[0].getName());
            assertEquals("hello", row.getColumns()[0].getValue().asString());
            checkTimestampWithDeviation(ts, row.getColumns()[0].getTimestamp());
        }
        { // batch get row
            List<PrimaryKey> primaryKeys = new ArrayList<PrimaryKey>();
            primaryKeys.add(new PrimaryKey(primaryKeyColumn));
            List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
            MultiRowQueryCriteria c = new MultiRowQueryCriteria(tableName);
            c.setRowKeys(primaryKeys);
            c.setMaxVersions(Integer.MAX_VALUE);
            criterias.add(c);
            
            BatchGetRowResponse r = OTSHelper.batchGetRow(ots, criterias);
            List<RowResult> results = r.getBatchGetRowResult(tableName);
            assertEquals(1, results.size());
            
            Row row = results.get(0).getRow();
            
            assertTrue(null != row);
            
            assertEquals(new PrimaryKey(primaryKeyColumn), row.getPrimaryKey());
            Column[] cols = row.getColumns();
            assertEquals(1, cols.length);
            assertEquals("attr", cols[0].getName());
            assertEquals("hello", row.getColumns()[0].getValue().asString());
            checkTimestampWithDeviation(ts, row.getColumns()[0].getTimestamp());
        }
        { // batch write row
            List<RowPutChange> puts = new ArrayList<RowPutChange>();
            RowPutChange put = new RowPutChange(tableName, new PrimaryKey(primaryKeyColumn));
            put.addColumn("attr", ColumnValue.fromLong(1));
            puts.add(put);
            OTSHelper.batchWriteRowNoLimit(ots, puts, null, null);
            
            Row row = OTSHelper.getRowForAll(ots, tableName, new PrimaryKey(primaryKeyColumn)).getRow();
            
            assertTrue(null != row);
            assertEquals(new PrimaryKey(primaryKeyColumn), row.getPrimaryKey());
            Column[] cols = row.getColumns();
            assertEquals(1, cols.length);
            assertEquals("attr", cols[0].getName());
            assertEquals(1, row.getColumns()[0].getValue().asLong());
            checkTimestampWithDeviation(ts, row.getColumns()[0].getTimestamp());
        }
        { // get range
            
            RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(new PrimaryKey(begin));
            rangeRowQueryCriteria.setExclusiveEndPrimaryKey(new PrimaryKey(end));
            rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
            
            GetRangeResponse r = OTSHelper.getRange(ots, rangeRowQueryCriteria);
            
            List<Row> rows = r.getRows();
            
            assertEquals(1, rows.size());
            
            Row row = rows.get(0);
            Column[] cols = row.getColumns();
            assertEquals(1, cols.length);
            assertEquals("attr", cols[0].getName());
            assertEquals(1, row.getColumns()[0].getValue().asLong());
            checkTimestampWithDeviation(ts, row.getColumns()[0].getTimestamp());
        }
    }
    
    /**
     * 对于每个参数包含column value的API，string value长度为max + 1，期望返回ErrorCode: OTSParameterInvalid
     */
    @Test
    public void testCase23() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < OTSRestrictedItemConst.COLUMN_VALUE_STRING_LENGTH_MAX + 1; i++) {
            sb.append("a");
        }
        String value = sb.toString();
        
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        List<PrimaryKeyColumn> primaryKeyColumn = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> begin = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> end = new ArrayList<PrimaryKeyColumn>();
        
        scheme.add(new PrimaryKeySchema("pk_0", PrimaryKeyType.STRING));
        primaryKeyColumn.add(new PrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("hello")));
        begin.add(new PrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MIN));
        end.add(new PrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MAX));
        
        List<Column> columns = new ArrayList<Column>();
        columns.add(new Column("attr", ColumnValue.fromString(value)));
        
        OTSHelper.createTable(ots, tableName, scheme);
        
        Utils.waitForPartitionLoad(tableName);
        
        { // put row
            try {
                OTSHelper.putRow(ots, tableName, new PrimaryKey(primaryKeyColumn), columns);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The length of attribute column: 'attr' exceeds the MaxLength:2097152 with CurrentLength:2097153.", 400, e);
            }
        }
        { // update row
            try {
                OTSHelper.updateRow(ots, tableName, new PrimaryKey(primaryKeyColumn), columns, null, null);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The length of attribute column: 'attr' exceeds the MaxLength:2097152 with CurrentLength:2097153.", 400, e);
            }
        }
        { // batch write row
            List<RowPutChange> puts = new ArrayList<RowPutChange>();
            RowPutChange put = new RowPutChange(tableName, new PrimaryKey(primaryKeyColumn));
            put.addColumns(columns);
            puts.add(put);
            try {
                OTSHelper.batchWriteRow(ots, puts, null, null);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The length of attribute column: 'attr' exceeds the MaxLength:2097152 with CurrentLength:2097153.", 400, e);
            }
        }
    }
    
    /**
     * 对于每个参数包含column value的API，string value长度为max，期望正常
     */
    @Test
    public void testCase24() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < OTSRestrictedItemConst.COLUMN_VALUE_STRING_LENGTH_MAX; i++) {
            sb.append("a");
        }
        String value = sb.toString();
        
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        List<PrimaryKeyColumn> primaryKeyColumn = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> begin = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> end = new ArrayList<PrimaryKeyColumn>();
        
        scheme.add(new PrimaryKeySchema("pk_0", PrimaryKeyType.STRING));
        primaryKeyColumn.add(new PrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("hello")));
        begin.add(new PrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MIN));
        end.add(new PrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MAX));
        
        List<Column> columns = new ArrayList<Column>();
        columns.add(new Column("attr", ColumnValue.fromString(value)));
        
        OTSHelper.createTable(ots, tableName, scheme);
        
        Utils.waitForPartitionLoad(tableName);
        
        long ts = System.currentTimeMillis();
        
        { // put row
            OTSHelper.putRow(ots, tableName, new PrimaryKey(primaryKeyColumn), columns);
            
            Row row = OTSHelper.getRowForAll(ots, tableName, new PrimaryKey(primaryKeyColumn)).getRow();
            assertTrue(null != row);
            assertEquals(new PrimaryKey(primaryKeyColumn), row.getPrimaryKey());
            assertEquals(1, row.getColumns().length);
            assertEquals("attr", row.getColumns()[0].getName());
            assertEquals(value, row.getColumns()[0].getValue().asString());
            checkTimestampWithDeviation(ts, row.getColumns()[0].getTimestamp());
        }
        { // update row
            OTSHelper.updateRow(ots, tableName, new PrimaryKey(primaryKeyColumn), columns, null, null);
            Row row = OTSHelper.getRowForAll(ots, tableName, new PrimaryKey(primaryKeyColumn)).getRow();
            assertTrue(null != row);
            assertEquals(new PrimaryKey(primaryKeyColumn), row.getPrimaryKey());
            assertEquals(2, row.getColumns().length);
            assertEquals("attr", row.getColumns()[0].getName());
            assertEquals("attr", row.getColumns()[1].getName());
            assertEquals(value, row.getColumns()[0].getValue().asString());
            assertEquals(value, row.getColumns()[1].getValue().asString());
            checkTimestampWithDeviation(ts, row.getColumns()[0].getTimestamp());
            checkTimestampWithDeviation(ts, row.getColumns()[1].getTimestamp());
        }
        { // batch write row
            List<RowPutChange> puts = new ArrayList<RowPutChange>();
            RowPutChange put = new RowPutChange(tableName, new PrimaryKey(primaryKeyColumn));
            put.addColumn("attr", ColumnValue.fromLong(1));
            puts.add(put);
            OTSHelper.batchWriteRowNoLimit(ots, puts, null, null);
            
            Row row = OTSHelper.getRowForAll(ots, tableName, new PrimaryKey(primaryKeyColumn)).getRow();
            
            assertTrue(null != row);
            assertEquals(new PrimaryKey(primaryKeyColumn), row.getPrimaryKey());
            Column[] cols = row.getColumns();
            assertEquals(1, cols.length);
            assertEquals("attr", cols[0].getName());
            assertEquals(1, row.getColumns()[0].getValue().asLong());
            checkTimestampWithDeviation(ts, cols[0].getTimestamp());
        }
    }
    
    /**
     * 对于每个参数包含column value的API，string value长度为0，期望正常
     */
    @Test
    public void testCase25() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 0; i++) {
            sb.append("a");
        }
        String value = sb.toString();
        
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        List<PrimaryKeyColumn> primaryKeyColumn = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> begin = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> end = new ArrayList<PrimaryKeyColumn>();
        
        scheme.add(new PrimaryKeySchema("pk_0", PrimaryKeyType.STRING));
        primaryKeyColumn.add(new PrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("hello")));
        begin.add(new PrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MIN));
        end.add(new PrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MAX));
        
        List<Column> columns = new ArrayList<Column>();
        columns.add(new Column("attr", ColumnValue.fromString(value)));
        
        OTSHelper.createTable(ots, tableName, scheme);
        
        Utils.waitForPartitionLoad(tableName);
        
        long ts = System.currentTimeMillis();
        
        { // put row
            OTSHelper.putRow(ots, tableName, new PrimaryKey(primaryKeyColumn), columns);
            
            Row row = OTSHelper.getRowForAll(ots, tableName, new PrimaryKey(primaryKeyColumn)).getRow();
            assertTrue(null != row);
            assertEquals(new PrimaryKey(primaryKeyColumn), row.getPrimaryKey());
            assertEquals(1, row.getColumns().length);
            assertEquals("attr", row.getColumns()[0].getName());
            assertEquals(value, row.getColumns()[0].getValue().asString());
            checkTimestampWithDeviation(ts, row.getColumns()[0].getTimestamp());
        }
        { // update row
            OTSHelper.updateRow(ots, tableName, new PrimaryKey(primaryKeyColumn), columns, null, null);
            Row row = OTSHelper.getRowForAll(ots, tableName, new PrimaryKey(primaryKeyColumn)).getRow();
            assertTrue(null != row);
            assertEquals(new PrimaryKey(primaryKeyColumn), row.getPrimaryKey());
            assertEquals(2, row.getColumns().length);
            assertEquals(value, row.getColumns()[0].getValue().asString());
            assertEquals(value, row.getColumns()[1].getValue().asString());
            checkTimestampWithDeviation(ts, row.getColumns()[0].getTimestamp());
            checkTimestampWithDeviation(ts, row.getColumns()[1].getTimestamp());
        }
        { // batch write row
            List<RowPutChange> puts = new ArrayList<RowPutChange>();
            RowPutChange put = new RowPutChange(tableName, new PrimaryKey(primaryKeyColumn));
            put.addColumn("attr", ColumnValue.fromLong(1));
            puts.add(put);
            OTSHelper.batchWriteRowNoLimit(ots, puts, null, null);
            
            Row row = OTSHelper.getRowForAll(ots, tableName, new PrimaryKey(primaryKeyColumn)).getRow();
            
            assertTrue(null != row);
            assertEquals(new PrimaryKey(primaryKeyColumn), row.getPrimaryKey());
            Column[] cols = row.getColumns();
            assertEquals(1, cols.length);
            assertEquals("attr", cols[0].getName());
            assertEquals(1, row.getColumns()[0].getValue().asLong());
            checkTimestampWithDeviation(ts, cols[0].getTimestamp());
        }
    }
    
    /**
     * 对于每个参数包含column value的API，binary value长度为max + 1，期望返回ErrorCode: OTSParameterInvalid
     * @throws UnsupportedEncodingException
     */
    @Test
    public void testCase26() throws UnsupportedEncodingException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < OTSRestrictedItemConst.COLUMN_VALUE_BINARY_LENGTH_MAX + 1; i++) {
            sb.append("a");
        }
        String value = sb.toString();
        
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        List<PrimaryKeyColumn> primaryKeyColumn = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> begin = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> end = new ArrayList<PrimaryKeyColumn>();
        
        scheme.add(new PrimaryKeySchema("pk_0", PrimaryKeyType.STRING));
        primaryKeyColumn.add(new PrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("hello")));
        begin.add(new PrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MIN));
        end.add(new PrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MAX));
        
        List<Column> columns = new ArrayList<Column>();
        columns.add(new Column("attr", ColumnValue.fromBinary(value.getBytes("UTF-8"))));
        
        OTSHelper.createTable(ots, tableName, scheme);
        
        Utils.waitForPartitionLoad(tableName);
        
        { // put row
            try {
                OTSHelper.putRow(ots, tableName, new PrimaryKey(primaryKeyColumn), columns);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The length of attribute column: 'attr' exceeds the MaxLength:2097152 with CurrentLength:2097153.", 400, e);
            }
        }
        { // update row
            try {
                OTSHelper.updateRow(ots, tableName, new PrimaryKey(primaryKeyColumn), columns, null, null);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The length of attribute column: 'attr' exceeds the MaxLength:2097152 with CurrentLength:2097153.", 400, e);
            }
        }
        { // batch write row
            List<RowPutChange> puts = new ArrayList<RowPutChange>();
            RowPutChange put = new RowPutChange(tableName, new PrimaryKey(primaryKeyColumn));
            put.addColumns(columns);
            puts.add(put);
            try {
                OTSHelper.batchWriteRow(ots, puts, null, null);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The length of attribute column: 'attr' exceeds the MaxLength:2097152 with CurrentLength:2097153.", 400, e);
            }
        }
    }
    
    /**
     * 对于每个参数包含column value的API，binary value长度为max，期望正常
     * @throws UnsupportedEncodingException
     */
    @Test
    public void testCase27() throws UnsupportedEncodingException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < OTSRestrictedItemConst.COLUMN_VALUE_BINARY_LENGTH_MAX; i++) {
            sb.append("a");
        }
        String value = sb.toString();
        
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        List<PrimaryKeyColumn> primaryKeyColumn = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> begin = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> end = new ArrayList<PrimaryKeyColumn>();
        
        scheme.add(new PrimaryKeySchema("pk_0", PrimaryKeyType.STRING));
        primaryKeyColumn.add(new PrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("hello")));
        begin.add(new PrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MIN));
        end.add(new PrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MAX));
        long ts = System.currentTimeMillis();
        List<Column> columns = new ArrayList<Column>();
        columns.add(new Column("attr", ColumnValue.fromBinary(value.getBytes("UTF-8")), ts));
        
        OTSHelper.createTable(ots, tableName, scheme);
        
        Utils.waitForPartitionLoad(tableName);
        
        { // put row
            OTSHelper.putRow(ots, tableName, new PrimaryKey(primaryKeyColumn), columns);
            
            Row row = OTSHelper.getRowForAll(ots, tableName, new PrimaryKey(primaryKeyColumn)).getRow();
            assertTrue(null != row);
            Row expect = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn(primaryKeyColumn)
                    .addAttrColumn("attr", ColumnValue.fromBinary(value.getBytes("UTF-8")), ts)
                    .toRow();
            
            checkRow(expect, row);
        }
        { // update row
            OTSHelper.updateRow(ots, tableName, new PrimaryKey(primaryKeyColumn), columns, null, null);
            Row row = OTSHelper.getRowForAll(ots, tableName, new PrimaryKey(primaryKeyColumn)).getRow();
            assertTrue(null != row);
            Row expect = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn(primaryKeyColumn)
                    .addAttrColumn("attr", ColumnValue.fromBinary(value.getBytes("UTF-8")), ts)
                    .toRow();
            
            checkRow(expect, row);
        }
        
        // batchGetRow
        {
            MultiRowQueryCriteria c = new MultiRowQueryCriteria(tableName);
            c.addRow(new PrimaryKey(primaryKeyColumn));
            c.setMaxVersions(Integer.MAX_VALUE);
            
            List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
            criterias.add(c);
            
            BatchGetRowResponse result = OTSHelper.batchGetRow(ots, criterias);
            List<RowResult> suc = result.getSucceedRows();
            List<RowResult> fail = result.getFailedRows();
            
            assertEquals(0, fail.size());
            assertEquals(1, suc.size());
            
            Row row = suc.get(0).getRow();
            
            Row expect = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn(primaryKeyColumn)
                    .addAttrColumn("attr", ColumnValue.fromBinary(value.getBytes("UTF-8")), ts)
                    .toRow();
            
            checkRow(expect, row);
        }
        // getRange
        {
            RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(Utils.getMinPK(scheme));
            rangeRowQueryCriteria.setExclusiveEndPrimaryKey(Utils.getMaxPK(scheme));
            rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
            GetRangeResponse result = OTSHelper.getRange(ots, rangeRowQueryCriteria);
            assertEquals(null, result.getNextStartPrimaryKey());
            assertEquals(1, result.getRows().size());
            
            Row expect = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn(primaryKeyColumn)
                    .addAttrColumn("attr", ColumnValue.fromBinary(value.getBytes("UTF-8")), ts)
                    .toRow();
            
            checkRow(expect, result.getRows().get(0));
        }
        { // batch write row
            List<RowPutChange> puts = new ArrayList<RowPutChange>();
            RowPutChange put = new RowPutChange(tableName, new PrimaryKey(primaryKeyColumn));
            put.addColumn("attr", ColumnValue.fromBinary(value.getBytes("UTF-8")), ts + 1);
            puts.add(put);
            OTSHelper.batchWriteRowNoLimit(ots, puts, null, null);
            
            Row row = OTSHelper.getRowForAll(ots, tableName, new PrimaryKey(primaryKeyColumn)).getRow();
            
            Row expect = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn(primaryKeyColumn)
                    .addAttrColumn("attr", ColumnValue.fromBinary(value.getBytes("UTF-8")), ts + 1)
                    .toRow();
            
            checkRow(expect, row);
        }
    }
    
    /**
     * 对于每个参数包含column value的API，binary value长度为0，期望正常
     * @throws UnsupportedEncodingException
     */
    @Test
    public void testCase28() throws UnsupportedEncodingException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 0; i++) {
            sb.append("a");
        }
        String value = sb.toString();
        
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        List<PrimaryKeyColumn> primaryKeyColumn = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> begin = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> end = new ArrayList<PrimaryKeyColumn>();
        
        scheme.add(new PrimaryKeySchema("pk_0", PrimaryKeyType.STRING));
        primaryKeyColumn.add(new PrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("hello")));
        begin.add(new PrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MIN));
        end.add(new PrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MAX));
        
        long ts = System.currentTimeMillis();
        List<Column> columns = new ArrayList<Column>();
        columns.add(new Column("attr", ColumnValue.fromBinary(value.getBytes("UTF-8")), ts));
        
        OTSHelper.createTable(ots, tableName, scheme);
        
        Utils.waitForPartitionLoad(tableName);
        
        { // put row
            OTSHelper.putRow(ots, tableName, new PrimaryKey(primaryKeyColumn), columns);
            // get row
            {
                Row row = OTSHelper.getRowForAll(ots, tableName, new PrimaryKey(primaryKeyColumn)).getRow();
                Row expect = OTSRowBuilder.newInstance()
                        .addPrimaryKeyColumn(primaryKeyColumn)
                        .addAttrColumn("attr", ColumnValue.fromBinary(value.getBytes("UTF-8")), ts)
                        .toRow();
                
                checkRow(expect, row);
            }
            // batchGetRow
            {
                MultiRowQueryCriteria c = new MultiRowQueryCriteria(tableName);
                c.addRow(new PrimaryKey(primaryKeyColumn));
                c.setMaxVersions(Integer.MAX_VALUE);
                
                List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
                criterias.add(c);
                
                BatchGetRowResponse result = OTSHelper.batchGetRow(ots, criterias);
                List<RowResult> suc = result.getSucceedRows();
                List<RowResult> fail = result.getFailedRows();
                
                assertEquals(0, fail.size());
                assertEquals(1, suc.size());
                
                Row row = suc.get(0).getRow();
                
                Row expect = OTSRowBuilder.newInstance()
                        .addPrimaryKeyColumn(primaryKeyColumn)
                        .addAttrColumn("attr", ColumnValue.fromBinary(value.getBytes("UTF-8")), ts)
                        .toRow();
                
                checkRow(expect, row);
            }
            // getRange
            {
                RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
                rangeRowQueryCriteria.setInclusiveStartPrimaryKey(Utils.getMinPK(scheme));
                rangeRowQueryCriteria.setExclusiveEndPrimaryKey(Utils.getMaxPK(scheme));
                rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
                GetRangeResponse result = OTSHelper.getRange(ots, rangeRowQueryCriteria);
                assertEquals(null, result.getNextStartPrimaryKey());
                assertEquals(1, result.getRows().size());
                
                Row expect = OTSRowBuilder.newInstance()
                        .addPrimaryKeyColumn(primaryKeyColumn)
                        .addAttrColumn("attr", ColumnValue.fromBinary(value.getBytes("UTF-8")), ts)
                        .toRow();
                
                checkRow(expect, result.getRows().get(0));
            }
        }
        { // update row
            OTSHelper.updateRow(ots, tableName, new PrimaryKey(primaryKeyColumn), columns, null, null);
            // get row
            {
                Row row = OTSHelper.getRowForAll(ots, tableName, new PrimaryKey(primaryKeyColumn)).getRow();
                Row expect = OTSRowBuilder.newInstance()
                        .addPrimaryKeyColumn(primaryKeyColumn)
                        .addAttrColumn("attr", ColumnValue.fromBinary(value.getBytes("UTF-8")), ts)
                        .toRow();
                
                checkRow(expect, row);
            }
            // batchGetRow
            {
                MultiRowQueryCriteria c = new MultiRowQueryCriteria(tableName);
                c.addRow(new PrimaryKey(primaryKeyColumn));
                c.setMaxVersions(Integer.MAX_VALUE);
                
                List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
                criterias.add(c);
                
                BatchGetRowResponse result = OTSHelper.batchGetRow(ots, criterias);
                List<RowResult> suc = result.getSucceedRows();
                List<RowResult> fail = result.getFailedRows();
                
                assertEquals(0, fail.size());
                assertEquals(1, suc.size());
                
                Row row = suc.get(0).getRow();
                
                Row expect = OTSRowBuilder.newInstance()
                        .addPrimaryKeyColumn(primaryKeyColumn)
                        .addAttrColumn("attr", ColumnValue.fromBinary(value.getBytes("UTF-8")), ts)
                        .toRow();
                
                checkRow(expect, row);
            }
            // getRange
            {
                RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
                rangeRowQueryCriteria.setInclusiveStartPrimaryKey(Utils.getMinPK(scheme));
                rangeRowQueryCriteria.setExclusiveEndPrimaryKey(Utils.getMaxPK(scheme));
                rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
                GetRangeResponse result = OTSHelper.getRange(ots, rangeRowQueryCriteria);
                assertEquals(null, result.getNextStartPrimaryKey());
                assertEquals(1, result.getRows().size());
                
                Row expect = OTSRowBuilder.newInstance()
                        .addPrimaryKeyColumn(primaryKeyColumn)
                        .addAttrColumn("attr", ColumnValue.fromBinary(value.getBytes("UTF-8")), ts)
                        .toRow();
                
                checkRow(expect, result.getRows().get(0));
            }
        }
        { // batch write row
            List<RowPutChange> puts = new ArrayList<RowPutChange>();
            RowPutChange put = new RowPutChange(tableName, new PrimaryKey(primaryKeyColumn));
            put.addColumn("attr", ColumnValue.fromLong(1), ts);
            puts.add(put);
            
            OTSHelper.batchWriteRowNoLimit(ots, puts, null, null);
            // get row
            {
                Row row = OTSHelper.getRowForAll(ots, tableName, new PrimaryKey(primaryKeyColumn)).getRow();
                Row expect = OTSRowBuilder.newInstance()
                        .addPrimaryKeyColumn(primaryKeyColumn)
                        .addAttrColumn("attr", ColumnValue.fromLong(1), ts)
                        .toRow();
                
                checkRow(expect, row);
            }
            // batchGetRow
            {
                MultiRowQueryCriteria c = new MultiRowQueryCriteria(tableName);
                c.addRow(new PrimaryKey(primaryKeyColumn));
                c.setMaxVersions(Integer.MAX_VALUE);
                
                List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
                criterias.add(c);
                
                BatchGetRowResponse result = OTSHelper.batchGetRow(ots, criterias);
                List<RowResult> suc = result.getSucceedRows();
                List<RowResult> fail = result.getFailedRows();
                
                assertEquals(0, fail.size());
                assertEquals(1, suc.size());
                
                Row row = suc.get(0).getRow();
                
                Row expect = OTSRowBuilder.newInstance()
                        .addPrimaryKeyColumn(primaryKeyColumn)
                        .addAttrColumn("attr", ColumnValue.fromLong(1), ts)
                        .toRow();
                
                checkRow(expect, row);
            }
            // getRange
            {
                RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
                rangeRowQueryCriteria.setInclusiveStartPrimaryKey(Utils.getMinPK(scheme));
                rangeRowQueryCriteria.setExclusiveEndPrimaryKey(Utils.getMaxPK(scheme));
                rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
                GetRangeResponse result = OTSHelper.getRange(ots, rangeRowQueryCriteria);
                assertEquals(null, result.getNextStartPrimaryKey());
                assertEquals(1, result.getRows().size());
                
                Row expect = OTSRowBuilder.newInstance()
                        .addPrimaryKeyColumn(primaryKeyColumn)
                        .addAttrColumn("attr", ColumnValue.fromLong(1), ts)
                        .toRow();
                
                checkRow(expect, result.getRows().get(0));
            }
        }
    }
    
    /**
     * MultiGetRow中包含的row个数为max + 1，期望返回ErrorCode: OTSParameterInvalid
     */
    @Test
    public void testCase29() {
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("pk_0", PrimaryKeyType.STRING));
        
        OTSHelper.createTable(ots, tableName, scheme);
        Utils.waitForPartitionLoad(tableName);
        
        List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
        
        MultiRowQueryCriteria query = new MultiRowQueryCriteria(tableName);
        for (int i = 0; i < OTSRestrictedItemConst.BATCH_GET_ROW_COUNT_MAX + 1; i++) {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("hello" + i))
                    .build();
            query.addRow(pk);
        }
        query.setMaxVersions(Integer.MAX_VALUE);
        criterias.add(query);
        try {
            OTSHelper.batchGetRow(ots, criterias);
            fail();
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PARAMETER, "Rows count exceeds the upper limit: 100.", 400, e);
        }
    }
    
    /**
     * MultiGetRow中包含的row个数为max，期望正常
     */
    @Test
    public void testCase30() {
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("pk_0", PrimaryKeyType.STRING));
        
        OTSHelper.createTable(ots, tableName, scheme);
        Utils.waitForPartitionLoad(tableName);
        
        List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
        
        MultiRowQueryCriteria query = new MultiRowQueryCriteria(tableName);
        for (int i = 0; i < OTSRestrictedItemConst.BATCH_GET_ROW_COUNT_MAX; i++) {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("hello" + i))
                    .build();
            query.addRow(pk);
        }
        query.setMaxVersions(Integer.MAX_VALUE);
        criterias.add(query);
        
        BatchGetRowResponse r = OTSHelper.batchGetRow(ots, criterias);
        List<RowResult> results = r.getBatchGetRowResult(tableName);
        assertEquals(OTSRestrictedItemConst.BATCH_GET_ROW_COUNT_MAX, results.size());
        
        for (int i = 0; i < results.size(); i++) {
            assertTrue(null == results.get(i).getRow());
            assertEquals(tableName, results.get(i).getTableName());
        }
    }
    
    /**
     * MultiWriteRow中包含的row个数为max + 1，期望返回ErrorCode: OTSParameterInvalid
     */
    @Test
    public void testCase31() {
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("pk_0", PrimaryKeyType.STRING));
        
        OTSHelper.createTable(ots, tableName, scheme);
        Utils.waitForPartitionLoad(tableName);
        
        List<RowPutChange> puts = new ArrayList<RowPutChange>();
        
        for (int i = 0; i < OTSRestrictedItemConst.BATCH_WRITE_ROW_COUNT_MAX+1; i++) {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("hello" + i))
                    .build();
            RowPutChange put = new RowPutChange(tableName, pk);
            put.addColumn("attr", ColumnValue.fromString("animal"));
            puts.add(put);
        }
        
        try {
            OTSHelper.batchWriteRow(ots, puts, null, null);
            fail();
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PARAMETER, "Rows count exceeds the upper limit: 200.", 400, e);
        }
    }
    
    /**
     * MultiWriteRow中包含的row个数为max，期望正常
     */
    @Test
    public void testCase32() {
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("pk_0", PrimaryKeyType.STRING));
        
        OTSHelper.createTable(ots, tableName, scheme);
        Utils.waitForPartitionLoad(tableName);
        
        List<RowPutChange> puts = new ArrayList<RowPutChange>();
        
        for (int i = 0; i < OTSRestrictedItemConst.BATCH_WRITE_ROW_COUNT_MAX ; i++) {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString(String.format("hello%06d", i)))
                    .build();
            RowPutChange put = new RowPutChange(tableName, pk);
            put.addColumn("attr", ColumnValue.fromString("animal"));
            puts.add(put);
        }
        
        OTSHelper.batchWriteRowNoLimit(ots, puts, null, null);
        
        List<PrimaryKeyColumn> begin = new ArrayList<PrimaryKeyColumn>();
        begin.add(new PrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MIN));
        
        List<PrimaryKeyColumn> end = new ArrayList<PrimaryKeyColumn>();
        end.add(new PrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MAX));
        
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(new PrimaryKey(begin));
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(new PrimaryKey(end));
        rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
        
        GetRangeResponse r = OTSHelper.getRange(ots, rangeRowQueryCriteria);
        
        List<Row> rows = r.getRows();
        
        assertEquals(OTSRestrictedItemConst.BATCH_WRITE_ROW_COUNT_MAX, rows.size());
        
        for (int i = 0; i < OTSRestrictedItemConst.BATCH_WRITE_ROW_COUNT_MAX; i++) {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString(String.format("hello%06d", i)))
                    .build();
            Row row = rows.get(i);
            
            assertEquals(pk, row.getPrimaryKey());
            Column[] cols = row.getColumns();
            assertEquals(1, cols.length);
            assertEquals("attr", row.getColumns()[0].getName());
            assertEquals("animal", row.getColumns()[0].getValue().asString());
        }
    }
    
    /**
     * 一个表中包含max + 1行，total size < max，GetRange读取期望2次完成
     */
    @Test
    public void testCase33() {
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("pk_0", PrimaryKeyType.INTEGER));
        
        OTSHelper.createTable(ots, tableName, scheme);
        Utils.waitForPartitionLoad(tableName);
        
        List<Column> columns = new ArrayList<Column>();
        columns.add(new Column("attr", ColumnValue.fromString("hello world")));
        
        // prapare
        for (int i = 0; i < OTSRestrictedItemConst.GET_RANGE_COUNT_MAX + 1; i++) {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(i))
                    .build();
            OTSHelper.putRow(ots, tableName, pk, columns);
        }
        
        //
        PrimaryKey inclusiveStartPrimaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MIN)
                .build();
        PrimaryKey exclusiveEndPrimaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MAX)
                .build();
        
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(inclusiveStartPrimaryKey);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(exclusiveEndPrimaryKey);
        rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
        GetRangeResponse r = OTSHelper.getRange(ots, rangeRowQueryCriteria);
        
        List<Row> rows = r.getRows();
        assertEquals(OTSRestrictedItemConst.GET_RANGE_COUNT_MAX, rows.size());
        
        for (int i = 0; i < OTSRestrictedItemConst.GET_RANGE_COUNT_MAX; i++) {
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(i))
                    .addAttrColumn("attr", ColumnValue.fromString("hello world"))
                    .toRow();
            checkRowNoTimestamp(row, rows.get(i));
        }
        
        PrimaryKey nextPK = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(OTSRestrictedItemConst.GET_RANGE_COUNT_MAX))
                .build();
        
        assertEquals(nextPK, r.getNextStartPrimaryKey());
        
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(nextPK);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(exclusiveEndPrimaryKey);
        rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
        r = OTSHelper.getRange(ots, rangeRowQueryCriteria);
        
        rows = r.getRows();
        assertEquals(1, rows.size());
        
        for (int i = 0; i < rows.size(); i++) {
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(5000 + i))
                    .addAttrColumn("attr", ColumnValue.fromString("hello world"))
                    .toRow();
            checkRowNoTimestamp(row, rows.get(i));
        }
        
        assertEquals(null, r.getNextStartPrimaryKey());
    }
    
    /**
     * 一个表中包含max行，total size < max，GetRange读取期望1次完成
     */
    @Test
    public void testCase34() {
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("pk_0", PrimaryKeyType.INTEGER));
        
        OTSHelper.createTable(ots, tableName, scheme);
        Utils.waitForPartitionLoad(tableName);
        
        List<Column> columns = new ArrayList<Column>();
        columns.add(new Column("attr", ColumnValue.fromString("hello world")));
        
        // prapare
        for (int i = 0; i < OTSRestrictedItemConst.GET_RANGE_COUNT_MAX; i++) {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(i))
                    .build();
            OTSHelper.putRow(ots, tableName, pk, columns);
        }
        
        //
        PrimaryKey inclusiveStartPrimaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MIN)
                .build();
        PrimaryKey exclusiveEndPrimaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MAX)
                .build();
        
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(inclusiveStartPrimaryKey);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(exclusiveEndPrimaryKey);
        rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
        GetRangeResponse r = OTSHelper.getRange(ots, rangeRowQueryCriteria);
        
        List<Row> rows = r.getRows();
        assertEquals(OTSRestrictedItemConst.GET_RANGE_COUNT_MAX, rows.size());
        
        for (int i = 0; i < OTSRestrictedItemConst.GET_RANGE_COUNT_MAX; i++) {
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(i))
                    .addAttrColumn("attr", ColumnValue.fromString("hello world"))
                    .toRow();
            checkRowNoTimestamp(row, rows.get(i));
        }
        
        assertEquals(null, r.getNextStartPrimaryKey());
    }
    
    /**
     * 一个表中包含max + 1 byte数据，行个数 < max，GetRange读取期望2次完成
     */
    @Test
    public void testCase35() {
        
        // 210 = (1024 * 1024) / 5000
        
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("pk_0", PrimaryKeyType.INTEGER));
        
        OTSHelper.createTable(ots, tableName, scheme);
        Utils.waitForPartitionLoad(tableName);
        
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 2000; i++) {
            sb.append("a");
        }
        
        List<Column> columns = new ArrayList<Column>();
        columns.add(new Column("attr", ColumnValue.fromString(sb.toString())));
        
        int expectCount = 0;
        int size = 0;
        while (true) {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(expectCount++))
                    .build();
            RowPutChange rowChange = new RowPutChange(tableName, pk);
            rowChange.addColumns(columns);
            
            size += rowChange.getDataSize();
            
            OTSHelper.putRow(ots, rowChange);
            
            if (size > OTSRestrictedItemConst.GET_RANGE_SIZE_MAX) {
                break;
            }
        }
        
        // 不应该大于最大限制数
        if (expectCount > OTSRestrictedItemConst.GET_RANGE_COUNT_MAX) {
            fail();
        }
        
        PrimaryKey inclusiveStartPrimaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MIN)
                .build();
        PrimaryKey exclusiveEndPrimaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MAX)
                .build();
        
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(inclusiveStartPrimaryKey);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(exclusiveEndPrimaryKey);
        rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
        GetRangeResponse r = OTSHelper.getRange(ots, rangeRowQueryCriteria);
        List<Row> rows = new ArrayList<Row>();
        rows.addAll(r.getRows());
        
        PrimaryKey nextPK = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(rows.size()))
                .build();
        
        assertEquals(nextPK, r.getNextStartPrimaryKey());
        
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(nextPK);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(exclusiveEndPrimaryKey);
        rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
        r = OTSHelper.getRange(ots, rangeRowQueryCriteria);
        
        rows.addAll(r.getRows());
        
        assertEquals(expectCount, rows.size());
        
        for (int i = 0; i < rows.size(); i++) {
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(i))
                    .addAttrColumn("attr", ColumnValue.fromString(sb.toString()))
                    .toRow();
            checkRowNoTimestamp(row, rows.get(i));
        }
        
        assertEquals(null, r.getNextStartPrimaryKey());
    }

    /**
     * 对于所有的写操作 
     * PutRow/UpdateRow（update和delete两种情况）/BatchWriteRow（put/delete/update），
     * 测试包含1024个列，期望返回正常，并且数据读出校验符合预期。
     */
    @Test
    public void testCase39() {
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("pk_0", PrimaryKeyType.INTEGER));
        
        OTSHelper.createTable(ots, tableName, scheme);
        Utils.waitForPartitionLoad(tableName);
        
        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(0))
                .build();
        
        List<Column> puts = new ArrayList<Column>();
        List<String> deletes = new ArrayList<String>();
        
        List<RowPutChange> putChanges = new ArrayList<RowPutChange>();
        List<RowUpdateChange> updateChanges = new ArrayList<RowUpdateChange>();
        List<RowUpdateChange> updateChangesForDelete = new ArrayList<RowUpdateChange>();
        
        RowPutChange putChange = new RowPutChange(tableName, pk);
        RowUpdateChange updateChange = new RowUpdateChange(tableName, pk);
        RowUpdateChange updateForDeleteChange = new RowUpdateChange(tableName, pk);
        for (int i = 0; i < OTSRestrictedItemConst.COLUMN_COUNT_MAX_IN_SINGLE_ROW; i++) {
            String columnName = String.format("attr_%06d", i);
            puts.add(new Column(columnName, ColumnValue.fromLong(i)));
            
            putChange.addColumn(new Column(columnName, ColumnValue.fromLong(i)));
            updateChange.put(new Column(columnName, ColumnValue.fromLong(i)));
            updateForDeleteChange.deleteColumns(columnName);
            
            deletes.add(columnName);
        }
        
        putChanges.add(putChange);
        updateChanges.add(updateChange);
        updateChangesForDelete.add(updateForDeleteChange);
        
        //
        {
            Utils.deleteTableIfExist(ots, tableName);
            OTSHelper.createTable(ots, tableName, scheme);
            Utils.waitForPartitionLoad(tableName);
            OTSHelper.putRow(ots, tableName, pk, puts);
            
            Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
            
            assertEquals(pk, row.getPrimaryKey());
            
            Column[] cols = row.getColumns();
            assertEquals(OTSRestrictedItemConst.COLUMN_COUNT_MAX_IN_SINGLE_ROW, cols.length);
            
            for (int i = 0; i < cols.length; i++) {
                assertEquals(String.format("attr_%06d", i), cols[i].getName());
                assertEquals(i, cols[i].getValue().asLong());
            }
            
            OTSHelper.deleteTable(ots, tableName);
        }
        
        {
            OTSHelper.createTable(ots, tableName, scheme);
            Utils.waitForPartitionLoad(tableName);
            
            OTSHelper.updateRow(ots, tableName, pk, puts, null, null);
            Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
            assertEquals(pk, row.getPrimaryKey());
            
            Column[] cols = row.getColumns();
            assertEquals(OTSRestrictedItemConst.COLUMN_COUNT_MAX_IN_SINGLE_ROW, cols.length);
            
            for (int i = 0; i < cols.length; i++) {
                System.out.println(cols[i].toString());
                LOG.info(cols[i].toString());
            }
            
            for (int i = 0; i < cols.length; i++) {
                assertEquals(String.format("attr_%06d", i), cols[i].getName());
                assertEquals(i, cols[i].getValue().asLong());
            }
        }

        OTSHelper.deleteTable(ots, tableName);
        {
            OTSHelper.createTable(ots, tableName, scheme);
            Utils.waitForPartitionLoad(tableName);
            OTSHelper.batchWriteRowNoLimit(ots, putChanges, null, null);
            
            Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
            assertEquals(pk, row.getPrimaryKey());
            
            Column[] cols = row.getColumns();
            assertEquals(OTSRestrictedItemConst.COLUMN_COUNT_MAX_IN_SINGLE_ROW, cols.length);
            
            for (int i = 0; i < cols.length; i++) {
                assertEquals(String.format("attr_%06d", i), cols[i].getName());
                assertEquals(i, cols[i].getValue().asLong());
            }
            
            OTSHelper.deleteTable(ots, tableName);
        }
        {
            OTSHelper.createTable(ots, tableName, scheme);
            Utils.waitForPartitionLoad(tableName);
            OTSHelper.batchWriteRowNoLimit(ots, null, updateChanges, null);
            
            Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
            assertEquals(pk, row.getPrimaryKey());
            
            Column[] cols = row.getColumns();
            assertEquals(OTSRestrictedItemConst.COLUMN_COUNT_MAX_IN_SINGLE_ROW, cols.length);
            
            for (int i = 0; i < cols.length; i++) {
                assertEquals(String.format("attr_%06d", i), cols[i].getName());
                assertEquals(i, cols[i].getValue().asLong());
            }
            OTSHelper.deleteTable(ots, tableName);
        }
        
        {
            OTSHelper.createTable(ots, tableName, scheme);
            Utils.waitForPartitionLoad(tableName);
            OTSHelper.batchWriteRowNoLimit(ots, null, updateChangesForDelete, null);
            
            Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
            assertTrue(null == row);
        }
    }
    
    /**
     * 对于所有的写操作
     * PutRow/UpdateRow（update和delete两种情况）/BatchWriteRow（updateChange/delete/update），
     * 测试包含1025个列，期望返回OTSParameterInvalid。
     */
    @Test
    public void testCase41() {
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("pk_0", PrimaryKeyType.INTEGER));
        
        OTSHelper.createTable(ots, tableName, scheme);
        Utils.waitForPartitionLoad(tableName);
        
        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(0))
                .build();
        
        List<Column> puts = new ArrayList<Column>();
        List<String> deletes = new ArrayList<String>();
        List<RowPutChange> putChanges = new ArrayList<RowPutChange>();
        List<RowUpdateChange> updateChanges = new ArrayList<RowUpdateChange>();
        List<RowUpdateChange> updateChangesForDelete = new ArrayList<RowUpdateChange>();
        
        RowPutChange put = new RowPutChange(tableName, pk);
        RowUpdateChange update = new RowUpdateChange(tableName, pk);
        RowUpdateChange updateForDelete = new RowUpdateChange(tableName, pk);
        for (int i = 0; i < OTSRestrictedItemConst.COLUMN_COUNT_MAX_IN_SINGLE_ROW + 1; i++) {
            String columnName = "attr_" + i;
            puts.add(new Column(columnName, ColumnValue.fromLong(i)));
            deletes.add(columnName);
            put.addColumn(new Column(columnName, ColumnValue.fromLong(i)));
            update.put(new Column(columnName, ColumnValue.fromLong(i)));
            updateForDelete.deleteColumns(columnName);
        }
        putChanges.add(put);
        updateChanges.add(update);
        updateChangesForDelete.add(updateForDelete);
        
        //
        {
            Utils.deleteTableIfExist(ots, tableName);
            OTSHelper.createTable(ots, tableName, scheme);
            Utils.waitForPartitionLoad(tableName);
            
            try {
                OTSHelper.putRow(ots, tableName, pk, puts);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The number of attribute columns exceeds the limit, limit count: 1024, column count: 1025.", 400, e);
            }
            OTSHelper.deleteTable(ots, tableName);
        }
        
        {
            OTSHelper.createTable(ots, tableName, scheme);
            Utils.waitForPartitionLoad(tableName);
            
            try {
                OTSHelper.updateRow(ots, tableName, pk, puts, null, null);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The number of attribute columns exceeds the limit, limit count: 1024, column count: 1025.", 400, e);
            }
        }
        
        {
            
            try {
                OTSHelper.updateRow(ots, tableName, pk, null, deletes, null);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The number of attribute columns exceeds the limit, limit count: 1024, column count: 1025.", 400, e);
            }
            
            OTSHelper.deleteTable(ots, tableName);
        }
        {
            OTSHelper.createTable(ots, tableName, scheme);
            Utils.waitForPartitionLoad(tableName);
            
            try {
                OTSHelper.batchWriteRow(ots, putChanges, null, null);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The number of attribute columns exceeds the limit, limit count: 1024, column count: 1025.", 400, e);
            }
            
            OTSHelper.deleteTable(ots, tableName);
        }
        {
            OTSHelper.createTable(ots, tableName, scheme);
            Utils.waitForPartitionLoad(tableName);
            
            try {
                OTSHelper.batchWriteRow(ots, null, updateChanges, null);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The number of attribute columns exceeds the limit, limit count: 1024, column count: 1025.", 400, e);
            }
            OTSHelper.deleteTable(ots, tableName);
        }
        
        {
            OTSHelper.createTable(ots, tableName, scheme);
            Utils.waitForPartitionLoad(tableName);
            
            try {
                OTSHelper.batchWriteRow(ots, null, updateChangesForDelete, null);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The number of attribute columns exceeds the limit, limit count: 1024, column count: 1025.", 400, e);
            }
            OTSHelper.deleteTable(ots, tableName);
        }
    }
    
    /**
     * 对于所有的读操作 GetRow/GetRange/BatchGet，测试ColumnsToGet包含1024个列，期望返回正常。
     */
    @Test
    public void testCase42() {
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("pk_0", PrimaryKeyType.INTEGER));
        
        OTSHelper.createTable(ots, tableName, scheme);
        Utils.waitForPartitionLoad(tableName);
        
        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(0))
                .build();
        
        List<Column> puts = new ArrayList<Column>();
        List<String> columnsToGet = new ArrayList<String>();
        for (int i = 0; i < OTSRestrictedItemConst.COLUMN_COUNT_MAX_IN_SINGLE_ROW; i++) {
            columnsToGet.add(String.format("attr_%06d", i));
            puts.add(new Column(String.format("attr_%06d", i), ColumnValue.fromLong(i)));
        }
        
        // prepare data
        OTSHelper.putRow(ots, tableName, pk, puts);
        
        // get row
        {
            SingleRowQueryCriteria rowQueryCriteria = new SingleRowQueryCriteria(tableName, pk);
            rowQueryCriteria.addColumnsToGet(columnsToGet);
            rowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
            
            Row row = OTSHelper.getRow(ots, rowQueryCriteria).getRow();
            assertEquals(pk, row.getPrimaryKey());
            Column[] cols = row.getColumns();
            assertEquals(OTSRestrictedItemConst.COLUMN_COUNT_MAX_IN_SINGLE_ROW, cols.length);
            
            for (int i = 0; i < cols.length; i++) {
                assertEquals(String.format("attr_%06d", i), cols[i].getName());
                assertEquals(i, cols[i].getValue().asLong());
            }
        }
        // batch get row
        {
            MultiRowQueryCriteria multiRowQueryCriteria = new MultiRowQueryCriteria(tableName);
            multiRowQueryCriteria.addRow(pk);
            multiRowQueryCriteria.addColumnsToGet(columnsToGet);
            multiRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
            List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
            criterias.add(multiRowQueryCriteria);
            BatchGetRowResponse r = OTSHelper.batchGetRow(ots, criterias);
            
            List<RowResult> rr = r.getBatchGetRowResult(tableName);
            assertEquals(1, rr.size());
            
            Row row = rr.get(0).getRow();
            
            assertEquals(pk, row.getPrimaryKey());
            Column[] cols = row.getColumns();
            assertEquals(OTSRestrictedItemConst.COLUMN_COUNT_MAX_IN_SINGLE_ROW, cols.length);
            
            for (int i = 0; i < cols.length; i++) {
                assertEquals(String.format("attr_%06d", i), cols[i].getName());
                assertEquals(i, cols[i].getValue().asLong());
            }
        }
        // Get Range
        {
            PrimaryKey begin = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MIN)
                    .build();
            PrimaryKey end = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MAX)
                    .build();
            
            RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(begin);
            rangeRowQueryCriteria.setExclusiveEndPrimaryKey(end);
            rangeRowQueryCriteria.addColumnsToGet(columnsToGet);
            rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
            GetRangeResponse r = OTSHelper.getRange(ots, rangeRowQueryCriteria);
            List<Row> rows = r.getRows();
            assertEquals(1, rows.size());
            
            Row row = rows.get(0);
            assertEquals(pk, row.getPrimaryKey());
            Column[] cols = row.getColumns();
            assertEquals(OTSRestrictedItemConst.COLUMN_COUNT_MAX_IN_SINGLE_ROW, cols.length);
            
            for (int i = 0; i < cols.length; i++) {
                assertEquals(String.format("attr_%06d", i), cols[i].getName());
                assertEquals(i, cols[i].getValue().asLong());
            }
        }
    }
    
    /**
     * 对于所有的读操作 GetRow/GetRange/BatchGet，测试ColumnsToGet包含1025个列，期望返回错误
     */
    @Test
    public void testCase43() {
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("pk_0", PrimaryKeyType.INTEGER));
        
        OTSHelper.createTable(ots, tableName, scheme);
        Utils.waitForPartitionLoad(tableName);
        
        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(0))
                .build();
        
        List<Column> puts = new ArrayList<Column>();
        List<String> columnsToGet = new ArrayList<String>();
        for (int i = 0; i < OTSRestrictedItemConst.COLUMN_COUNT_MAX_IN_SINGLE_ROW; i++) {
            columnsToGet.add(String.format("attr_%06d", i));
            puts.add(new Column(String.format("attr_%06d", i), ColumnValue.fromLong(i)));
        }
        columnsToGet.add(String.format("attr_%06d", OTSRestrictedItemConst.COLUMN_COUNT_MAX_IN_SINGLE_ROW));
        
        // prepare data
        OTSHelper.putRow(ots, tableName, pk, puts);
        
        // get row
        {
            SingleRowQueryCriteria rowQueryCriteria = new SingleRowQueryCriteria(tableName, pk);
            rowQueryCriteria.addColumnsToGet(columnsToGet);
            rowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
            
            try {
                OTSHelper.getRow(ots, rowQueryCriteria);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The number of columns from the request exceeds the limit, limit count: 1024, column count: 1025.", 400, e);
            }
            
        }
        // batch get row
        {
            MultiRowQueryCriteria multiRowQueryCriteria = new MultiRowQueryCriteria(tableName);
            multiRowQueryCriteria.addRow(pk);
            multiRowQueryCriteria.addColumnsToGet(columnsToGet);
            multiRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
            List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
            criterias.add(multiRowQueryCriteria);
            try {
                OTSHelper.batchGetRow(ots, criterias);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The number of columns from the request exceeds the limit, limit count: 1024, column count: 1025.", 400, e);
            }
        }
        // Get Range
        {
            PrimaryKey begin = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MIN)
                    .build();
            PrimaryKey end = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MAX)
                    .build();
            
            RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(begin);
            rangeRowQueryCriteria.setExclusiveEndPrimaryKey(end);
            rangeRowQueryCriteria.addColumnsToGet(columnsToGet);
            rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
            
            try {
                OTSHelper.getRange(ots, rangeRowQueryCriteria);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The number of columns from the request exceeds the limit, limit count: 1024, column count: 1025.", 400, e);
            }
        }
    }
    
    /**
     * 分别对UpdateRow和BatchWriteRow中的UpdateRow操作：一个row已经包含1024个
     * column，update添加一个column成功
     */
    @Test
    public void testCase44() {
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("pk_0", PrimaryKeyType.INTEGER));
        
        OTSHelper.createTable(ots, tableName, scheme);
        Utils.waitForPartitionLoad(tableName);
        
        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(0))
                .build();
        
        List<Column> puts = new ArrayList<Column>();
        List<String> columnsToGet = new ArrayList<String>();
        for (int i = 0; i < OTSRestrictedItemConst.COLUMN_COUNT_MAX_IN_SINGLE_ROW; i++) {
            columnsToGet.add(String.format("attr_%06d", i));
            puts.add(new Column(String.format("attr_%06d", i), ColumnValue.fromLong(i)));
        }
        
        // prepare data
        OTSHelper.putRow(ots, tableName, pk, puts);
        
        // get row
        {
            SingleRowQueryCriteria rowQueryCriteria = new SingleRowQueryCriteria(tableName, pk);
            rowQueryCriteria.addColumnsToGet(columnsToGet);
            rowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
            
            Row row = OTSHelper.getRow(ots, rowQueryCriteria).getRow();
            assertEquals(pk, row.getPrimaryKey());
            Column[] cols = row.getColumns();
            assertEquals(OTSRestrictedItemConst.COLUMN_COUNT_MAX_IN_SINGLE_ROW, cols.length);
            
            for (int i = 0; i < cols.length; i++) {
                assertEquals(String.format("attr_%06d", i), cols[i].getName());
                assertEquals(i, cols[i].getValue().asLong());
            }
        }
        // batch get row
        {
            MultiRowQueryCriteria multiRowQueryCriteria = new MultiRowQueryCriteria(tableName);
            multiRowQueryCriteria.addRow(pk);
            multiRowQueryCriteria.addColumnsToGet(columnsToGet);
            multiRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
            List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
            criterias.add(multiRowQueryCriteria);
            BatchGetRowResponse r = OTSHelper.batchGetRow(ots, criterias);
            
            List<RowResult> rr = r.getBatchGetRowResult(tableName);
            assertEquals(1, rr.size());
            
            Row row = rr.get(0).getRow();
            
            assertEquals(pk, row.getPrimaryKey());
            Column[] cols = row.getColumns();
            assertEquals(OTSRestrictedItemConst.COLUMN_COUNT_MAX_IN_SINGLE_ROW, cols.length);
            
            for (int i = 0; i < cols.length; i++) {
                assertEquals(String.format("attr_%06d", i), cols[i].getName());
                assertEquals(i, cols[i].getValue().asLong());
            }
        }
        // Get Range
        {
            PrimaryKey begin = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MIN)
                    .build();
            PrimaryKey end = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MAX)
                    .build();
            
            RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(begin);
            rangeRowQueryCriteria.setExclusiveEndPrimaryKey(end);
            rangeRowQueryCriteria.addColumnsToGet(columnsToGet);
            rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
            GetRangeResponse r = OTSHelper.getRange(ots, rangeRowQueryCriteria);
            List<Row> rows = r.getRows();
            assertEquals(1, rows.size());
            
            Row row = rows.get(0);
            assertEquals(pk, row.getPrimaryKey());
            Column[] cols = row.getColumns();
            assertEquals(OTSRestrictedItemConst.COLUMN_COUNT_MAX_IN_SINGLE_ROW, cols.length);
            
            for (int i = 0; i < cols.length; i++) {
                assertEquals(String.format("attr_%06d", i), cols[i].getName());
                assertEquals(i, cols[i].getValue().asLong());
            }
        }
    }
    
    /**
     * 测试所有的API指定timestamp为 long.max/1000 - 1的情况，期望返回正常
     */
    @Test
    public void testCase45() {
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("pk_0", PrimaryKeyType.INTEGER));
        
        OTSHelper.createTable(ots, tableName, scheme);
        Utils.waitForPartitionLoad(tableName);
        
        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(0))
                .build();
        
        List<Column> puts = new ArrayList<Column>();
        List<String> columnsToGet = new ArrayList<String>();
        for (int i = 0; i < 10; i++) {
            columnsToGet.add(String.format("attr_%06d", i));
            puts.add(new Column(String.format("attr_%06d", i), ColumnValue.fromLong(i), Long.MAX_VALUE/1000 - 1));
        }
        
        {
            OTSHelper.putRow(ots, tableName, pk, puts);
        }
        {
            Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
            assertEquals(pk, row.getPrimaryKey());
            Column[] cols = row.getColumns();
            assertEquals(10, cols.length);
            
            for (int i = 0; i < cols.length; i++) {
                assertEquals(String.format("attr_%06d", i), cols[i].getName());
                assertEquals(i, cols[i].getValue().asLong());
                assertEquals(Long.MAX_VALUE/1000 - 1, cols[i].getTimestamp());
            }
        }
        {
            OTSHelper.deleteRow(ots, tableName, pk);
            Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
            assertTrue(null == row);
        }
        {
            OTSHelper.updateRow(ots, tableName, pk, puts, null, null);
            
            Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
            assertEquals(pk, row.getPrimaryKey());
            Column[] cols = row.getColumns();
            assertEquals(10, cols.length);
            
            for (int i = 0; i < cols.length; i++) {
                assertEquals(String.format("attr_%06d", i), cols[i].getName());
                assertEquals(i, cols[i].getValue().asLong());
                assertEquals(Long.MAX_VALUE/1000 - 1, cols[i].getTimestamp());
            }
            OTSHelper.deleteRow(ots, tableName, pk);
            row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
            assertTrue(null == row);
        }
        {
            List<RowPutChange> putsChange = new ArrayList<RowPutChange>();
            RowPutChange p = new RowPutChange(tableName, pk);
            for (int i = 0; i < 10; i++) {
                p.addColumn(new Column("attr_" + i, ColumnValue.fromLong(i), Long.MAX_VALUE/1000 - 1));
            }
            putsChange.add(p);
            
            List<RowUpdateChange> updatesChange = new ArrayList<RowUpdateChange>();
            
            OTSHelper.batchWriteRowNoLimit(ots, putsChange, updatesChange, null);
            
            List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
            MultiRowQueryCriteria mrc = new MultiRowQueryCriteria(tableName);
            mrc.addRow(pk);
            mrc.setMaxVersions(Integer.MAX_VALUE);
            criterias.add(mrc);
            List<RowResult> r = OTSHelper.batchGetRow(ots, criterias).getBatchGetRowResult(tableName);
            
            assertEquals(1, r.size());
            
            Row row = r.get(0).getRow();
            assertEquals(pk, row.getPrimaryKey());
            Column[] cols = row.getColumns();
            assertEquals(10, cols.length);
            
            for (int i = 0; i < 10; i++) {
                assertEquals(i, cols[i].getValue().asLong());
                assertEquals(Long.MAX_VALUE/1000 - 1, cols[i].getTimestamp());
            }
        }
    }
    
    /**
     * 测试所有的API指定timestamp为 long.max/1000的情况，期望返回OTSParameterInvalid
     */
    @Test
    public void testCase46() {
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("pk", PrimaryKeyType.INTEGER));
        
        OTSHelper.createTable(ots, tableName, scheme);
        Utils.waitForPartitionLoad(tableName);
        
        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(0))
                .build();
        
        List<Column> puts = new ArrayList<Column>();
        List<String> columnsToGet = new ArrayList<String>();
        for (int i = 0; i < 10; i++) {
            columnsToGet.add(String.format("attr_%06d", i));
            puts.add(new Column(String.format("attr_%06d", i), ColumnValue.fromLong(i), Long.MAX_VALUE/1000));
        }
        
        {
            try {
                OTSHelper.putRow(ots, tableName, pk, puts);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "Timestamp must be in range [0, INT64_MAX/1000).", 400, e);
            }
            
        }
        {
            try {
                OTSHelper.updateRow(ots, tableName, pk, puts, null, null);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "Timestamp must be in range [0, INT64_MAX/1000).", 400, e);
            }
        }
        {
            List<RowPutChange> putsChange = new ArrayList<RowPutChange>();
            RowPutChange p = new RowPutChange(tableName, pk);
            for (int i = 0; i < 10; i++) {
                p.addColumn(new Column(String.format("attr_%06d", i), ColumnValue.fromLong(i), Long.MAX_VALUE/1000));
            }
            putsChange.add(p);
            
            List<RowUpdateChange> updatesChange = new ArrayList<RowUpdateChange>();
            try {
                BatchWriteRowResponse r = OTSHelper.batchWriteRow(ots, putsChange, updatesChange, null);
                fail();
            } catch (TableStoreException ex) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "Timestamp must be in range [0, INT64_MAX/1000).", 400, ex);
            }

        }
    }
    
    private String getString(char c, int size) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size; i++) {
            sb.append(c);
        }
        return sb.toString();
    }
    
    /**
     * 构造BatchWriteRow的put和update操作，数据量正好为1M的情况，类型分别为STRING/BINARY, 
     * 期望返回正常，并且BatchGetRow读出校验通过。
     */
    @Test
    public void testCase47_string_put() {
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("pk", PrimaryKeyType.STRING));
        
        OTSHelper.createTable(ots, tableName, scheme);
        Utils.waitForPartitionLoad(tableName);
        
        //   2    +     98     +    50    + 64 * 1024 + 8  = 65694
        // pk_name + pk_value + col_name + col_value + ts
        
        // 1024 * 1024 - 15 * 65694 = 63166
        //   2    +     98    +     50    + 63008     + 8  = 63166
        
        List<RowPutChange> puts = new ArrayList<RowPutChange>();
        
        String columName = getString('k', 50);
        
        for (int i = 0; i < 15; i++) {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(String.format("%s%02d", getString('v', 48), i)))
                    .build();
            
            RowPutChange put = new RowPutChange(tableName, pk);
            put.addColumn(columName, ColumnValue.fromString(getString('a', 64 * 1024)), (new Date()).getTime());
            puts.add(put);
        }
        
        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(String.format("%s%02d", getString('v', 48), 15)))
                .build();
        
        RowPutChange put = new RowPutChange(tableName, pk);
        put.addColumn(columName, ColumnValue.fromString(getString('a', 63008)), (new Date()).getTime());
        puts.add(put);
        
        OTSHelper.batchWriteRowNoLimit(ots, puts, null, null);
        
        //
        List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
        MultiRowQueryCriteria c = new MultiRowQueryCriteria(tableName);
        c.setMaxVersions(Integer.MAX_VALUE);
        for (int i = 0 ; i < 16; i++) {
            PrimaryKey tmpPk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(String.format("%s%02d", getString('v', 48), i)))
                    .build();
            c.addRow(tmpPk);
        }
        criterias.add(c);
        
        List<RowResult> rr = OTSHelper.batchGetRowNoLimit(ots, criterias);
        
        assertEquals(16, rr.size());
        
        for (int i = 0; i < 16; i++) {
            Row row = rr.get(i).getRow();
            
            PrimaryKey expect = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(String.format("%s%02d", getString('v', 48), i)))
                    .build();
            
            assertEquals(expect, row.getPrimaryKey());
            assertEquals(1, row.getColumns().length);
            assertEquals(columName, row.getColumns()[0].getName());
            if (i < 15) {
                assertEquals(getString('a', 64 * 1024), row.getColumns()[0].getValue().asString());
            } else {
                assertEquals(getString('a', 63008), row.getColumns()[0].getValue().asString());
            }
        }
    }
    
    @Test
    public void testCase47_string_update() {
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("pk", PrimaryKeyType.STRING));
        
        OTSHelper.createTable(ots, tableName, scheme);
        Utils.waitForPartitionLoad(tableName);
        
        //   2    +     98     +    50    + 64 * 1024 + 8  = 65694
        // pk_name + pk_value + col_name + col_value + ts
        
        // 1024 * 1024 - 15 * 65694 = 63166
        //   2    +     98    +     50    + 63008     + 8  = 63166
        
        List<RowUpdateChange> updates = new ArrayList<RowUpdateChange>();
        
        String columName = getString('k', 50);
        
        for (int i = 0; i < 15; i++) {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(String.format("%s%02d", getString('v', 48), i)))
                    .build();
            
            RowUpdateChange up = new RowUpdateChange(tableName, pk);
            up.put(columName, ColumnValue.fromString(getString('a', 64 * 1024)), (new Date()).getTime());
            updates.add(up);
        }
        
        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(String.format("%s%02d", getString('v', 48), 15)))
                .build();
        
        RowUpdateChange up = new RowUpdateChange(tableName, pk);
        up.put(columName, ColumnValue.fromString(getString('a', 63008)), (new Date()).getTime());
        updates.add(up);
        
        OTSHelper.batchWriteRowNoLimit(ots, null, updates, null);
        
        //
        List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
        MultiRowQueryCriteria c = new MultiRowQueryCriteria(tableName);
        c.setMaxVersions(Integer.MAX_VALUE);
        for (int i = 0 ; i < 16; i++) {
            PrimaryKey tmpPk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(String.format("%s%02d", getString('v', 48), i)))
                    .build();
            c.addRow(tmpPk);
        }
        criterias.add(c);
        
        List<RowResult> rr = OTSHelper.batchGetRowNoLimit(ots, criterias);
        
        assertEquals(16, rr.size());
        
        for (int i = 0; i < 16; i++) {
            Row row = rr.get(i).getRow();
            
            PrimaryKey expect = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(String.format("%s%02d", getString('v', 48), i)))
                    .build();
            
            assertEquals(expect, row.getPrimaryKey());
            assertEquals(1, row.getColumns().length);
            assertEquals(columName, row.getColumns()[0].getName());
            if (i < 15) {
                assertEquals(getString('a', 64 * 1024), row.getColumns()[0].getValue().asString());
            } else {
                assertEquals(getString('a', 63008), row.getColumns()[0].getValue().asString());
            }
        }
    }
    
    @Test
    public void testCase47_binary_put() throws UnsupportedEncodingException {
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("pk", PrimaryKeyType.STRING));
        
        OTSHelper.createTable(ots, tableName, scheme);
        Utils.waitForPartitionLoad(tableName);
        
        //   2    +     98     +    50    + 64 * 1024 + 8  = 65694
        // pk_name + pk_value + col_name + col_value + ts
        
        // 1024 * 1024 - 15 * 65694 = 63166
        //   2    +     98    +     50    + 63008     + 8  = 63166
        
        List<RowPutChange> puts = new ArrayList<RowPutChange>();
        
        String columName = getString('k', 50);
        
        for (int i = 0; i < 15; i++) {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(String.format("%s%02d", getString('v', 48), i)))
                    .build();
            
            RowPutChange put = new RowPutChange(tableName, pk);
            put.addColumn(columName, ColumnValue.fromBinary(getString('a', 64 * 1024).getBytes("UTF-8")), (new Date()).getTime());
            puts.add(put);
        }
        
        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(String.format("%s%02d", getString('v', 48), 15)))
                .build();
        
        RowPutChange put = new RowPutChange(tableName, pk);
        put.addColumn(columName, ColumnValue.fromBinary(getString('a', 63008).getBytes("UTF-8")), (new Date()).getTime());
        puts.add(put);
        
        OTSHelper.batchWriteRowNoLimit(ots, puts, null, null);
        
        //
        List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
        MultiRowQueryCriteria c = new MultiRowQueryCriteria(tableName);
        c.setMaxVersions(Integer.MAX_VALUE);
        for (int i = 0 ; i < 16; i++) {
            PrimaryKey tmpPk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(String.format("%s%02d", getString('v', 48), i)))
                    .build();
            c.addRow(tmpPk);
        }
        criterias.add(c);
        
        List<RowResult> rr = OTSHelper.batchGetRowNoLimit(ots, criterias);
        
        assertEquals(16, rr.size());
        
        for (int i = 0; i < 16; i++) {
            Row row = rr.get(i).getRow();
            
            PrimaryKey expect = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(String.format("%s%02d", getString('v', 48), i)))
                    .build();
            
            assertEquals(expect, row.getPrimaryKey());
            assertEquals(1, row.getColumns().length);
            if (i < 15) {
                assertEquals(getString('a', 64 * 1024), new String(row.getColumns()[0].getValue().asBinary(), "UTF-8"));
            } else {
                assertEquals(getString('a', 63008), new String(row.getColumns()[0].getValue().asBinary(), "UTF-8"));
            }
        }
    }
    
    @Test
    public void testCase47_binary_update() throws UnsupportedEncodingException {
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("pk", PrimaryKeyType.STRING));
        
        OTSHelper.createTable(ots, tableName, scheme);
        Utils.waitForPartitionLoad(tableName);
        
        //   2    +     98     +    50    + 64 * 1024 + 8  = 65694
        // pk_name + pk_value + col_name + col_value + ts
        
        // 1024 * 1024 - 15 * 65694 = 63166
        //   2    +     98    +     50    + 63008     + 8  = 63166
        
        List<RowUpdateChange> updates = new ArrayList<RowUpdateChange>();
        
        String columName = getString('k', 50);
        
        for (int i = 0; i < 15; i++) {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(String.format("%s%02d", getString('v', 48), i)))
                    .build();
            
            RowUpdateChange up = new RowUpdateChange(tableName, pk);
            up.put(columName, ColumnValue.fromBinary(getString('a', 64 * 1024).getBytes("UTF-8")), (new Date()).getTime());
            updates.add(up);
        }
        
        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(String.format("%s%02d", getString('v', 48), 15)))
                .build();
        
        RowUpdateChange up = new RowUpdateChange(tableName, pk);
        up.put(columName, ColumnValue.fromBinary(getString('a', 63008).getBytes("UTF-8")), (new Date()).getTime());
        updates.add(up);
        
        OTSHelper.batchWriteRowNoLimit(ots, null, updates, null);
        
        //
        List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
        MultiRowQueryCriteria c = new MultiRowQueryCriteria(tableName);
        c.setMaxVersions(Integer.MAX_VALUE);
        for (int i = 0 ; i < 16; i++) {
            PrimaryKey tmpPk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(String.format("%s%02d", getString('v', 48), i)))
                    .build();
            c.addRow(tmpPk);
        }
        criterias.add(c);
        List<RowResult> rr = OTSHelper.batchGetRowNoLimit(ots, criterias);
        
        assertEquals(16, rr.size());
        
        for (int i = 0; i < 16; i++) {
            Row row = rr.get(i).getRow();
            
            PrimaryKey expect = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(String.format("%s%02d", getString('v', 48), i)))
                    .build();
            
            assertEquals(expect, row.getPrimaryKey());
            assertEquals(1, row.getColumns().length);
            if (i < 15) {
                assertEquals(getString('a', 64 * 1024), new String(row.getColumns()[0].getValue().asBinary(), "UTF-8"));
            } else {
                assertEquals(getString('a', 63008), new String(row.getColumns()[0].getValue().asBinary(), "UTF-8"));
            }
            
        }
    }
    
    /**
     * 构造BatchWriteRow的put和update操作，数据量正好为4M + 1的情况，类型分别为STRING/BINARY, 
     * 期望返回OTSParameterInvalid
     */
    @Test
    public void testCase48_string_put() {
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("pk", PrimaryKeyType.STRING));
        
        OTSHelper.createTable(ots, tableName, scheme);
        Utils.waitForPartitionLoad(tableName);
        
         // 4,194,305
        // 1,398,103
        // 1,398,101
        // 1,398,101

        //   2    +     98     +    50    + 1397945 + 8  = 1,398,103
        // pk_name + pk_value + col_name + col_value + ts

        //   2    +     98     +    50    + 1397943 + 8  = 1,398,101
        // pk_name + pk_value + col_name + col_value + ts

        //   2    +     98     +    50    + 1397943 + 8  = 1,398,101
        // pk_name + pk_value + col_name + col_value + ts 
       
        
        List<RowPutChange> puts = new ArrayList<RowPutChange>();
        
        String columName = getString('k', 50);
        
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(String.format("%s%02d", getString('v', 96), 0)))
                    .build();
            
            RowPutChange put = new RowPutChange(tableName, pk);
            put.addColumn(columName, ColumnValue.fromString(getString('a', 1397945)), (new Date()).getTime());
            puts.add(put);
        }
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(String.format("%s%02d", getString('v', 96), 1)))
                    .build();
            
            RowPutChange put = new RowPutChange(tableName, pk);
            put.addColumn(columName, ColumnValue.fromString(getString('a', 1397943)), (new Date()).getTime());
            puts.add(put);
        }
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(String.format("%s%02d", getString('v', 96), 2)))
                    .build();
            
            RowPutChange put = new RowPutChange(tableName, pk);
            put.addColumn(columName, ColumnValue.fromString(getString('a', 1397943)), (new Date()).getTime());
            puts.add(put);
        }
       
        try {
            OTSHelper.batchWriteRow(ots, puts, null, null);
            fail();
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The total data size of BatchWriteRow request exceeds the limit, limit size: 4194304, data size: 4194305.", 400, e);
        }
    }
    
    @Test
    public void testCase48_string_update() {
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("pk", PrimaryKeyType.STRING));
        
        OTSHelper.createTable(ots, tableName, scheme);
        Utils.waitForPartitionLoad(tableName);
        
        // 4,194,305
        // 1,398,103
        // 1,398,101
        // 1,398,101

        //   2    +     98     +    50    + 1397945 + 8  = 1,398,103
        // pk_name + pk_value + col_name + col_value + ts

        //   2    +     98     +    50    + 1397943 + 8  = 1,398,101
        // pk_name + pk_value + col_name + col_value + ts

        //   2    +     98     +    50    + 1397943 + 8  = 1,398,101
        // pk_name + pk_value + col_name + col_value + ts
 
        List<RowUpdateChange> updates = new ArrayList<RowUpdateChange>();
        
        String columName = getString('k', 50);
        
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(String.format("%s%02d", getString('v', 96), 0)))
                    .build();
            
            RowUpdateChange up = new RowUpdateChange(tableName, pk);
            up.put(columName, ColumnValue.fromString(getString('a', 1397945)), (new Date()).getTime());
            updates.add(up);
        }
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(String.format("%s%02d", getString('v', 96), 1)))
                    .build();
            
            RowUpdateChange up = new RowUpdateChange(tableName, pk);
            up.put(columName, ColumnValue.fromString(getString('a', 1397943)), (new Date()).getTime());
            updates.add(up);
        }
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(String.format("%s%02d", getString('v', 96), 2)))
                    .build();
            
            RowUpdateChange up = new RowUpdateChange(tableName, pk);
            up.put(columName, ColumnValue.fromString(getString('a', 1397943)), (new Date()).getTime());
            updates.add(up);
        }
       
        try {
            OTSHelper.batchWriteRow(ots, null, updates, null);
            fail();
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The total data size of BatchWriteRow request exceeds the limit, limit size: 4194304, data size: 4194305.", 400, e);
        }
    }
    
    @Test
    public void testCase48_binary_put() throws UnsupportedEncodingException {
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("pk", PrimaryKeyType.STRING));
        
        OTSHelper.createTable(ots, tableName, scheme);
        Utils.waitForPartitionLoad(tableName);

        // 4,194,305
        // 1,398,103
        // 1,398,101
        // 1,398,101

        //   2    +     98     +    50    + 1397945 + 8  = 1,398,103
        // pk_name + pk_value + col_name + col_value + ts

        //   2    +     98     +    50    + 1397943 + 8  = 1,398,101
        // pk_name + pk_value + col_name + col_value + ts

        //   2    +     98     +    50    + 1397943 + 8  = 1,398,101
        // pk_name + pk_value + col_name + col_value + ts 
        
        List<RowPutChange> puts = new ArrayList<RowPutChange>();
        
        String columName = getString('k', 50);
        
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(String.format("%s%02d", getString('v', 96), 0)))
                    .build();
            
            RowPutChange put = new RowPutChange(tableName, pk);
            put.addColumn(columName, ColumnValue.fromBinary(getString('a', 1397945).getBytes("UTF-8")), (new Date()).getTime());
            puts.add(put);
        }
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(String.format("%s%02d", getString('v', 96), 1)))
                    .build();
            
            RowPutChange put = new RowPutChange(tableName, pk);
            put.addColumn(columName, ColumnValue.fromBinary(getString('a', 1397943).getBytes("UTF-8")), (new Date()).getTime());
            puts.add(put);
        }
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(String.format("%s%02d", getString('v', 96), 2)))
                    .build();
            
            RowPutChange put = new RowPutChange(tableName, pk);
            put.addColumn(columName, ColumnValue.fromBinary(getString('a', 1397943).getBytes("UTF-8")), (new Date()).getTime());
            puts.add(put);
        }
       
        try {
            OTSHelper.batchWriteRow(ots, puts, null, null);
            fail();
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The total data size of BatchWriteRow request exceeds the limit, limit size: 4194304, data size: 4194305.", 400, e);
        }
    }
    
    @Test
    public void testCase48_binary_update() throws UnsupportedEncodingException {
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("pk", PrimaryKeyType.STRING));
        
        OTSHelper.createTable(ots, tableName, scheme);
        Utils.waitForPartitionLoad(tableName);
        
        // 4,194,305
        // 1,398,103
        // 1,398,101
        // 1,398,101

        //   2    +     98     +    50    + 1397945 + 8  = 1,398,103
        // pk_name + pk_value + col_name + col_value + ts

        //   2    +     98     +    50    + 1397943 + 8  = 1,398,101
        // pk_name + pk_value + col_name + col_value + ts

        //   2    +     98     +    50    + 1397943 + 8  = 1,398,101
        // pk_name + pk_value + col_name + col_value + ts        

        List<RowUpdateChange> updates = new ArrayList<RowUpdateChange>();
        
        String columName = getString('k', 50);
        
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(String.format("%s%02d", getString('v', 96), 0)))
                    .build();
            
            RowUpdateChange up = new RowUpdateChange(tableName, pk);
            up.put(columName, ColumnValue.fromBinary(getString('a', 1397945).getBytes("UTF-8")), (new Date()).getTime());
            updates.add(up);
        }
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(String.format("%s%02d", getString('v', 96), 1)))
                    .build();
            
            RowUpdateChange up = new RowUpdateChange(tableName, pk);
            up.put(columName, ColumnValue.fromBinary(getString('a', 1397943).getBytes("UTF-8")), (new Date()).getTime());
            updates.add(up);
        }
        {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(String.format("%s%02d", getString('v', 96), 2)))
                    .build();
            
            RowUpdateChange up = new RowUpdateChange(tableName, pk);
            up.put(columName, ColumnValue.fromBinary(getString('a', 1397943).getBytes("UTF-8")), (new Date()).getTime());
            updates.add(up);
        }
     
        try {
            OTSHelper.batchWriteRow(ots, null, updates, null);
            fail();
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The total data size of BatchWriteRow request exceeds the limit, limit size: 4194304, data size: 4194305.", 400, e);
        }
    }
}
