package com.alicloud.openservices.tablestore.functiontest;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
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
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.common.OTSHelper;
import com.alicloud.openservices.tablestore.common.Utils;
import com.google.gson.JsonSyntaxException;

public class APITest {
    
    private static String tableName = "APIFunctiontest";
    private static SyncClientInterface ots;
    private static final Logger LOG = LoggerFactory.getLogger(APITest.class);
    
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
     * 建表MaxVersion=3, UpdateRow重复100次，每次写入128个不同的列，每列包含1个版本。
     * GetRow每次指定不同的列名，每次读取128个列，数据校验通过。这个CASE要针对 
     * BINARY/INTEGER/STRING/BOOLEAN/DOUBLE类型的列值都做一遍。
     * @throws UnsupportedEncodingException
     */
    @Test
    public void testCase1() throws UnsupportedEncodingException {
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("pk", PrimaryKeyType.STRING));
        
        // 建表MaxVersion=3
        OTSHelper.createTable(ots, tableName, scheme, Integer.MAX_VALUE, 3);
        
        Utils.waitForPartitionLoad(tableName);
        
        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("a"))
                .build();
        
        List<Column> puts = new ArrayList<Column>();
        
        long ts = (new Date()).getTime();
        
        LOG.info("Begin time stamp : " + ts);
        
        int columnIndex = 0;
        for (int p = 0; p < 100; p++) { // 100次
            puts.clear();
            // 每次写入128个不同的列
            for (int i = 0; i < 128; i++) {
                String columnName = String.format("attr_%06d", columnIndex++);
                if (i == 0 ) { // integer
                    for (int k = 0; k < 1; k++) {
                        Column c = new Column(columnName, ColumnValue.fromLong(i), ts + k);
                        LOG.info(c.toString());
                        puts.add(c);
                    }
                } else if (i == 1) { // double
                    for (int k = 0; k < 1; k++) {
                        Column c = new Column(columnName, ColumnValue.fromDouble(i), ts + k);
                        LOG.info(c.toString());
                        puts.add(c);
                    }
                } else if (i == 2) { // boolean
                    for (int k = 0; k < 1; k++) {
                        Column c = new Column(columnName, ColumnValue.fromBoolean(true), ts + k);
                        LOG.info(c.toString());
                        puts.add(c);
                    }
                } else if (i == 3) { // binary
                    for (int k = 0; k < 1; k++) {
                        Column c = new Column(columnName, ColumnValue.fromBinary("a".getBytes("UTF-8")), ts + k);
                        LOG.info(c.toString());
                        puts.add(c);
                    }
                } else { // string
                    for (int k = 0; k < 1; k++) {
                        Column c = new Column(columnName, ColumnValue.fromString("" + i), ts + k);
                        LOG.info(c.toString());
                        puts.add(c);
                    }
                } 
            }
            //
            OTSHelper.updateRow(ots, tableName, pk, puts, null, null);
        }
        columnIndex = 0;
        for (int p = 0; p < 100; p++) {
            List<String> columnToGet = new ArrayList<String>();
            for (int i = 0; i < 128; i++) {
                String columnName = String.format("attr_%06d", (columnIndex + i));
                columnToGet.add(columnName);
            }
            
            Row row = OTSHelper.getRow(ots, tableName, pk, null, Integer.MAX_VALUE, columnToGet).getRow();
            
            assertTrue(null != row);
            assertEquals(pk, row.getPrimaryKey());
            Column[] cols = row.getColumns();
            assertEquals(128, cols.length);
            for (int i = 0; i < 128; i++) {
                String columnName = String.format("attr_%06d", (columnIndex++));
                Column col = cols[i];
                assertEquals(columnName, col.getName());
                if (i == 0 ) { // integer
                    assertEquals(i, col.getValue().asLong());
                } else if (i == 1) { // double
                    assertTrue(i == col.getValue().asDouble());
                } else if (i == 2) { // boolean
                    assertEquals(true, col.getValue().asBoolean());
                } else if (i == 3) { // binary
                    assertEquals("a", new String(col.getValue().asBinary(), "UTF-8"));
                } else { // string
                    assertEquals("" + i, col.getValue().asString());
                } 
            }
        }
    }
    
    /**
     * 建表MaxVersion=100000, UpdateRow重复100次，每次写入1列，每列包含128个新的版本。
     * GetRow每次指定不同的版本，每次读取128个版本，数据校验通过。这个CASE要针对 
     * BINARY/INTEGER/STRING/BOOLEAN/DOUBLE类型的列值都做一遍
     * @throws UnsupportedEncodingException
     */
    @Test
    public void testCase2() throws UnsupportedEncodingException {
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("pk", PrimaryKeyType.STRING));
        
        // 建表MaxVersion=100000
        OTSHelper.createTable(ots, tableName, scheme, Integer.MAX_VALUE, 100000);
        
        Utils.waitForPartitionLoad(tableName);
        
        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("a"))
                .build();
        
        List<Column> puts = new ArrayList<Column>();
        
        long ts = (new Date()).getTime();
        
        LOG.info("Begin time stamp : " + ts);
        
        int columnIndex = 0;
        for (int p = 0; p < 100; p++) { // 100次
            puts.clear();
            // 每次写入1个不同的列
            for (int i = 0; i < 1; i++) {
                String columnName = String.format("attr_%06d", columnIndex++);
                if (columnIndex%5 == 0 ) { // integer
                    for (int k = 0; k < 128; k++) {
                        Column c = new Column(columnName, ColumnValue.fromLong(k), ts + k);
                        LOG.info(c.toString());
                        puts.add(c);
                    }
                } else if (columnIndex%5 == 1) { // double
                    for (int k = 0; k < 128; k++) {
                        Column c = new Column(columnName, ColumnValue.fromDouble(k), ts + k);
                        LOG.info(c.toString());
                        puts.add(c);
                    }
                } else if (columnIndex%5 == 2) { // boolean
                    for (int k = 0; k < 128; k++) {
                        Column c = new Column(columnName, ColumnValue.fromBoolean(true), ts + k);
                        LOG.info(c.toString());
                        puts.add(c);
                    }
                } else if (columnIndex%5 == 3) { // binary
                    for (int k = 0; k < 128; k++) {
                        Column c = new Column(columnName, ColumnValue.fromBinary("a".getBytes("UTF-8")), ts + k);
                        LOG.info(c.toString());
                        puts.add(c);
                    }
                } else { // string
                    for (int k = 0; k < 128; k++) {
                        Column c = new Column(columnName, ColumnValue.fromString("" + k), ts + k);
                        LOG.info(c.toString());
                        puts.add(c);
                    }
                } 
            }
            //
            OTSHelper.updateRow(ots, tableName, pk, puts, null, null);
        }
        columnIndex = 0;
        for (int p = 0; p < 100; p++) {
            List<String> columnToGet = new ArrayList<String>();
            String columnName = String.format("attr_%06d", columnIndex++);
            columnToGet.add(columnName);
            
            Row row = OTSHelper.getRow(ots, tableName, pk, null, Integer.MAX_VALUE, columnToGet).getRow();
            
            assertTrue(null != row);
            assertEquals(pk, row.getPrimaryKey());
            Column[] cols = row.getColumns();
            assertEquals(128, cols.length);
            for (int i = 0; i < 128; i++) {
                Column col = cols[i];
                assertEquals(columnName, col.getName());
                if (columnIndex%5 == 0 ) { // integer
                    assertEquals(127 - i, col.getValue().asLong());
                } else if (columnIndex%5 == 1) { // double
                    assertTrue((127 - i) == col.getValue().asDouble());
                } else if (columnIndex%5 == 2) { // boolean
                    assertEquals(true, col.getValue().asBoolean());
                } else if (columnIndex%5 == 3) { // binary
                    assertEquals("a", new String(col.getValue().asBinary(), "UTF-8"));
                } else { // string
                    assertEquals("" + (127 - i), col.getValue().asString());
                }
            }
        }
    }

    private String getString(char c, int size) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0 ; i < size; i++) {
            sb.append(c);
        }
        return sb.toString();
    }
    
    /**
     * MaxVersion=100, PutRow包含16个列，列名长度为255，每列包含8个cell，timestamp相同，
     * GetRow读出校验，期望每个cell返回最后一次数据。然后再UpdateRow，包含同样的行和列和timestamp，
     * 再次GetRow读出校验。
     */
    @Test
    public void testCase5() {
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("pk", PrimaryKeyType.INTEGER));
        OTSHelper.createTable(ots, tableName, scheme, Integer.MAX_VALUE, 100);
        Utils.waitForPartitionLoad(tableName);
        
        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(0))
                .build();
        
        long ts = (new Date()).getTime();
        
        List<Column> columns = new ArrayList<Column>();
        for (int i = 0; i < 16; i++) {
            for (int j = 0; j < 8; j++) {
                columns.add(new Column(String.format("%s%02d", getString('p', 253), i), ColumnValue.fromLong(j), ts));
            }
        }
        
        OTSHelper.putRow(ots, tableName, pk, columns);
        
        {
            Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
            Column[] cols = row.getColumns();
            assertEquals(16, cols.length);
            
            for (int i = 0; i < 16; i++) {
                assertEquals(String.format("%s%02d", getString('p', 253), i), cols[i].getName());
                assertEquals(7, cols[i].getValue().asLong());
                assertEquals(ts, cols[i].getTimestamp());
            }
        }
        
        // 然后再UpdateRow，包含同样的行和列和timestamp，再次GetRow读出校验。
        OTSHelper.updateRow(ots, tableName, pk, columns, null, null);
        {
            Row row = OTSHelper.getRowForAll(ots, tableName, pk).getRow();
            Column[] cols = row.getColumns();
            assertEquals(16, cols.length);
            
            for (int i = 0; i < 16; i++) {
                assertEquals(String.format("%s%02d", getString('p', 253), i), cols[i].getName());
                assertEquals(7, cols[i].getValue().asLong());
                assertEquals(ts, cols[i].getTimestamp());
            }
        }
    }
    
    /**
     * MaxVersion=100，BatchWriteRow包含10行，每行包含16个列，列名长度为255，每列包含8个
     * cell，timestamp相同，先put后update再delete，每次都BatchGetRow读取校验。
     */
    
    @Test
    public void testCase6() {
        List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
        scheme.add(new PrimaryKeySchema("pk", PrimaryKeyType.INTEGER));
        OTSHelper.createTable(ots, tableName, scheme, Integer.MAX_VALUE, 100);
        Utils.waitForPartitionLoad(tableName);
        
        List<RowPutChange> puts = new ArrayList<RowPutChange>();
        List<RowUpdateChange> updates = new ArrayList<RowUpdateChange>();
        List<RowDeleteChange> deletes = new ArrayList<RowDeleteChange>();

        MultiRowQueryCriteria multiRowQueryCriteria = new MultiRowQueryCriteria(tableName);

        long ts = (new Date()).getTime();
        
        // BatchWriteRow包含10行，每行包含16个列，列名长度为255，每列包含8个cell，timestamp相同
        {
            for (int k = 0; k < 10; k++) {
                List<Column> columns = new ArrayList<Column>();
                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(k))
                        .build();

                multiRowQueryCriteria.addRow(pk);

                RowPutChange put = new RowPutChange(tableName, pk);
                
                for (int i = 0; i < 16; i++) {
                    for (int j = 0; j < 8; j++) {
                        columns.add(new Column(String.format("%s%02d", getString('p', 253), i), ColumnValue.fromLong(j), ts));
                    }
                }
                
                put.addColumns(columns);
                
                //
                puts.add(put);
            }
            OTSHelper.batchWriteRowNoLimit(ots, puts, null, null);
        }

        multiRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
        List<MultiRowQueryCriteria> multiRowQueryCriterias = new ArrayList<MultiRowQueryCriteria>();
        multiRowQueryCriterias.add(multiRowQueryCriteria);
        BatchGetRowResponse batchGetRowResult = OTSHelper.batchGetRow(ots, multiRowQueryCriterias);
        assertEquals(10, batchGetRowResult.getSucceedRows().size());
        assertEquals(0, batchGetRowResult.getFailedRows().size());
        for (int i = 0; i < 10; i++) {
            BatchGetRowResponse.RowResult rowResult = batchGetRowResult.getSucceedRows().get(i);
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(rowResult.getIndex()))
                    .build();
            assertEquals(pk, rowResult.getRow().getPrimaryKey());
            Column[] cols = rowResult.getRow().getColumns();
            assertEquals(16, cols.length);
            for (int j = 0; j < 16; j++) {
                assertEquals(String.format("%s%02d", getString('p', 253), j), cols[j].getName());
                assertEquals(7, cols[j].getValue().asLong());
                assertEquals(ts, cols[j].getTimestamp());
            }
        }

        {
            for (int k = 0; k < 10; k++) {
                List<Column> columns = new ArrayList<Column>();
                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(k))
                        .build();
                
                RowUpdateChange update = new RowUpdateChange(tableName, pk);
                
                for (int i = 0; i < 16; i++) {
                    for (int j = 0; j < 8; j++) {
                        columns.add(new Column(String.format("%s%02d", getString('p', 253), i), ColumnValue.fromLong(-1), ts));
                    }
                }
                
                update.put(columns);
                
                //
                updates.add(update);
            }
            OTSHelper.batchWriteRowNoLimit(ots, null, updates, null);
        }

        batchGetRowResult = OTSHelper.batchGetRow(ots, multiRowQueryCriterias);
        assertEquals(10, batchGetRowResult.getSucceedRows().size());
        assertEquals(0, batchGetRowResult.getFailedRows().size());
        for (int i = 0; i < 10; i++) {
            BatchGetRowResponse.RowResult rowResult = batchGetRowResult.getSucceedRows().get(i);
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(rowResult.getIndex()))
                    .build();
            assertEquals(pk, rowResult.getRow().getPrimaryKey());
            Column[] cols = rowResult.getRow().getColumns();
            assertEquals(16, cols.length);
            for (int j = 0; j < 16; j++) {
                assertEquals(String.format("%s%02d", getString('p', 253), j), cols[j].getName());
                assertEquals(-1, cols[j].getValue().asLong());
                assertEquals(ts, cols[j].getTimestamp());
            }
        }

        {
            for (int k = 0; k < 10; k++) {
                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(k))
                        .build();
                
                RowDeleteChange delete = new RowDeleteChange(tableName, pk);
                //
                deletes.add(delete);
            }
            OTSHelper.batchWriteRowNoLimit(ots, null, null, deletes);
        }

        batchGetRowResult = OTSHelper.batchGetRow(ots, multiRowQueryCriterias);
        assertEquals(10, batchGetRowResult.getSucceedRows().size());
        assertEquals(0, batchGetRowResult.getFailedRows().size());
        for (int i = 0; i < 10; i++) {
            BatchGetRowResponse.RowResult rowResult = batchGetRowResult.getSucceedRows().get(i);
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(rowResult.getIndex()))
                    .build();
            assertEquals(null, rowResult.getRow());
        }

        PrimaryKey begin = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.INF_MIN)
                .build();
        
        PrimaryKey end = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk", PrimaryKeyValue.INF_MAX)
                .build();
        
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(begin);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(end);
        rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
        List<Row> rows = OTSHelper.getRange(ots, rangeRowQueryCriteria).getRows();
        assertEquals(0, rows.size());
    }

    @Test
    public void testStreamWithLongTableName() throws Exception {
        int startSize = 230;
        int maxSize = 256;
        int maxSizeForStream = 238;
        for (int longTableNameSize = startSize; longTableNameSize < maxSize; longTableNameSize += 1) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < longTableNameSize; i++) {
                sb.append("a");
            }
            String longTableName = sb.toString();
            List<PrimaryKeySchema> scheme = new ArrayList<PrimaryKeySchema>();
            scheme.add(new PrimaryKeySchema("pk", PrimaryKeyType.INTEGER));
            OTSHelper.createTable(ots, longTableName, scheme, -1, 1);
            UpdateTableRequest updateTableRequest = new UpdateTableRequest(longTableName);
            updateTableRequest.setStreamSpecification(new StreamSpecification(true, 168));
            ots.updateTable(updateTableRequest);
        }
        Utils.waitForPartitionLoad("");
        for (int longTableNameSize = startSize; longTableNameSize < maxSize; longTableNameSize += 1) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < longTableNameSize; i++) {
                sb.append("a");
            }
            String longTableName = sb.toString();
            for (int i = 0; i < 10; i++) {
                OTSHelper.putRow(ots, longTableName, PrimaryKeyBuilder.createPrimaryKeyBuilder().addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(i)).build(),
                        Arrays.asList(new Column("col", ColumnValue.fromLong(i))));
            }
            ListStreamRequest listStreamRequest = new ListStreamRequest(longTableName);
            String streamId = ots.listStream(listStreamRequest).getStreams().get(0).getStreamId();
            DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest(streamId);
            String shardId = ots.describeStream(describeStreamRequest).getShards().get(0).getShardId();
            GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest(streamId, shardId);
            String shardIterator = ots.getShardIterator(getShardIteratorRequest).getShardIterator();
            GetStreamRecordRequest getStreamRecordRequest = new GetStreamRecordRequest(shardIterator);
            if (longTableNameSize > maxSizeForStream) {
                try {
                    GetStreamRecordResponse getStreamRecordResponse = ots.getStreamRecord(getStreamRecordRequest);
                    fail();
                } catch (TableStoreException ex) {
                    assertEquals("OTSParameterInvalid", ex.getErrorCode());
                    assertTrue(ex.getMessage().startsWith("Invalid stream id"));
                }
            } else {
                GetStreamRecordResponse getStreamRecordResponse = ots.getStreamRecord(getStreamRecordRequest);
                assertEquals(10, getStreamRecordResponse.getRecords().size());
            }
        }
        OTSHelper.deleteAllTable(ots);
    }
}
