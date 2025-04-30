package com.alicloud.openservices.tablestore.functiontest;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.core.ErrorCode;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.filter.Filter;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;
import com.alicloud.openservices.tablestore.model.tunnel.BulkExportQueryCriteria;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.xml.crypto.Data;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class TestBulkExportFunciton {

    static String tableName = "YSTestBulkRead";
    static SyncClient internalClient = null;
    static SyncClient publicClient = null;

    @BeforeClass
    public static void beforClass() {
        ServiceSettings settings = ServiceSettings.load();
        final String endPoint = settings.getOTSEndpoint();
        final String accessId = settings.getOTSAccessKeyId();
        final String accessKey = settings.getOTSAccessKeySecret();
        final String publicInstanceName = settings.getOTSInstanceName();
        final String internalInstanceName =  settings.getOTSInstanceNameInternal();


        internalClient = new SyncClient(endPoint, accessId, accessKey, internalInstanceName);
        publicClient = new SyncClient(endPoint, accessId, accessKey, publicInstanceName);
    }

    @AfterClass
    public static void afterClass() {
        internalClient.shutdown();
        publicClient.shutdown();
    }

    @Before
    public void setUp() throws Exception {
        try {
            deleteTable(internalClient);
        } catch (TableStoreException e) {
            if (!e.getErrorCode().equals(ErrorCode.OBJECT_NOT_EXIST)) {
                throw e;
            }
        }
        try {
            deleteTable(publicClient);
        } catch (TableStoreException e) {
            if (!e.getErrorCode().equals(ErrorCode.OBJECT_NOT_EXIST)) {
                throw e;
            }
        }

        creatTable(internalClient);
        creatTable(publicClient);

        Thread.sleep(TimeUnit.SECONDS.toMillis(5));
    }

    private void deleteTable(SyncClient client) {
        DeleteTableRequest request = new DeleteTableRequest(tableName);
        client.deleteTable(request);
    }

    private void creatTable(SyncClient client) {
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("PK1", PrimaryKeyType.STRING));
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("PK2", PrimaryKeyType.INTEGER));
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("PK3", PrimaryKeyType.BINARY));
        tableMeta.addDefinedColumn(new DefinedColumnSchema("DC1", DefinedColumnType.STRING));
        tableMeta.addDefinedColumn(new DefinedColumnSchema("DC2", DefinedColumnType.INTEGER));
        tableMeta.addDefinedColumn(new DefinedColumnSchema("DC3", DefinedColumnType.BINARY));

        int timeToLive = -1;
        int maxVersions = 1;

        TableOptions tableOptions = new TableOptions(timeToLive, maxVersions);

        CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);

        client.createTable(request);
    }

    private List<RowPutChange> creatData(Integer start, Integer end) throws UnsupportedEncodingException {
        List<RowPutChange> rowPutChanges = new ArrayList<RowPutChange>();
        for (Integer i = start; i < end; i++) {
            PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            primaryKeyBuilder.addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString(i.toString()));
            primaryKeyBuilder.addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(i));
            primaryKeyBuilder.addPrimaryKeyColumn("PK3", PrimaryKeyValue.fromBinary(i.toString().getBytes("UTF-8")));
            RowPutChange rowPutChange = new RowPutChange(tableName, primaryKeyBuilder.build());
            rowPutChange.addColumn("DC1", ColumnValue.fromString(i.toString()));
            rowPutChange.addColumn("DC2", ColumnValue.fromLong(i));
            rowPutChange.addColumn("DC3", ColumnValue.fromBinary(i.toString().getBytes("UTF-8")));
            rowPutChanges.add(rowPutChange);
        }
        return rowPutChanges;
    }

    private void putData(SyncClient client, List<RowPutChange> rowPutChanges) {
        int from = 0;
        while (from < rowPutChanges.size()) {
            int to = from + 200 < rowPutChanges.size() ? from + 200 : rowPutChanges.size();
            List<RowPutChange>  sub = rowPutChanges.subList(from, to);
            BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();
            for (RowPutChange rowPutChange : sub) {
                batchWriteRowRequest.addRowChange(rowPutChange);
            }
            client.batchWriteRow(batchWriteRowRequest);
            from += 200;
        }
    }

    private static PrimaryKey getPK(int value) throws UnsupportedEncodingException {
        PrimaryKeyBuilder pkBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        pkBuilder.addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString(new Integer(value).toString()));
        pkBuilder.addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(value));
        pkBuilder.addPrimaryKeyColumn("PK3", PrimaryKeyValue.fromBinary(new Integer(value).toString().getBytes("UTF-8")));
        PrimaryKey pk = pkBuilder.build();
        return pk;
    }

    @Test
    public void testBase() throws Exception {
        List<RowPutChange> rowPutChanges = creatData(100, 300);
        putData(internalClient, rowPutChanges);
        putData(publicClient, rowPutChanges);

        // public matrix
        {
            PrimaryKey lowerPK = getPK(150);
            PrimaryKey upperPK = getPK(200);

            BulkExportRequest bulkExportRequest = new BulkExportRequest();
            BulkExportQueryCriteria bulkExportQueryCriteria = new BulkExportQueryCriteria(tableName);
            String[] columns = new String[]{"PK1", "PK3", "DC1", "DC2"};

            bulkExportQueryCriteria.setInclusiveStartPrimaryKey(lowerPK);
            bulkExportQueryCriteria.setExclusiveEndPrimaryKey(upperPK);
            bulkExportQueryCriteria.addColumnsToGet(columns);

            bulkExportRequest.setBulkExportQueryCriteria(bulkExportQueryCriteria);
            BulkExportResponse response = publicClient.bulkExport(bulkExportRequest);

            assertEquals(DataBlockType.DBT_SIMPLE_ROW_MATRIX, response.getDataBlockType());

            assertEquals(0, response.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit());
            assertEquals(0, response.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit());
            assertEquals(2000, response.getConsumedCapacity().getCapacityDataSize().getReadCapacityDataSize());
            assertEquals(0, response.getConsumedCapacity().getCapacityDataSize().getWriteCapacityDataSize());

            SimpleRowMatrixBlockParser parser = new SimpleRowMatrixBlockParser(response.getRows());

            assertEquals(50, parser.getRowCount());
            assertArrayEquals(columns, parser.parseFieldNames());
            assertEquals(2, parser.getPkCount());
            assertEquals(2, parser.getAttrCount());
            assertEquals(4, parser.getFieldCount());

            for (int i = 150; i < 200; i++) {
                assertEquals(i - 150, parser.next());
                assertEquals(Integer.toString(i), parser.getString(0));
                assertArrayEquals(Integer.toString(i).getBytes("UTF-8"), parser.getBinary(1));
                assertEquals(Integer.toString(i), parser.getString(2));
                assertEquals(i, parser.getLong(3));
                assertEquals(false, parser.isNull(0));
                assertEquals(false, parser.isNull(1));
                assertEquals(false, parser.isNull(2));
                assertEquals(false, parser.isNull(3));

                PrimaryKeyBuilder pkBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
                pkBuilder.addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString(new Integer(i).toString()));
                pkBuilder.addPrimaryKeyColumn("PK3", PrimaryKeyValue.fromBinary(new Integer(i).toString().getBytes("UTF-8")));
                PrimaryKey pk = pkBuilder.build();
                Row row = parser.getRow();
                assertEquals(pk, row.getPrimaryKey());

                assertEquals(Integer.toString(i), row.getLatestColumn("DC1").getValue().asString());
                assertEquals(i, row.getLatestColumn("DC2").getValue().asLong());
            }
        }
        // public plain buffer
        {
            PrimaryKey lowerPK = getPK(150);
            PrimaryKey upperPK = getPK(200);

            BulkExportRequest bulkExportRequest = new BulkExportRequest();
            BulkExportQueryCriteria bulkExportQueryCriteria = new BulkExportQueryCriteria(tableName);
            String[] columns = new String[]{"PK1", "PK3", "DC1", "DC2"};

            bulkExportQueryCriteria.setInclusiveStartPrimaryKey(lowerPK);
            bulkExportQueryCriteria.setExclusiveEndPrimaryKey(upperPK);
            bulkExportQueryCriteria.setDataBlockType(DataBlockType.DBT_PLAIN_BUFFER);
            bulkExportQueryCriteria.addColumnsToGet(columns);

            bulkExportRequest.setBulkExportQueryCriteria(bulkExportQueryCriteria);
            BulkExportResponse response = publicClient.bulkExport(bulkExportRequest);

            assertEquals(DataBlockType.DBT_PLAIN_BUFFER, response.getDataBlockType());

            PlainBufferBlockParser parser = new PlainBufferBlockParser(response.getRows());

            assertEquals(50, parser.getRowCount());

            List<Row> rows = parser.getRows();
            for (int i = 0; i < 50; i++) {
                Row row = rows.get(i);
                {
                    PrimaryKey target = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                            .addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString(Integer.toString(150 + i)))
                            .addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(150 + i))
                            .addPrimaryKeyColumn("PK3", PrimaryKeyValue.fromBinary(Integer.toString(150 + i).getBytes("UTF-8")))
                            .build();
                    assertEquals(target, row.getPrimaryKey());
                }
                {
                    assertEquals("DC1", row.getColumns()[0].getName());
                    assertEquals(Integer.toString(150 + i), row.getColumns()[0].getValue().asString());
                    assertTrue(row.getColumns()[0].getTimestamp() > 0);

                    assertEquals("DC2", row.getColumns()[1].getName());
                    assertEquals(150 + i, row.getColumns()[1].getValue().asLong());
                    assertTrue(row.getColumns()[1].getTimestamp() > 0);
                }
            }
        }
        // internal matrix
        {
            PrimaryKey lowerPK = getPK(150);
            PrimaryKey upperPK = getPK(200);

            BulkExportRequest bulkExportRequest = new BulkExportRequest();
            BulkExportQueryCriteria bulkExportQueryCriteria = new BulkExportQueryCriteria(tableName);
            String[] columns = new String[]{"PK1", "PK3", "DC1", "DC2"};
            String[] outOfColumns = new String[]{"PK1", "PK2", "PK3", "DC1", "DC2"};

            bulkExportQueryCriteria.setInclusiveStartPrimaryKey(lowerPK);
            bulkExportQueryCriteria.setExclusiveEndPrimaryKey(upperPK);
            bulkExportQueryCriteria.setDataBlockType(DataBlockType.DBT_SIMPLE_ROW_MATRIX);
            bulkExportQueryCriteria.addColumnsToGet(columns);

            bulkExportRequest.setBulkExportQueryCriteria(bulkExportQueryCriteria);
            BulkExportResponse response = internalClient.bulkExport(bulkExportRequest);

            assertEquals(0, response.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit());
            assertEquals(0, response.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit());
            assertEquals(2000, response.getConsumedCapacity().getCapacityDataSize().getReadCapacityDataSize());
            assertEquals(0, response.getConsumedCapacity().getCapacityDataSize().getWriteCapacityDataSize());

            SimpleRowMatrixBlockParser parser = new SimpleRowMatrixBlockParser(response.getRows());

            assertEquals(50, parser.getRowCount());
            assertArrayEquals(outOfColumns, parser.parseFieldNames());
            assertEquals(3, parser.getPkCount());
            assertEquals(2, parser.getAttrCount());
            assertEquals(5, parser.getFieldCount());

            for (int i = 150; i < 200; i++) {
                assertEquals(i - 150, parser.next());
                assertEquals(Integer.toString(i), parser.getString(0));
                assertEquals(i, parser.getLong(1));
                assertArrayEquals(Integer.toString(i).getBytes("UTF-8"), parser.getBinary(2));
                assertEquals(Integer.toString(i), parser.getString(3));
                assertEquals(i, parser.getLong(4));
                assertEquals(false, parser.isNull(0));
                assertEquals(false, parser.isNull(1));
                assertEquals(false, parser.isNull(2));
                assertEquals(false, parser.isNull(3));
                assertEquals(false, parser.isNull(4));

                PrimaryKeyBuilder pkBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
                pkBuilder.addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString(new Integer(i).toString()));
                pkBuilder.addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(i));
                pkBuilder.addPrimaryKeyColumn("PK3", PrimaryKeyValue.fromBinary(new Integer(i).toString().getBytes("UTF-8")));
                PrimaryKey pk = pkBuilder.build();
                Row row = parser.getRow();
                assertEquals(pk, row.getPrimaryKey());

                assertEquals(Integer.toString(i), row.getLatestColumn("DC1").getValue().asString());
                assertEquals(i, row.getLatestColumn("DC2").getValue().asLong());
            }
        }
        // internal plain buffer
        {
            PrimaryKey lowerPK = getPK(150);
            PrimaryKey upperPK = getPK(200);

            BulkExportRequest bulkExportRequest = new BulkExportRequest();
            BulkExportQueryCriteria bulkExportQueryCriteria = new BulkExportQueryCriteria(tableName);
            String[] columns = new String[]{"PK1", "PK3", "DC1", "DC2"};

            bulkExportQueryCriteria.setInclusiveStartPrimaryKey(lowerPK);
            bulkExportQueryCriteria.setExclusiveEndPrimaryKey(upperPK);
            bulkExportQueryCriteria.setDataBlockType(DataBlockType.DBT_PLAIN_BUFFER);
            bulkExportQueryCriteria.addColumnsToGet(columns);

            bulkExportRequest.setBulkExportQueryCriteria(bulkExportQueryCriteria);
            BulkExportResponse response = internalClient.bulkExport(bulkExportRequest);

            PlainBufferBlockParser parser = new PlainBufferBlockParser(response.getRows());

            assertEquals(50, parser.getRowCount());

            List<Row> rows = parser.getRows();
            for (int i = 0; i < 50; i++) {
                Row row = rows.get(i);
                {
                    PrimaryKey target = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                            .addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString(Integer.toString(150 + i)))
                            .addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(150 + i))
                            .addPrimaryKeyColumn("PK3", PrimaryKeyValue.fromBinary(Integer.toString(150 + i).getBytes("UTF-8")))
                            .build();
                    assertEquals(target, row.getPrimaryKey());
                }
                {
                    assertEquals("DC1", row.getColumns()[0].getName());
                    assertEquals(Integer.toString(150 + i), row.getColumns()[0].getValue().asString());
                    assertTrue(row.getColumns()[0].getTimestamp() > 0);

                    assertEquals("DC2", row.getColumns()[1].getName());
                    assertEquals(150 + i, row.getColumns()[1].getValue().asLong());
                    assertTrue(row.getColumns()[1].getTimestamp() > 0);
                }
            }
        }
    }

    @Test
    public void testNoPrimaryKey() throws Exception {
        List<RowPutChange> rowPutChanges = creatData(100, 300);
        putData(internalClient, rowPutChanges);
        putData(publicClient, rowPutChanges);

        // public matrix
        {
            PrimaryKey lowerPK = getPK(150);
            PrimaryKey upperPK = getPK(250);

            BulkExportRequest bulkExportRequest = new BulkExportRequest();
            BulkExportQueryCriteria bulkExportQueryCriteria = new BulkExportQueryCriteria(tableName);
            String[] columns = new String[]{"DC1", "DC2"};

            bulkExportQueryCriteria.setInclusiveStartPrimaryKey(lowerPK);
            bulkExportQueryCriteria.setExclusiveEndPrimaryKey(upperPK);
            bulkExportQueryCriteria.setDataBlockType(DataBlockType.DBT_SIMPLE_ROW_MATRIX);
            bulkExportQueryCriteria.addColumnsToGet(columns);

            bulkExportRequest.setBulkExportQueryCriteria(bulkExportQueryCriteria);
            BulkExportResponse response = publicClient.bulkExport(bulkExportRequest);

            assertEquals(0, response.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit());
            assertEquals(0, response.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit());
            assertEquals(4000, response.getConsumedCapacity().getCapacityDataSize().getReadCapacityDataSize());
            assertEquals(0, response.getConsumedCapacity().getCapacityDataSize().getWriteCapacityDataSize());

            SimpleRowMatrixBlockParser parser = new SimpleRowMatrixBlockParser(response.getRows());

            assertEquals(100, parser.getRowCount());
            assertArrayEquals(columns, parser.parseFieldNames());
            assertEquals(0, parser.getPkCount());
            assertEquals(2, parser.getAttrCount());
            assertEquals(2, parser.getFieldCount());

            for (int i = 150; i < 250; i++) {
                assertEquals(i - 150, parser.next());
                assertEquals(Integer.toString(i), parser.getString(0));
                assertEquals(i, parser.getLong(1));
                assertEquals(false, parser.isNull(0));
                assertEquals(false, parser.isNull(1));
            }
        }
        // public plain buffer
        {
            PrimaryKey lowerPK = getPK(150);
            PrimaryKey upperPK = getPK(250);

            BulkExportRequest bulkExportRequest = new BulkExportRequest();
            BulkExportQueryCriteria bulkExportQueryCriteria = new BulkExportQueryCriteria(tableName);
            String[] columns = new String[]{"DC1", "DC2"};

            bulkExportQueryCriteria.setInclusiveStartPrimaryKey(lowerPK);
            bulkExportQueryCriteria.setExclusiveEndPrimaryKey(upperPK);
            bulkExportQueryCriteria.setDataBlockType(DataBlockType.DBT_PLAIN_BUFFER);
            bulkExportQueryCriteria.addColumnsToGet(columns);

            bulkExportRequest.setBulkExportQueryCriteria(bulkExportQueryCriteria);
            BulkExportResponse response = publicClient.bulkExport(bulkExportRequest);

            PlainBufferBlockParser parser = new PlainBufferBlockParser(response.getRows());

            assertEquals(100, parser.getRowCount());

            List<Row> rows = parser.getRows();
            for (int i = 0; i < 100; i++) {
                Row row = rows.get(i);
                {
                    PrimaryKey target = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                            .addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString(Integer.toString(150 + i)))
                            .addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(150 + i))
                            .addPrimaryKeyColumn("PK3", PrimaryKeyValue.fromBinary(Integer.toString(150 + i).getBytes("UTF-8")))
                            .build();
                    assertEquals(target, row.getPrimaryKey());
                }
                {
                    assertEquals("DC1", row.getColumns()[0].getName());
                    assertEquals(Integer.toString(150 + i), row.getColumns()[0].getValue().asString());
                    assertTrue(row.getColumns()[0].getTimestamp() > 0);

                    assertEquals("DC2", row.getColumns()[1].getName());
                    assertEquals(150 + i, row.getColumns()[1].getValue().asLong());
                    assertTrue(row.getColumns()[1].getTimestamp() > 0);
                }
            }
        }
        // internal matrix
        {
            PrimaryKey lowerPK = getPK(150);
            PrimaryKey upperPK = getPK(250);

            BulkExportRequest bulkExportRequest = new BulkExportRequest();
            BulkExportQueryCriteria bulkExportQueryCriteria = new BulkExportQueryCriteria(tableName);
            String[] columns = new String[]{"DC1", "DC2"};
            String[] outOfColumns = new String[]{"DC1", "DC2"};

            bulkExportQueryCriteria.setInclusiveStartPrimaryKey(lowerPK);
            bulkExportQueryCriteria.setExclusiveEndPrimaryKey(upperPK);
            bulkExportQueryCriteria.setDataBlockType(DataBlockType.DBT_SIMPLE_ROW_MATRIX);
            bulkExportQueryCriteria.addColumnsToGet(columns);

            bulkExportRequest.setBulkExportQueryCriteria(bulkExportQueryCriteria);
            BulkExportResponse response = internalClient.bulkExport(bulkExportRequest);

            assertEquals(0, response.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit());
            assertEquals(0, response.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit());
            assertEquals(4000, response.getConsumedCapacity().getCapacityDataSize().getReadCapacityDataSize());
            assertEquals(0, response.getConsumedCapacity().getCapacityDataSize().getWriteCapacityDataSize());

            SimpleRowMatrixBlockParser parser = new SimpleRowMatrixBlockParser(response.getRows());

            assertEquals(100, parser.getRowCount());
            assertArrayEquals(outOfColumns, parser.parseFieldNames());
            assertEquals(0, parser.getPkCount());
            assertEquals(2, parser.getAttrCount());
            assertEquals(2, parser.getFieldCount());

            for (int i = 150; i < 250; i++) {
                assertEquals(i - 150, parser.next());
                assertEquals(Integer.toString(i), parser.getString(0));
                assertEquals(i, parser.getLong(1));
                assertEquals(false, parser.isNull(0));
                assertEquals(false, parser.isNull(1));
            }
        }
        // internal plain buffer
        {
            PrimaryKey lowerPK = getPK(150);
            PrimaryKey upperPK = getPK(250);

            BulkExportRequest bulkExportRequest = new BulkExportRequest();
            BulkExportQueryCriteria bulkExportQueryCriteria = new BulkExportQueryCriteria(tableName);
            String[] columns = new String[]{"DC1", "DC2"};

            bulkExportQueryCriteria.setInclusiveStartPrimaryKey(lowerPK);
            bulkExportQueryCriteria.setExclusiveEndPrimaryKey(upperPK);
            bulkExportQueryCriteria.setDataBlockType(DataBlockType.DBT_PLAIN_BUFFER);
            bulkExportQueryCriteria.addColumnsToGet(columns);

            bulkExportRequest.setBulkExportQueryCriteria(bulkExportQueryCriteria);
            BulkExportResponse response = internalClient.bulkExport(bulkExportRequest);

            PlainBufferBlockParser parser = new PlainBufferBlockParser(response.getRows());

            assertEquals(100, parser.getRowCount());

            List<Row> rows = parser.getRows();
            for (int i = 0; i < 100; i++) {
                Row row = rows.get(i);
                {
                    PrimaryKey target = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                            .addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString(Integer.toString(150 + i)))
                            .addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(150 + i))
                            .addPrimaryKeyColumn("PK3", PrimaryKeyValue.fromBinary(Integer.toString(150 + i).getBytes("UTF-8")))
                            .build();
                    assertEquals(target, row.getPrimaryKey());
                }
                {
                    assertEquals("DC1", row.getColumns()[0].getName());
                    assertEquals(Integer.toString(150 + i), row.getColumns()[0].getValue().asString());
                    assertTrue(row.getColumns()[0].getTimestamp() > 0);

                    assertEquals("DC2", row.getColumns()[1].getName());
                    assertEquals(150 + i, row.getColumns()[1].getValue().asLong());
                    assertTrue(row.getColumns()[1].getTimestamp() > 0);
                }
            }
        }
    }

    @Test
    public void testWithFilter() throws Exception {
        List<RowPutChange> rowPutChanges = creatData(100, 300);
        putData(internalClient, rowPutChanges);
        putData(publicClient, rowPutChanges);

        // public matrix
        {
            PrimaryKey lowerPK = getPK(150);
            PrimaryKey upperPK = getPK(300);

            BulkExportRequest bulkExportRequest = new BulkExportRequest();
            BulkExportQueryCriteria bulkExportQueryCriteria = new BulkExportQueryCriteria(tableName);
            String[] columns = new String[]{"DC1", "DC2"};

            bulkExportQueryCriteria.setInclusiveStartPrimaryKey(lowerPK);
            bulkExportQueryCriteria.setExclusiveEndPrimaryKey(upperPK);
            bulkExportQueryCriteria.addColumnsToGet(columns);
            Filter filter = new SingleColumnValueFilter(
                    "DC1",
                    SingleColumnValueFilter.CompareOperator.GREATER_EQUAL,
                    ColumnValue.fromString(Integer.toString(250)));
            bulkExportQueryCriteria.setFilter(filter);

            bulkExportRequest.setBulkExportQueryCriteria(bulkExportQueryCriteria);
            BulkExportResponse response = publicClient.bulkExport(bulkExportRequest);

            assertEquals(0, response.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit());
            assertEquals(0, response.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit());
            assertEquals(6000, response.getConsumedCapacity().getCapacityDataSize().getReadCapacityDataSize());
            assertEquals(0, response.getConsumedCapacity().getCapacityDataSize().getWriteCapacityDataSize());

            SimpleRowMatrixBlockParser parser = new SimpleRowMatrixBlockParser(response.getRows());

            assertEquals(50, parser.getRowCount());
            assertArrayEquals(columns, parser.parseFieldNames());
            assertEquals(0, parser.getPkCount());
            assertEquals(2, parser.getAttrCount());
            assertEquals(2, parser.getFieldCount());

            for (int i = 250; i < 300; i++) {
                assertEquals(i - 250, parser.next());
                assertEquals(Integer.toString(i), parser.getString(0));
                assertEquals(i, parser.getLong(1));
                assertEquals(false, parser.isNull(0));
                assertEquals(false, parser.isNull(1));
            }
        }

        // public plain buffer
        {
            PrimaryKey lowerPK = getPK(150);
            PrimaryKey upperPK = getPK(300);

            BulkExportRequest bulkExportRequest = new BulkExportRequest();
            BulkExportQueryCriteria bulkExportQueryCriteria = new BulkExportQueryCriteria(tableName);
            String[] columns = new String[]{"DC1", "DC2"};

            bulkExportQueryCriteria.setInclusiveStartPrimaryKey(lowerPK);
            bulkExportQueryCriteria.setExclusiveEndPrimaryKey(upperPK);
            bulkExportQueryCriteria.setDataBlockType(DataBlockType.DBT_PLAIN_BUFFER);
            bulkExportQueryCriteria.addColumnsToGet(columns);
            Filter filter = new SingleColumnValueFilter(
                    "DC1",
                    SingleColumnValueFilter.CompareOperator.GREATER_EQUAL,
                    ColumnValue.fromString(Integer.toString(250)));
            bulkExportQueryCriteria.setFilter(filter);

            bulkExportRequest.setBulkExportQueryCriteria(bulkExportQueryCriteria);
            BulkExportResponse response = publicClient.bulkExport(bulkExportRequest);

            assertEquals(0, response.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit());
            assertEquals(0, response.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit());
            assertEquals(6000, response.getConsumedCapacity().getCapacityDataSize().getReadCapacityDataSize());
            assertEquals(0, response.getConsumedCapacity().getCapacityDataSize().getWriteCapacityDataSize());

            PlainBufferBlockParser parser = new PlainBufferBlockParser(response.getRows());

            assertEquals(50, parser.getRowCount());

            List<Row> rows = parser.getRows();
            for (int i = 0; i < 50; i++) {
                Row row = rows.get(i);
                {
                    PrimaryKey target = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                            .addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString(Integer.toString(250 + i)))
                            .addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(250 + i))
                            .addPrimaryKeyColumn("PK3", PrimaryKeyValue.fromBinary(Integer.toString(250 + i).getBytes("UTF-8")))
                            .build();
                    assertEquals(target, row.getPrimaryKey());
                }
                {
                    assertEquals("DC1", row.getColumns()[0].getName());
                    assertEquals(Integer.toString(250 + i), row.getColumns()[0].getValue().asString());
                    assertTrue(row.getColumns()[0].getTimestamp() > 0);

                    assertEquals("DC2", row.getColumns()[1].getName());
                    assertEquals(250 + i, row.getColumns()[1].getValue().asLong());
                    assertTrue(row.getColumns()[1].getTimestamp() > 0);
                }
            }
        }

        // internal matrix
        {
            PrimaryKey lowerPK = getPK(150);
            PrimaryKey upperPK = getPK(300);

            BulkExportRequest bulkExportRequest = new BulkExportRequest();
            BulkExportQueryCriteria bulkExportQueryCriteria = new BulkExportQueryCriteria(tableName);
            String[] columns = new String[]{"DC1", "DC2"};
            String[] outOfColumns = new String[]{"DC1", "DC2"};

            bulkExportQueryCriteria.setInclusiveStartPrimaryKey(lowerPK);
            bulkExportQueryCriteria.setExclusiveEndPrimaryKey(upperPK);
            bulkExportQueryCriteria.setDataBlockType(DataBlockType.DBT_SIMPLE_ROW_MATRIX);
            bulkExportQueryCriteria.addColumnsToGet(columns);
            Filter filter = new SingleColumnValueFilter(
                    "DC1",
                    SingleColumnValueFilter.CompareOperator.GREATER_EQUAL,
                    ColumnValue.fromString(Integer.toString(250)));
            bulkExportQueryCriteria.setFilter(filter);

            bulkExportRequest.setBulkExportQueryCriteria(bulkExportQueryCriteria);
            BulkExportResponse response = internalClient.bulkExport(bulkExportRequest);

            assertEquals(2, bulkExportQueryCriteria.numColumnsToGet());

            assertEquals(0, response.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit());
            assertEquals(0, response.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit());
            assertEquals(6000, response.getConsumedCapacity().getCapacityDataSize().getReadCapacityDataSize());
            assertEquals(0, response.getConsumedCapacity().getCapacityDataSize().getWriteCapacityDataSize());

            SimpleRowMatrixBlockParser parser = new SimpleRowMatrixBlockParser(response.getRows());

            assertEquals(50, parser.getRowCount());
            assertArrayEquals(outOfColumns, parser.parseFieldNames());
            assertEquals(0, parser.getPkCount());
            assertEquals(2, parser.getAttrCount());
            assertEquals(2, parser.getFieldCount());

            for (int i = 250; i < 300; i++) {
                assertEquals(i - 250, parser.next());
                assertEquals(Integer.toString(i), parser.getString(0));
                assertEquals(i, parser.getLong(1));
                assertEquals(false, parser.isNull(0));
                assertEquals(false, parser.isNull(1));
            }
        }
        // internal plain buffer
        {
            PrimaryKey lowerPK = getPK(150);
            PrimaryKey upperPK = getPK(300);

            BulkExportRequest bulkExportRequest = new BulkExportRequest();
            BulkExportQueryCriteria bulkExportQueryCriteria = new BulkExportQueryCriteria(tableName);
            String[] columns = new String[]{"DC1", "DC2"};

            bulkExportQueryCriteria.setInclusiveStartPrimaryKey(lowerPK);
            bulkExportQueryCriteria.setExclusiveEndPrimaryKey(upperPK);
            bulkExportQueryCriteria.setDataBlockType(DataBlockType.DBT_PLAIN_BUFFER);
            bulkExportQueryCriteria.addColumnsToGet(columns);
            Filter filter = new SingleColumnValueFilter(
                    "DC1",
                    SingleColumnValueFilter.CompareOperator.GREATER_EQUAL,
                    ColumnValue.fromString(Integer.toString(250)));
            bulkExportQueryCriteria.setFilter(filter);

            bulkExportRequest.setBulkExportQueryCriteria(bulkExportQueryCriteria);
            BulkExportResponse response = internalClient.bulkExport(bulkExportRequest);

            assertEquals(0, response.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit());
            assertEquals(0, response.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit());
            assertEquals(6000, response.getConsumedCapacity().getCapacityDataSize().getReadCapacityDataSize());
            assertEquals(0, response.getConsumedCapacity().getCapacityDataSize().getWriteCapacityDataSize());

            PlainBufferBlockParser parser = new PlainBufferBlockParser(response.getRows());

            assertEquals(50, parser.getRowCount());

            List<Row> rows = parser.getRows();
            for (int i = 0; i < 50; i++) {
                Row row = rows.get(i);
                {
                    PrimaryKey target = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                            .addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString(Integer.toString(250 + i)))
                            .addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(250 + i))
                            .addPrimaryKeyColumn("PK3", PrimaryKeyValue.fromBinary(Integer.toString(250 + i).getBytes("UTF-8")))
                            .build();
                    assertEquals(target, row.getPrimaryKey());
                }
                {
                    assertEquals("DC1", row.getColumns()[0].getName());
                    assertEquals(Integer.toString(250 + i), row.getColumns()[0].getValue().asString());
                    assertTrue(row.getColumns()[0].getTimestamp() > 0);

                    assertEquals("DC2", row.getColumns()[1].getName());
                    assertEquals(250 + i, row.getColumns()[1].getValue().asLong());
                    assertTrue(row.getColumns()[1].getTimestamp() > 0);
                }
            }
        }
    }

    @Test
    public void testNoColumnToGet() throws Exception {
        List<RowPutChange> rowPutChanges = creatData(500, 600);
        putData(internalClient, rowPutChanges);
        putData(publicClient, rowPutChanges);

        // public - matrix
        {
            PrimaryKey lowerPK = getPK(550);
            PrimaryKey upperPK = getPK(570);

            BulkExportRequest bulkExportRequest = new BulkExportRequest();
            BulkExportQueryCriteria bulkExportQueryCriteria = new BulkExportQueryCriteria(tableName);
            String[] columns = new String[]{};

            bulkExportQueryCriteria.setInclusiveStartPrimaryKey(lowerPK);
            bulkExportQueryCriteria.setExclusiveEndPrimaryKey(upperPK);
            bulkExportQueryCriteria.setDataBlockType(DataBlockType.DBT_SIMPLE_ROW_MATRIX);
            bulkExportQueryCriteria.addColumnsToGet(columns);
            bulkExportRequest.setBulkExportQueryCriteria(bulkExportQueryCriteria);

            try {
                publicClient.bulkExport(bulkExportRequest);
                fail();
            } catch (TableStoreException e) {
                assertEquals(ErrorCode.INVALID_PARAMETER, e.getErrorCode());
                assertEquals("DBT_SIMPLE_ROW_MATRIX need at least one column to get.", e.getMessage());
            }
        }
        // public - pb
        {
            PrimaryKey lowerPK = getPK(550);
            PrimaryKey upperPK = getPK(570);

            BulkExportRequest bulkExportRequest = new BulkExportRequest();
            BulkExportQueryCriteria bulkExportQueryCriteria = new BulkExportQueryCriteria(tableName);
            String[] columns = new String[]{};

            bulkExportQueryCriteria.setInclusiveStartPrimaryKey(lowerPK);
            bulkExportQueryCriteria.setExclusiveEndPrimaryKey(upperPK);
            bulkExportQueryCriteria.setDataBlockType(DataBlockType.DBT_PLAIN_BUFFER);
            bulkExportQueryCriteria.addColumnsToGet(columns);

            bulkExportRequest.setBulkExportQueryCriteria(bulkExportQueryCriteria);
            BulkExportResponse response = publicClient.bulkExport(bulkExportRequest);

            assertEquals(0, response.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit());
            assertEquals(0, response.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit());
            assertEquals(920, response.getConsumedCapacity().getCapacityDataSize().getReadCapacityDataSize());
            assertEquals(0, response.getConsumedCapacity().getCapacityDataSize().getWriteCapacityDataSize());

            PlainBufferBlockParser parser = new PlainBufferBlockParser(response.getRows());

            assertEquals(20, parser.getRowCount());

            List<Row> rows = parser.getRows();
            for (int i = 0; i < 20; i++) {
                Row row = rows.get(i);
                {
                    PrimaryKey target = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                            .addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString(Integer.toString(550 + i)))
                            .addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(550 + i))
                            .addPrimaryKeyColumn("PK3", PrimaryKeyValue.fromBinary(Integer.toString(550 + i).getBytes("UTF-8")))
                            .build();
                    assertEquals(target, row.getPrimaryKey());
                }
                {
                    assertEquals("DC1", row.getColumns()[0].getName());
                    assertEquals(Integer.toString(550 + i), row.getColumns()[0].getValue().asString());
                    assertTrue(row.getColumns()[0].getTimestamp() > 0);

                    assertEquals("DC2", row.getColumns()[1].getName());
                    assertEquals(550 + i, row.getColumns()[1].getValue().asLong());
                    assertTrue(row.getColumns()[1].getTimestamp() > 0);

                    assertEquals("DC3", row.getColumns()[2].getName());
                    assertArrayEquals(Integer.toString(550 + i).getBytes("UTF-8"), row.getColumns()[2].getValue().asBinary());
                    assertTrue(row.getColumns()[2].getTimestamp() > 0);
                }
            }
        }
        // internal - matrix
        {
            PrimaryKey lowerPK = getPK(550);
            PrimaryKey upperPK = getPK(570);

            BulkExportRequest bulkExportRequest = new BulkExportRequest();
            BulkExportQueryCriteria bulkExportQueryCriteria = new BulkExportQueryCriteria(tableName);
            String[] columns = new String[]{};

            bulkExportQueryCriteria.setInclusiveStartPrimaryKey(lowerPK);
            bulkExportQueryCriteria.setExclusiveEndPrimaryKey(upperPK);
            bulkExportQueryCriteria.setDataBlockType(DataBlockType.DBT_SIMPLE_ROW_MATRIX);
            bulkExportQueryCriteria.addColumnsToGet(columns);
            bulkExportRequest.setBulkExportQueryCriteria(bulkExportQueryCriteria);

            try {
                internalClient.bulkExport(bulkExportRequest);
                fail();
            } catch (TableStoreException e) {
                assertEquals(ErrorCode.INVALID_PARAMETER, e.getErrorCode());
                assertEquals("DBT_SIMPLE_ROW_MATRIX need at least one column to get.", e.getMessage());
            }
        }
        // internal - pb
        {
            PrimaryKey lowerPK = getPK(550);
            PrimaryKey upperPK = getPK(570);

            BulkExportRequest bulkExportRequest = new BulkExportRequest();
            BulkExportQueryCriteria bulkExportQueryCriteria = new BulkExportQueryCriteria(tableName);
            String[] columns = new String[]{};

            bulkExportQueryCriteria.setInclusiveStartPrimaryKey(lowerPK);
            bulkExportQueryCriteria.setExclusiveEndPrimaryKey(upperPK);
            bulkExportQueryCriteria.setDataBlockType(DataBlockType.DBT_PLAIN_BUFFER);
            bulkExportQueryCriteria.addColumnsToGet(columns);

            bulkExportRequest.setBulkExportQueryCriteria(bulkExportQueryCriteria);
            BulkExportResponse response = internalClient.bulkExport(bulkExportRequest);

            assertEquals(0, bulkExportQueryCriteria.numColumnsToGet());

            assertEquals(0, response.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit());
            assertEquals(0, response.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit());
            assertEquals(920, response.getConsumedCapacity().getCapacityDataSize().getReadCapacityDataSize());
            assertEquals(0, response.getConsumedCapacity().getCapacityDataSize().getWriteCapacityDataSize());

            PlainBufferBlockParser parser = new PlainBufferBlockParser(response.getRows());

            assertEquals(20, parser.getRowCount());

            List<Row> rows = parser.getRows();
            for (int i = 0; i < 20; i++) {
                Row row = rows.get(i);
                {
                    PrimaryKey target = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                            .addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString(Integer.toString(550 + i)))
                            .addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(550 + i))
                            .addPrimaryKeyColumn("PK3", PrimaryKeyValue.fromBinary(Integer.toString(550 + i).getBytes("UTF-8")))
                            .build();
                    assertEquals(target, row.getPrimaryKey());
                }
                {
                    assertEquals("DC1", row.getColumns()[0].getName());
                    assertEquals(Integer.toString(550 + i), row.getColumns()[0].getValue().asString());
                    assertTrue(row.getColumns()[0].getTimestamp() > 0);

                    assertEquals("DC2", row.getColumns()[1].getName());
                    assertEquals(550 + i, row.getColumns()[1].getValue().asLong());
                    assertTrue(row.getColumns()[1].getTimestamp() > 0);

                    assertEquals("DC3", row.getColumns()[2].getName());
                    assertArrayEquals(Integer.toString(550 + i).getBytes("UTF-8"), row.getColumns()[2].getValue().asBinary());
                    assertTrue(row.getColumns()[2].getTimestamp() > 0);
                }
            }
        }
    }

    @Test
    public void testInvalidParam() throws Exception {
        {
            BulkExportRequest bulkExportRequest = new BulkExportRequest();
            try {
                publicClient.bulkExport(bulkExportRequest);
                fail();
            } catch (NullPointerException e) {
                assertTrue(true);
            }
        }
        {
            PrimaryKey upperPK = getPK(570);

            PrimaryKeyBuilder pkBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            pkBuilder.addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString(new Integer(111).toString()));
            pkBuilder.addPrimaryKeyColumn("PK3", PrimaryKeyValue.fromBinary(new Integer(111).toString().getBytes("UTF-8")));
            PrimaryKey lowerPK = pkBuilder.build();

            String[] columns = new String[]{"PK1"};

            BulkExportRequest bulkExportRequest = new BulkExportRequest();
            BulkExportQueryCriteria bulkExportQueryCriteria = new BulkExportQueryCriteria(tableName);

            bulkExportQueryCriteria.setInclusiveStartPrimaryKey(lowerPK);
            bulkExportQueryCriteria.setExclusiveEndPrimaryKey(upperPK);
            bulkExportQueryCriteria.addColumnsToGet(columns);

            bulkExportRequest.setBulkExportQueryCriteria(bulkExportQueryCriteria);

            try {
                publicClient.bulkExport(bulkExportRequest);
                fail();
            } catch (TableStoreException e) {
                assertEquals(ErrorCode.INVALID_PK, e.getErrorCode());
                assertEquals("Validate PK size fail. Input: 2, Meta: 3.", e.getMessage());
            }
        }
        {
            PrimaryKey lowerPK = getPK(570);
            PrimaryKey upperPK = getPK(150);

            String[] columns = new String[]{"PK1"};

            BulkExportRequest bulkExportRequest = new BulkExportRequest();
            BulkExportQueryCriteria bulkExportQueryCriteria = new BulkExportQueryCriteria(tableName);

            bulkExportQueryCriteria.setInclusiveStartPrimaryKey(lowerPK);
            bulkExportQueryCriteria.setExclusiveEndPrimaryKey(upperPK);
            bulkExportQueryCriteria.addColumnsToGet(columns);

            bulkExportRequest.setBulkExportQueryCriteria(bulkExportQueryCriteria);

            try {
                publicClient.bulkExport(bulkExportRequest);
                fail();
            } catch (TableStoreException e) {
                assertEquals(ErrorCode.INVALID_PARAMETER, e.getErrorCode());
                assertEquals("Begin key must less than end key in FORWARD", e.getMessage());
            }
        }
        {
            PrimaryKey lowerPK = getPK(150);
            PrimaryKey upperPK = getPK(150);

            String[] columns = new String[]{"PK1"};

            BulkExportRequest bulkExportRequest = new BulkExportRequest();
            BulkExportQueryCriteria bulkExportQueryCriteria = new BulkExportQueryCriteria(tableName);

            bulkExportQueryCriteria.setInclusiveStartPrimaryKey(lowerPK);
            bulkExportQueryCriteria.setExclusiveEndPrimaryKey(upperPK);
            bulkExportQueryCriteria.addColumnsToGet(columns);

            bulkExportRequest.setBulkExportQueryCriteria(bulkExportQueryCriteria);

            try {
                publicClient.bulkExport(bulkExportRequest);
                fail();
            } catch (TableStoreException e) {
                assertEquals(ErrorCode.INVALID_PARAMETER, e.getErrorCode());
                assertEquals("Begin key must less than end key in FORWARD", e.getMessage());
            }
        }
        {
            try {
                new BulkExportQueryCriteria("");
                fail();
            } catch (IllegalArgumentException e) {
                assertEquals("The name of table should not be null or empty.", e.getMessage());
            }

            try {
                new BulkExportQueryCriteria(null);
                fail();
            } catch (IllegalArgumentException e) {
                assertEquals("The name of table should not be null or empty.", e.getMessage());
            }
        }
    }

    @Test
    public void testWithLimit() throws Exception {
        {
            PrimaryKey lowerPK = getPK(150);
            PrimaryKey upperPK = getPK(151);

            BulkExportRequest bulkExportRequest = new BulkExportRequest();
            BulkExportQueryCriteria bulkExportQueryCriteria = new BulkExportQueryCriteria(tableName);

            bulkExportQueryCriteria.setInclusiveStartPrimaryKey(lowerPK);
            bulkExportQueryCriteria.setExclusiveEndPrimaryKey(upperPK);
            for (int i = 0; i < 1024; i++) {
                bulkExportQueryCriteria.addColumnsToGet("DC" + i);
            }

            bulkExportRequest.setBulkExportQueryCriteria(bulkExportQueryCriteria);
            publicClient.bulkExport(bulkExportRequest);

            bulkExportQueryCriteria.addColumnsToGet("DC" + 1024);
            try {
                publicClient.bulkExport(bulkExportRequest);
                fail();
            } catch (TableStoreException e) {
                assertEquals(ErrorCode.INVALID_PARAMETER, e.getErrorCode());
                assertEquals("The number of columns from the request exceeds the limit, limit count: 1024, column count: 1025.", e.getMessage());
            }
        }
    }

    @Test
    public void testWithMatrixIterator()  throws Exception {
        List<RowPutChange> rowPutChanges = creatData(10000, 16000);
        putData(publicClient, rowPutChanges);
        putData(internalClient, rowPutChanges);

        {
            PrimaryKey lowerPK = getPK(10000);
            PrimaryKey upperPK = getPK(16000);
            BulkExportQueryCriteria bulkExportQueryCriteria = new BulkExportQueryCriteria(tableName);
            bulkExportQueryCriteria.setInclusiveStartPrimaryKey(lowerPK);
            bulkExportQueryCriteria.setExclusiveEndPrimaryKey(upperPK);
            bulkExportQueryCriteria.addColumnsToGet("PK2");
            bulkExportQueryCriteria.addColumnsToGet("DC1");
            bulkExportQueryCriteria.addColumnsToGet("DC2");
            bulkExportQueryCriteria.addColumnsToGet("DC3");
            bulkExportQueryCriteria.addColumnsToGet("DC4");

            SimpleRowMatrixBlockRowIterator iterator = new SimpleRowMatrixBlockRowIterator(publicClient, bulkExportQueryCriteria, false);
            int i = 0;
            while (iterator.hasNext()) {
                int y = 10000 + i;
                        assertEquals(i, iterator.next().intValue());
                {
                    assertEquals(y, iterator.getLong(0));
                    assertEquals(Integer.toString(y), iterator.getString(1));
                    assertEquals(y, iterator.getLong(2));
                    assertArrayEquals(Integer.toString(y).getBytes("UTF-8"), iterator.getBinary(3));
                    assertEquals(null, iterator.getObject(4));

                    assertEquals(false, iterator.isNull(0));
                    assertEquals(false, iterator.isNull(1));
                    assertEquals(false, iterator.isNull(2));
                    assertEquals(false, iterator.isNull(3));
                    assertEquals(true, iterator.isNull(4));
                }
                Row row = iterator.getRow();
                {
                    PrimaryKey target = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                            .addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(y))
                            .build();
                    assertEquals(target, row.getPrimaryKey());
                }
                {
                    assertEquals(Integer.toString(y), row.getLatestColumn("DC1").getValue().asString());
                    assertEquals(y, row.getLatestColumn("DC2").getValue().asLong());
                    assertArrayEquals(Integer.toString(y).getBytes("UTF-8"), row.getLatestColumn("DC3").getValue().asBinary());
                    assertEquals(null, row.getLatestColumn("DC4"));
                }
                i++;
            }
        }
        {
            PrimaryKey lowerPK = getPK(10000);
            PrimaryKey upperPK = getPK(16000);
            BulkExportQueryCriteria bulkExportQueryCriteria = new BulkExportQueryCriteria(tableName);
            bulkExportQueryCriteria.setInclusiveStartPrimaryKey(lowerPK);
            bulkExportQueryCriteria.setExclusiveEndPrimaryKey(upperPK);
            bulkExportQueryCriteria.addColumnsToGet("PK2");
            bulkExportQueryCriteria.addColumnsToGet("DC1");
            bulkExportQueryCriteria.addColumnsToGet("DC2");
            bulkExportQueryCriteria.addColumnsToGet("DC3");
            bulkExportQueryCriteria.addColumnsToGet("DC4");

            SimpleRowMatrixBlockRowIterator iterator = new SimpleRowMatrixBlockRowIterator(internalClient, bulkExportQueryCriteria, false);
            int i = 0;
            while (iterator.hasNext()) {
                int y = 10000 + i;
                assertEquals(i, iterator.next().intValue());
                {
                    assertEquals(Integer.toString(y), iterator.getString(0));
                    assertEquals(y, iterator.getLong(1));
                    assertArrayEquals(Integer.toString(y).getBytes("UTF-8"), iterator.getBinary(2));
                    assertEquals(Integer.toString(y), iterator.getString(3));
                    assertEquals(y, iterator.getLong(4));
                    assertArrayEquals(Integer.toString(y).getBytes("UTF-8"), iterator.getBinary(5));
                    assertEquals(null, iterator.getObject(6));

                    assertEquals(false, iterator.isNull(0));
                    assertEquals(false, iterator.isNull(1));
                    assertEquals(false, iterator.isNull(2));
                    assertEquals(false, iterator.isNull(3));
                    assertEquals(false, iterator.isNull(4));
                    assertEquals(false, iterator.isNull(5));
                    assertEquals(true, iterator.isNull(6));
                }
                Row row = iterator.getRow();
                {
                    PrimaryKey target = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                            .addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString(Integer.toString(y)))
                            .addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(y))
                            .addPrimaryKeyColumn("PK3", PrimaryKeyValue.fromBinary(Integer.toString(y).getBytes("UTF-8")))
                            .build();
                    assertEquals(target, row.getPrimaryKey());
                }
                {
                    assertEquals(Integer.toString(y), row.getLatestColumn("DC1").getValue().asString());
                    assertEquals(y, row.getLatestColumn("DC2").getValue().asLong());
                    assertArrayEquals(Integer.toString(y).getBytes("UTF-8"), row.getLatestColumn("DC3").getValue().asBinary());
                    assertEquals(null, row.getLatestColumn("DC4"));
                }
                i++;
            }
        }
    }

    @Test
    public void testWithPlainBufferIterator() throws Exception {
        List<RowPutChange> rowPutChanges = creatData(10000, 16000);
        putData(publicClient, rowPutChanges);
        putData(internalClient, rowPutChanges);

        {
            PrimaryKey lowerPK = getPK(10000);
            PrimaryKey upperPK = getPK(16000);
            BulkExportQueryCriteria bulkExportQueryCriteria = new BulkExportQueryCriteria(tableName);
            bulkExportQueryCriteria.setInclusiveStartPrimaryKey(lowerPK);
            bulkExportQueryCriteria.setExclusiveEndPrimaryKey(upperPK);
            bulkExportQueryCriteria.addColumnsToGet("PK2");
            bulkExportQueryCriteria.addColumnsToGet("DC1");
            bulkExportQueryCriteria.addColumnsToGet("DC2");
            bulkExportQueryCriteria.addColumnsToGet("DC3");
            bulkExportQueryCriteria.addColumnsToGet("DC4");

            PlainBufferBlockRowIterator iterator = new PlainBufferBlockRowIterator(publicClient, bulkExportQueryCriteria);
            int i = 0;
            while (iterator.hasNext()) {
                int y = 10000 + i;
                Row row = iterator.next();
                {
                    PrimaryKey target = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                            .addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString(Integer.toString(y)))
                            .addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(y))
                            .addPrimaryKeyColumn("PK3", PrimaryKeyValue.fromBinary(Integer.toString(y).getBytes("UTF-8")))
                            .build();
                    assertEquals(target, row.getPrimaryKey());
                }
                {
                    assertEquals(Integer.toString(y), row.getLatestColumn("DC1").getValue().asString());
                    assertEquals(y, row.getLatestColumn("DC2").getValue().asLong());
                    assertArrayEquals(Integer.toString(y).getBytes("UTF-8"), row.getLatestColumn("DC3").getValue().asBinary());
                    assertEquals(null, row.getLatestColumn("DC4"));
                }
                i++;
            }
        }
        {
            PrimaryKey lowerPK = getPK(10000);
            PrimaryKey upperPK = getPK(16000);
            BulkExportQueryCriteria bulkExportQueryCriteria = new BulkExportQueryCriteria(tableName);
            bulkExportQueryCriteria.setInclusiveStartPrimaryKey(lowerPK);
            bulkExportQueryCriteria.setExclusiveEndPrimaryKey(upperPK);
            bulkExportQueryCriteria.addColumnsToGet("PK2");
            bulkExportQueryCriteria.addColumnsToGet("DC1");
            bulkExportQueryCriteria.addColumnsToGet("DC2");
            bulkExportQueryCriteria.addColumnsToGet("DC3");
            bulkExportQueryCriteria.addColumnsToGet("DC4");

            PlainBufferBlockRowIterator iterator = new PlainBufferBlockRowIterator(internalClient, bulkExportQueryCriteria);
            int i = 0;
            while (iterator.hasNext()) {
                int y = 10000 + i;
                Row row = iterator.next();
                {
                    PrimaryKey target = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                            .addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString(Integer.toString(y)))
                            .addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(y))
                            .addPrimaryKeyColumn("PK3", PrimaryKeyValue.fromBinary(Integer.toString(y).getBytes("UTF-8")))
                            .build();
                    assertEquals(target, row.getPrimaryKey());
                }
                {
                    assertEquals(Integer.toString(y), row.getLatestColumn("DC1").getValue().asString());
                    assertEquals(y, row.getLatestColumn("DC2").getValue().asLong());
                    assertArrayEquals(Integer.toString(y).getBytes("UTF-8"), row.getLatestColumn("DC3").getValue().asBinary());
                    assertEquals(null, row.getLatestColumn("DC4"));
                }
                i++;
            }
        }
    }
}
