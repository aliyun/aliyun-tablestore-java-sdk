package com.alicloud.openservices.tablestore.functiontest;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.core.ErrorCode;
import com.alicloud.openservices.tablestore.core.protocol.TunnelServiceApi;
import com.alicloud.openservices.tablestore.model.*;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class TestBulkImportFunction {

    static String tableName = "YSTestBulkWrite";
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

    private static List<RowChange> getPutRowChange(Integer start, Integer end) throws Exception {
        List<RowChange> rowChanges = new ArrayList<RowChange>();
        for (Integer i = start; i < end; i++) {
            RowPutChange rowChange = new RowPutChange(tableName, getPK(i));
            rowChange.addColumn(new Column("DC1", ColumnValue.fromString(i.toString()), System.currentTimeMillis()));
            rowChange.addColumn(new Column("DC2", ColumnValue.fromLong(i)));
            rowChange.addColumn(new Column("DC3", ColumnValue.fromBinary(i.toString().getBytes("UTF-8"))));
            rowChange.setReturnType(ReturnType.RT_PK);
            rowChanges.add(rowChange);
        }
        return rowChanges;
    }

    private static List<RowChange> getUpDateRowChange(Integer start, Integer end) throws Exception {
        List<RowChange> rowChanges = new ArrayList<RowChange>();
        for (Integer i = start; i < end; i++) {
            RowUpdateChange rowChange = new RowUpdateChange(tableName, getPK(i));
            rowChange.put(new Column("DC4", ColumnValue.fromString(i.toString())));
            rowChange.put(new Column("DC5", ColumnValue.fromLong(i)));
            rowChange.put(new Column("DC6", ColumnValue.fromBinary(i.toString().getBytes("UTF-8"))));
            rowChange.setReturnType(ReturnType.RT_PK);
            rowChanges.add(rowChange);
        }
        return rowChanges;
    }

    private static List<RowChange> getDeleteRowChange(Integer start, Integer end) throws Exception {
        List<RowChange> rowChanges = new ArrayList<RowChange>();
        for (Integer i = start; i < end; i++) {
            RowDeleteChange rowChange = new RowDeleteChange(tableName, getPK(i));
            rowChange.setReturnType(ReturnType.RT_PK);
            rowChanges.add(rowChange);
        }
        return rowChanges;
    }

    private static GetRangeResponse getRange(SyncClient client, PrimaryKey startPk, PrimaryKey endPk) {
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(startPk);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(endPk);
        rangeRowQueryCriteria.setMaxVersions(1);
        return client.getRange(new GetRangeRequest(rangeRowQueryCriteria));
    }

    @Test
    public void testBase() throws Exception {
        {
            BulkImportRequest bulkImportRequest = new BulkImportRequest(tableName);
            bulkImportRequest.addRowChanges(getPutRowChange(2000, 2010));
            bulkImportRequest.addRowChanges(getUpDateRowChange(2010, 2020));
            bulkImportRequest.addRowChanges(getDeleteRowChange(2020, 2030));

            BulkImportResponse response = publicClient.bulkImport(bulkImportRequest);
            assertEquals(36, response.getRequestId().length());
            assertEquals(30, response.getSucceedRows().size());
            assertEquals(0, response.getFailedRows().size());
            assertEquals(true, response.isAllSucceed());
            assertEquals(30, response.getSucceedRows().size());

            int i = 2000;
            for (BulkImportResponse.RowResult rr : response.getRowResults().subList(0, 10)) {
                assertEquals(0, rr.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit());
                assertEquals(0, rr.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit());
                assertEquals(0, rr.getConsumedCapacity().getCapacityDataSize().getReadCapacityDataSize());
                assertEquals(50, rr.getConsumedCapacity().getCapacityDataSize().getWriteCapacityDataSize());
                assertEquals(null, rr.getError());
                i++;
            }
            for (BulkImportResponse.RowResult rr : response.getRowResults().subList(10, 20)) {
                assertEquals(0, rr.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit());
                assertEquals(0, rr.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit());
                assertEquals(0, rr.getConsumedCapacity().getCapacityDataSize().getReadCapacityDataSize());
                assertEquals(50, rr.getConsumedCapacity().getCapacityDataSize().getWriteCapacityDataSize());
                assertEquals(null, rr.getError());
                i++;
            }
            for (BulkImportResponse.RowResult rr : response.getRowResults().subList(20, 30)) {
                assertEquals(0, rr.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit());
                assertEquals(0, rr.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit());
                assertEquals(0, rr.getConsumedCapacity().getCapacityDataSize().getReadCapacityDataSize());
                assertEquals(25, rr.getConsumedCapacity().getCapacityDataSize().getWriteCapacityDataSize());
                assertEquals(null, rr.getError());
                i++;
            }

            GetRangeResponse getRangeResponse = getRange(publicClient, getPK(2000), getPK(2030));
            assertEquals(20, getRangeResponse.getRows().size());
            for (int y = 0; y < 10; y++) {
                Row row = getRangeResponse.getRows().get(y);
                int p = y + 2000;
                assertEquals(getPK(p), row.getPrimaryKey());
                assertEquals(Integer.toString(p), row.getLatestColumn("DC1").getValue().asString());
                assertEquals(p, row.getLatestColumn("DC2").getValue().asLong());
                assertArrayEquals(Integer.toString(p).getBytes("UTF-8"), row.getLatestColumn("DC3").getValue().asBinary());
                assertEquals(null, row.getLatestColumn("DC4"));
                assertEquals(null, row.getLatestColumn("DC5"));
                assertEquals(null, row.getLatestColumn("DC6"));
            }
            for (int y = 10; y < 20; y++) {
                Row row = getRangeResponse.getRows().get(y);
                int p = y + 2000;
                assertEquals(getPK(p), row.getPrimaryKey());
                assertEquals(null, row.getLatestColumn("DC1"));
                assertEquals(null, row.getLatestColumn("DC2"));
                assertEquals(null, row.getLatestColumn("DC3"));
                assertEquals(Integer.toString(p), row.getLatestColumn("DC4").getValue().asString());
                assertEquals(p, row.getLatestColumn("DC5").getValue().asLong());
                assertArrayEquals(Integer.toString(p).getBytes("UTF-8"), row.getLatestColumn("DC6").getValue().asBinary());
            }
        }
        {
            BulkImportRequest bulkImportRequest = new BulkImportRequest(tableName);
            bulkImportRequest.addRowChanges(getPutRowChange(2000, 2010));
            bulkImportRequest.addRowChanges(getUpDateRowChange(2010, 2020));
            bulkImportRequest.addRowChanges(getDeleteRowChange(2020, 2030));

            BulkImportResponse response = internalClient.bulkImport(bulkImportRequest);
            assertEquals(36, response.getRequestId().length());
            assertEquals(30, response.getSucceedRows().size());
            assertEquals(0, response.getFailedRows().size());
            assertEquals(true, response.isAllSucceed());
            assertEquals(30, response.getSucceedRows().size());

            int i = 2000;
            for (BulkImportResponse.RowResult rr : response.getRowResults().subList(0, 10)) {
                assertEquals(0, rr.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit());
                assertEquals(0, rr.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit());
                assertEquals(0, rr.getConsumedCapacity().getCapacityDataSize().getReadCapacityDataSize());
                assertEquals(50, rr.getConsumedCapacity().getCapacityDataSize().getWriteCapacityDataSize());
                assertEquals(null, rr.getError());
                i++;
            }
            for (BulkImportResponse.RowResult rr : response.getRowResults().subList(10, 20)) {
                assertEquals(0, rr.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit());
                assertEquals(0, rr.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit());
                assertEquals(0, rr.getConsumedCapacity().getCapacityDataSize().getReadCapacityDataSize());
                assertEquals(50, rr.getConsumedCapacity().getCapacityDataSize().getWriteCapacityDataSize());
                assertEquals(null, rr.getError());
                i++;
            }
            for (BulkImportResponse.RowResult rr : response.getRowResults().subList(20, 30)) {
                assertEquals(0, rr.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit());
                assertEquals(0, rr.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit());
                assertEquals(0, rr.getConsumedCapacity().getCapacityDataSize().getReadCapacityDataSize());
                assertEquals(25, rr.getConsumedCapacity().getCapacityDataSize().getWriteCapacityDataSize());
                assertEquals(null, rr.getError());
                i++;
            }

            GetRangeResponse getRangeResponse = getRange(publicClient, getPK(2000), getPK(2030));
            assertEquals(20, getRangeResponse.getRows().size());
            for (int y = 0; y < 10; y++) {
                Row row = getRangeResponse.getRows().get(y);
                int p = y + 2000;
                assertEquals(getPK(p), row.getPrimaryKey());
                assertEquals(Integer.toString(p), row.getLatestColumn("DC1").getValue().asString());
                assertEquals(p, row.getLatestColumn("DC2").getValue().asLong());
                assertArrayEquals(Integer.toString(p).getBytes("UTF-8"), row.getLatestColumn("DC3").getValue().asBinary());
                assertEquals(null, row.getLatestColumn("DC4"));
                assertEquals(null, row.getLatestColumn("DC5"));
                assertEquals(null, row.getLatestColumn("DC6"));
            }
            for (int y = 10; y < 20; y++) {
                Row row = getRangeResponse.getRows().get(y);
                int p = y + 2000;
                assertEquals(getPK(p), row.getPrimaryKey());
                assertEquals(null, row.getLatestColumn("DC1"));
                assertEquals(null, row.getLatestColumn("DC2"));
                assertEquals(null, row.getLatestColumn("DC3"));
                assertEquals(Integer.toString(p), row.getLatestColumn("DC4").getValue().asString());
                assertEquals(p, row.getLatestColumn("DC5").getValue().asLong());
                assertArrayEquals(Integer.toString(p).getBytes("UTF-8"), row.getLatestColumn("DC6").getValue().asBinary());
            }
        }
    }

    @Test
    public void testInvalidParam() throws Exception {
        // no param
        {
            BulkImportRequest bulkImportRequest = new BulkImportRequest(tableName);
            try {
                publicClient.bulkImport(bulkImportRequest);
                fail();
            } catch (TableStoreException e) {
                assertEquals(String.format("No operation is specified for table: '%s'.", tableName), e.getMessage());
            }
        }
        // error table
        {
            try {
                new BulkImportRequest("");
                fail();
            } catch (IllegalArgumentException e) {
                assertEquals("The name of table should not be null or empty.", e.getMessage());
            }
        }
        {
            PrimaryKeyBuilder pkBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            pkBuilder.addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString(new Integer(0).toString()));
            pkBuilder.addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(0));
            pkBuilder.addPrimaryKeyColumn("PK3", PrimaryKeyValue.fromBinary(new Integer(0).toString().getBytes("UTF-8")));
            PrimaryKey pk = pkBuilder.build();

            RowPutChange rowChange = new RowPutChange(tableName, pk);
            rowChange.addColumn(new Column("DC1", ColumnValue.fromString(""), System.currentTimeMillis()));
            rowChange.addColumn(new Column("DC2", ColumnValue.fromLong(0)));
            rowChange.addColumn(new Column("DC3", ColumnValue.fromBinary("0".getBytes("UTF-8"))));
            rowChange.setReturnType(ReturnType.RT_PK);

            BulkImportRequest bulkImportRequest = new BulkImportRequest( tableName+ "abcdefg");
            bulkImportRequest.addRowChange(rowChange);

            BulkImportResponse response = publicClient.bulkImport(bulkImportRequest);
            assertEquals(false, response.isAllSucceed());
            assertEquals(1, response.getFailedRows().size());

            BulkImportResponse.RowResult result = response.getFailedRows().get(0);
            assertEquals(ErrorCode.OBJECT_NOT_EXIST, result.getError().getCode());
            assertEquals("Requested table does not exist.", result.getError().getMessage());
        }
        // error pk
        {
            PrimaryKeyBuilder pkBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            pkBuilder.addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString(new Integer(0).toString()));
            pkBuilder.addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromString(new Integer(0).toString()));
            pkBuilder.addPrimaryKeyColumn("PK3", PrimaryKeyValue.fromBinary(new Integer(0).toString().getBytes("UTF-8")));
            PrimaryKey pk = pkBuilder.build();

            RowPutChange rowChange = new RowPutChange(tableName, pk);
            rowChange.addColumn(new Column("DC1", ColumnValue.fromString(""), System.currentTimeMillis()));
            rowChange.addColumn(new Column("DC2", ColumnValue.fromLong(0)));
            rowChange.addColumn(new Column("DC3", ColumnValue.fromBinary("0".getBytes("UTF-8"))));
            rowChange.setReturnType(ReturnType.RT_PK);

            BulkImportRequest bulkImportRequest = new BulkImportRequest(tableName);
            bulkImportRequest.addRowChange(rowChange);

            BulkImportResponse response = publicClient.bulkImport(bulkImportRequest);
            assertEquals(false, response.isAllSucceed());
            assertEquals(1, response.getFailedRows().size());

            BulkImportResponse.RowResult result = response.getFailedRows().get(0);
            assertEquals(ErrorCode.INVALID_PK, result.getError().getCode());
            assertEquals("Validate PK type fail. Input: VT_STRING, Meta: VT_INTEGER.", result.getError().getMessage());
        }
        {
            PrimaryKeyBuilder pkBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            pkBuilder.addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString(new Integer(0).toString()));
            pkBuilder.addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(0));
            pkBuilder.addPrimaryKeyColumn("PK3", PrimaryKeyValue.fromBinary(new Integer(0).toString().getBytes("UTF-8")));
            PrimaryKey pk = pkBuilder.build();

            RowPutChange rowChange = new RowPutChange(tableName, pk);
            rowChange.addColumn(new Column("DC1", ColumnValue.fromString(""), System.currentTimeMillis()));
            rowChange.addColumn(new Column("DC2", ColumnValue.fromLong(0)));
            rowChange.addColumn(new Column("DC2", ColumnValue.fromBinary("0".getBytes("UTF-8"))));
            rowChange.setReturnType(ReturnType.RT_PK);

            BulkImportRequest bulkImportRequest = new BulkImportRequest( tableName);
            bulkImportRequest.addRowChange(rowChange);

            BulkImportResponse response = publicClient.bulkImport(bulkImportRequest);
            assertEquals(false, response.isAllSucceed());
            assertEquals(1, response.getFailedRows().size());

            BulkImportResponse.RowResult result = response.getFailedRows().get(0);
            assertEquals(ErrorCode.INVALID_PARAMETER, result.getError().getCode());
            assertEquals("Invalid column type for column DC2 expect VT_INTEGER actual VT_BLOB", result.getError().getMessage());
        }
    }

    @Test
    public void testLimit() throws Exception {
        {
            BulkImportRequest bulkImportRequest = new BulkImportRequest(tableName);
            bulkImportRequest.addRowChanges(getPutRowChange(2000, 2100));
            bulkImportRequest.addRowChanges(getUpDateRowChange(2100, 2150));
            bulkImportRequest.addRowChanges(getDeleteRowChange(2150, 2200));

            BulkImportResponse response = publicClient.bulkImport(bulkImportRequest);
            assertEquals(true, response.isAllSucceed());
        }
        {
            BulkImportRequest bulkImportRequest = new BulkImportRequest(tableName);
            bulkImportRequest.addRowChanges(getPutRowChange(2000, 2100));
            bulkImportRequest.addRowChanges(getUpDateRowChange(2100, 2150));
            bulkImportRequest.addRowChanges(getDeleteRowChange(2150, 2201));

            try {
                publicClient.bulkImport(bulkImportRequest);
                fail();
            } catch (TableStoreException e) {
                assertEquals(ErrorCode.INVALID_PARAMETER, e.getErrorCode());
                assertEquals("Rows count exceeds the upper limit: 200.", e.getMessage());
            }
        }
    }
}
