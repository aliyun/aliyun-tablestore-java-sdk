package examples;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.BulkExportRequest;
import com.alicloud.openservices.tablestore.model.condition.ColumnCondition;
import com.alicloud.openservices.tablestore.model.condition.SingleColumnValueCondition;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;
import com.alicloud.openservices.tablestore.model.tunnel.BulkExportQueryCriteria;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DataOperationSample {

    /**
     * In this example, a table named sampleTable is created, which contains only one primary key, and the primary key name is pk.
     */
    private static final String TABLE_NAME = "sampleTable";
    private static final String PRIMARY_KEY_NAME = "pk";

    public static void main(String[] args) {
        final String endPoint = "";
        final String accessId = "";
        final String accessKey = "";
        final String instanceName = "";

        SyncClient client = new SyncClient(endPoint, accessId, accessKey,
                instanceName);

        try {
            // Create table
            deleteTable(client);
            createTable(client);

            // Wait for the table to load.
            try {
                Thread.sleep(10 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // putRow
            putRow(client, "pkValue");

            // getRow
            getRow(client, "pkValue");

            // updateRow
            updateRow(client, "pkValue");

            // Use the condition to increment a column
            updateRowWithCondition(client, "pkValue");

            // getRow
            getRow(client, "pkValue");

            // Write two more rows
            putRow(client, "aaa");
            putRow(client, "bbb");

            increment(client, "pkValue");

            // getRange
            getRange(client, "a", "z");

            // Use iterator for getRange
            getRangeByIterator(client, "a", "z");

            batchWriteRow(client);

            batchGetRow(client);

            getRange(client, "a", "z");

        } catch (TableStoreException e) {
            System.err.println("operation failed, detail: " + e.getMessage());
            System.err.println("Request ID:" + e.getRequestId());
        } catch (ClientException e) {
            System.err.println("request failed, detail: " + e.getMessage());
        } finally {
            // For security reasons, table deletion cannot be set as default here. If you need to delete a table, you need to enable it manually.
            // deleteTable(client);
        }
        client.shutdown();
    }

    private static void createTable(SyncClient client) {
        TableMeta tableMeta = new TableMeta(TABLE_NAME);
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(PRIMARY_KEY_NAME, PrimaryKeyType.STRING));

        int timeToLive = -1; // The expiration time of the data, in seconds. -1 means never expires. For example, if you set the expiration time to one year, it would be 365 * 24 * 3600.
        int maxVersions = 1; // The maximum number of versions to save, setting to 1 means that at most one version is saved for each column (saving the latest version).

        TableOptions tableOptions = new TableOptions(timeToLive, maxVersions);

        CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);

        client.createTable(request);
    }

    private static void deleteTable(SyncClient client) {
        DeleteTableRequest request = new DeleteTableRequest(TABLE_NAME);
        client.deleteTable(request);
    }

    private static void putRow(SyncClient client, String pkValue) {
        // Construct the primary key
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString(pkValue));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        RowPutChange rowPutChange = new RowPutChange(TABLE_NAME, primaryKey);

        // Add some property columns
        long ts = System.currentTimeMillis();
        rowPutChange.addColumn(new Column("price", ColumnValue.fromLong(5120), ts));

        client.putRow(new PutRowRequest(rowPutChange));
    }

    private static void updateRow(SyncClient client, String pkValue) {
        // Construct the primary key
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString(pkValue));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        RowUpdateChange rowUpdateChange = new RowUpdateChange(TABLE_NAME, primaryKey);

        // Update some columns
        for (int i = 0; i < 10; i++) {
            rowUpdateChange.put(new Column("Col" + i, ColumnValue.fromLong(i)));
        }

        // Delete a specific version of a column
        rowUpdateChange.deleteColumn("Col10", 1465373223000L);

        // Delete a certain column
        rowUpdateChange.deleteColumns("Col11");

        client.updateRow(new UpdateRowRequest(rowUpdateChange));
    }

    private static void deleteRow(SyncClient client, String pkValue) {
        // Construct the primary key
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString(pkValue));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        RowDeleteChange rowDeleteChange = new RowDeleteChange(TABLE_NAME, primaryKey);

        client.deleteRow(new DeleteRowRequest(rowDeleteChange));
    }

    private static void increment(SyncClient client, String pkValue) {
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString(pkValue));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        RowUpdateChange rowUpdateChange = new RowUpdateChange(TABLE_NAME, primaryKey);
        rowUpdateChange.increment(new Column("price", ColumnValue.fromLong(10000L)));
        rowUpdateChange.setReturnType(ReturnType.RT_PK);

        UpdateRowResponse response = client.updateRow(new UpdateRowRequest(rowUpdateChange));
        Row row = response.getRow();
        System.out.println("update result (add value = 10000): start");
        System.out.println(row);
        System.out.println("update result (add value = 10000): end");
    }

    private static void batchWriteRow(SyncClient client) {
        BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();

        // Construct rowPutChange1
        PrimaryKeyBuilder pk1Builder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        pk1Builder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString("pk1"));
        RowPutChange rowPutChange1 = new RowPutChange(TABLE_NAME, pk1Builder.build());
        // Add some columns
        for (int i = 0; i < 10; i++) {
            rowPutChange1.addColumn(new Column("Col" + i, ColumnValue.fromLong(i)));
        }
        // Add to batch operation
        batchWriteRowRequest.addRowChange(rowPutChange1);

        // Construct rowPutChange2
        PrimaryKeyBuilder pk2Builder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        pk2Builder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString("pk2"));
        RowPutChange rowPutChange2 = new RowPutChange(TABLE_NAME, pk2Builder.build());
        // Add some columns
        for (int i = 0; i < 10; i++) {
            rowPutChange2.addColumn(new Column("Col" + i, ColumnValue.fromLong(i)));
        }
        // Add to batch operation
        batchWriteRowRequest.addRowChange(rowPutChange2);

        // Construct rowUpdateChange
        PrimaryKeyBuilder pk3Builder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        pk3Builder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString("pk3"));
        RowUpdateChange rowUpdateChange = new RowUpdateChange(TABLE_NAME, pk3Builder.build());
        // Add some columns
        for (int i = 0; i < 10; i++) {
            rowUpdateChange.put(new Column("Col" + i, ColumnValue.fromLong(i)));
        }
        // Delete a column
        rowUpdateChange.deleteColumns("Col10");
        // Add to batch operation
        batchWriteRowRequest.addRowChange(rowUpdateChange);

        // Construct rowDeleteChange
        PrimaryKeyBuilder pk4Builder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        pk4Builder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString("pk4"));
        RowDeleteChange rowDeleteChange = new RowDeleteChange(TABLE_NAME, pk4Builder.build());
        // Add to batch operation
        batchWriteRowRequest.addRowChange(rowDeleteChange);

        // Construct increment
        PrimaryKeyBuilder primaryKeyBuilderInc = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilderInc.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString("pkValue"));
        PrimaryKey primaryKeyInc = primaryKeyBuilderInc.build();
        RowUpdateChange rowUpdateChangeInc = new RowUpdateChange(TABLE_NAME, primaryKeyInc);
        rowUpdateChangeInc.increment(new Column("price", ColumnValue.fromLong(20000L)));
        rowUpdateChangeInc.setReturnType(ReturnType.RT_PK);
        batchWriteRowRequest.addRowChange(rowUpdateChangeInc);

        BatchWriteRowResponse response = client.batchWriteRow(batchWriteRowRequest);

        System.out.println("isAllSuccess:" + response.isAllSucceed());
        if (!response.isAllSucceed()) {
            for (BatchWriteRowResponse.RowResult rowResult : response.getFailedRows()) {
                System.out.println("failed rows:" + batchWriteRowRequest.getRowChange(rowResult.getTableName(), rowResult.getIndex()).getPrimaryKey());
                System.out.println("failed reason:" + rowResult.getError());
            }
            /**
             * You can reconstruct a request for failed rows to retry using the createRequestForRetry method. Here, only the part of constructing the retry request is provided.
             * The recommended retry method is to use the SDK's custom retry policy feature, which supports retrying partial row errors in batch operations. After setting the retry policy, there is no need to add retry code at the interface call.
             */
            BatchWriteRowRequest retryRequest = batchWriteRowRequest.createRequestForRetry(response.getFailedRows());
        }
    }

    private static void getRow(SyncClient client, String pkValue) {
        // Construct the primary key
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString(pkValue));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        // Read a row
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(TABLE_NAME, primaryKey);
        // Set to read the latest version
        criteria.setMaxVersions(1);
        GetRowResponse getRowResponse = client.getRow(new GetRowRequest(criteria));
        Row row = getRowResponse.getRow();

        System.out.println("read all rows, result: ");
        System.out.println(row);

        // Set to read certain columns
        criteria.addColumnsToGet("Col0");
        getRowResponse = client.getRow(new GetRowRequest(criteria));
        row = getRowResponse.getRow();

        System.out.println("read (col0), result: ");
        System.out.println(row);
    }

    private static void getRowWithFilter(SyncClient client, String pkValue) {
        // Construct the primary key
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString(pkValue));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        // Read a row
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(TABLE_NAME, primaryKey);
        // Set to read the latest version
        criteria.setMaxVersions(1);

        // Set the filter, return the row when the value of Col0 is 0.
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("Col0",
                SingleColumnValueFilter.CompareOperator.EQUAL, ColumnValue.fromLong(0));
        // If the Col0 column does not exist, it will not return.
        singleColumnValueFilter.setPassIfMissing(false);
        // Only judge the latest version
        singleColumnValueFilter.setLatestVersionsOnly(true);

        criteria.setFilter(singleColumnValueFilter);

        GetRowResponse getRowResponse = client.getRow(new GetRowRequest(criteria));
        Row row = getRowResponse.getRow();

        System.out.println("read all rows, result: ");
        System.out.println(row);
    }

    private static void batchGetRow(SyncClient client) {
        MultiRowQueryCriteria multiRowQueryCriteria = new MultiRowQueryCriteria(TABLE_NAME);
        // Add 10 rows to be read.
        for (int i = 0; i < 10; i++) {
            PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString("pk" + i));
            PrimaryKey primaryKey = primaryKeyBuilder.build();
            multiRowQueryCriteria.addRow(primaryKey);
        }
        // Add conditions
        multiRowQueryCriteria.setMaxVersions(1);
        multiRowQueryCriteria.addColumnsToGet("Col0");
        multiRowQueryCriteria.addColumnsToGet("Col1");
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("Col0",
                SingleColumnValueFilter.CompareOperator.EQUAL, ColumnValue.fromLong(0));
        singleColumnValueFilter.setPassIfMissing(false);
        multiRowQueryCriteria.setFilter(singleColumnValueFilter);

        BatchGetRowRequest batchGetRowRequest = new BatchGetRowRequest();
        // batchGetRow supports reading data from multiple tables. A multiRowQueryCriteria corresponds to the query condition of one table, and multiple multiRowQueryCriteria can be added.
        batchGetRowRequest.addMultiRowQueryCriteria(multiRowQueryCriteria);

        BatchGetRowResponse batchGetRowResponse = client.batchGetRow(batchGetRowRequest);

        System.out.println("isAllSuccess:" + batchGetRowResponse.isAllSucceed());
        if (!batchGetRowResponse.isAllSucceed()) {
            for (BatchGetRowResponse.RowResult rowResult : batchGetRowResponse.getFailedRows()) {
                System.out.println("failed rows:" + batchGetRowRequest.getPrimaryKey(rowResult.getTableName(), rowResult.getIndex()));
                System.out.println("failed reason:" + rowResult.getError());
            }

            /**
             * You can reconstruct a request for failed rows to retry by using the createRequestForRetry method. Here, only the part of constructing the retry request is provided.
             * The recommended retry method is to use the custom retry policy feature of the SDK, which supports retrying partial row errors in batch operations. After setting the retry policy, there is no need to add retry code at the interface call.
             */
            BatchGetRowRequest retryRequest = batchGetRowRequest.createRequestForRetry(batchGetRowResponse.getFailedRows());
        }
    }

    // Implement the optimistic locking mechanism through Condition, increment a column.
    private static void updateRowWithCondition(SyncClient client, String pkValue) {
        // Construct the primary key
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString(pkValue));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        // Read a row
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(TABLE_NAME, primaryKey);
        criteria.setMaxVersions(1);
        GetRowResponse getRowResponse = client.getRow(new GetRowRequest(criteria));
        Row row = getRowResponse.getRow();
        long col0Value = row.getLatestColumn("Col0").getValue().asLong();

        // Conditionally update the Col0 column, incrementing the column value by 1.
        RowUpdateChange rowUpdateChange = new RowUpdateChange(TABLE_NAME, primaryKey);
        Condition condition = new Condition(RowExistenceExpectation.EXPECT_EXIST);
        ColumnCondition columnCondition = new SingleColumnValueCondition("Col0", SingleColumnValueCondition.CompareOperator.EQUAL, ColumnValue.fromLong(col0Value));
        condition.setColumnCondition(columnCondition);
        rowUpdateChange.setCondition(condition);
        rowUpdateChange.put(new Column("Col0", ColumnValue.fromLong(col0Value + 1)));

        try {
            client.updateRow(new UpdateRowRequest(rowUpdateChange));
        } catch (TableStoreException ex) {
            System.out.println(ex.toString());
        }
    }

    private static void getRange(SyncClient client, String startPkValue, String endPkValue) {
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(TABLE_NAME);

        // Set the start primary key
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString(startPkValue));
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(primaryKeyBuilder.build());

        // Set the end primary key
        primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString(endPkValue));
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(primaryKeyBuilder.build());

        rangeRowQueryCriteria.setMaxVersions(1);

        System.out.println("GetRange result:");
        while (true) {
            GetRangeResponse getRangeResponse = client.getRange(new GetRangeRequest(rangeRowQueryCriteria));
            for (Row row : getRangeResponse.getRows()) {
                System.out.println(row);
            }

            // If nextStartPrimaryKey is not null, continue reading.
            if (getRangeResponse.getNextStartPrimaryKey() != null) {
                rangeRowQueryCriteria.setInclusiveStartPrimaryKey(getRangeResponse.getNextStartPrimaryKey());
            } else {
                break;
            }
        }
    }

    private static void getRangeByIterator(SyncClient client, String startPkValue, String endPkValue) {
        RangeIteratorParameter rangeIteratorParameter = new RangeIteratorParameter(TABLE_NAME);

        // Set the start primary key
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString(startPkValue));
        rangeIteratorParameter.setInclusiveStartPrimaryKey(primaryKeyBuilder.build());

        // Set the end primary key
        primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString(endPkValue));
        rangeIteratorParameter.setExclusiveEndPrimaryKey(primaryKeyBuilder.build());

        rangeIteratorParameter.setMaxVersions(1);

        Iterator<Row> iterator = client.createRangeIterator(rangeIteratorParameter);

        System.out.println("use Iterator to GetRange, result:");
        while (iterator.hasNext()) {
            Row row = iterator.next();
            System.out.println(row);
        }
    }

    private static void bulkImport(SyncClient client, String start, String end){
        // create bulkImportRequest
        BulkImportRequest bulkImportRequest = new BulkImportRequest(TABLE_NAME);

        // create rowChanges
        List<RowChange> rowChanges = new ArrayList<RowChange>();
        for (Integer i = Integer.valueOf(start); i <= Integer.valueOf(end); i++){
            PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            primaryKeyBuilder.addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(String.valueOf(i)));
            PrimaryKey primaryKey = primaryKeyBuilder.build();
            RowPutChange rowChange = new RowPutChange(TABLE_NAME,primaryKey);
            rowChange.addColumn(new Column("DC1", ColumnValue.fromString(i.toString())));
            rowChange.addColumn(new Column("DC2", ColumnValue.fromString(i.toString())));
            rowChange.addColumn(new Column("DC3", ColumnValue.fromString(i.toString())));
            rowChanges.add(rowChange);
        }

        bulkImportRequest.addRowChanges(rowChanges);
        // get bulkImportResponse
        BulkImportResponse bulkImportResponse = client.bulkImport(bulkImportRequest);
    }

    private static void bulkExport(SyncClient client, String start, String end){
        // create startPrimaryKey
        PrimaryKeyBuilder startPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        startPrimaryKeyBuilder.addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(String.valueOf(start)));
        PrimaryKey startPrimaryKey = startPrimaryKeyBuilder.build();

        // create endPrimaryKey
        PrimaryKeyBuilder endPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endPrimaryKeyBuilder.addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(String.valueOf(end)));
        PrimaryKey endPrimaryKey = endPrimaryKeyBuilder.build();

        // create bulkExportRequest
        BulkExportRequest bulkExportRequest = new BulkExportRequest();
        // create bulkExportQueryCriteria
        BulkExportQueryCriteria bulkExportQueryCriteria = new BulkExportQueryCriteria(TABLE_NAME);

        bulkExportQueryCriteria.setInclusiveStartPrimaryKey(startPrimaryKey);
        bulkExportQueryCriteria.setExclusiveEndPrimaryKey(endPrimaryKey);
        bulkExportQueryCriteria.setDataBlockType(DataBlockType.DBT_PLAIN_BUFFER);
        bulkExportQueryCriteria.addColumnsToGet("pk");
        bulkExportQueryCriteria.addColumnsToGet("DC1");
        bulkExportQueryCriteria.addColumnsToGet("DC2");

        bulkExportRequest.setBulkExportQueryCriteria(bulkExportQueryCriteria);
        // get bulkExportResponse
        BulkExportResponse bulkExportResponse = client.bulkExport(bulkExportRequest);
    }
}
