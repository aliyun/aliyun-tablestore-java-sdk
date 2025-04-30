package examples;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.condition.ColumnCondition;
import com.alicloud.openservices.tablestore.model.condition.SingleColumnValueCondition;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;

import java.util.Iterator;

public class PkAutoIncrSample {

    /**
     * In this example, a table named sampleTable is created with two primary keys, namely pk1 and pk2.
     */
    private static final String TABLE_NAME = "sampleTable_pk";
    private static final String PRIMARY_KEY_NAME_1 = "pk1";
    private static final String PRIMARY_KEY_NAME_2 = "pk2";

    public static void main(String[] args) {
        final String endPoint = "";
        final String accessId = "";
        final String accessKey = "";
        final String instanceName = "";

        SyncClient client = new SyncClient(endPoint, accessId, accessKey,
                instanceName);

        try {
            // Create table
            createTable(client);

            System.out.println("create table succeeded.");

            // Wait for the table to load.
            try {
                Thread.sleep(10 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // putRow
            PrimaryKey pk = putRow(client);

            System.out.println("put row succeeded,pk:" + pk.toString());

            // getRow
            getRow(client, pk);

            // updateRow
            updateRow(client, pk);

            getRowWithFilter(client, pk);

            // Use the "condition" to increment a column
            updateRowWithCondition(client, pk);

            // getRow
            getRow(client, pk);

            // Write two more rows
            putRow(client);
            putRow(client);

            // getRange
            getRange(client, "a", "z");

            // Use iterator for getRange
            getRangeByIterator(client, "a", "z");

            batchWriteRow(client);

        } catch (TableStoreException e) {
            System.err.println("operation failed, detail: " + e.getMessage());
            System.err.println("Request ID:" + e.getRequestId());
        } catch (ClientException e) {
            System.err.println("request failed, detail: " + e.getMessage());
        } finally {
            // For security reasons, drop table cannot be set as default here. If you need to drop the table, please enable it manually.
            // deleteTable(client);
        }
        client.shutdown();
    }

    private static void createTable(SyncClient client) {
        TableMeta tableMeta = new TableMeta(TABLE_NAME);
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(PRIMARY_KEY_NAME_1, PrimaryKeyType.STRING));
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(PRIMARY_KEY_NAME_2, PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT));

        int timeToLive = -1; // The expiration time of the data, in seconds, -1 means never expires. If the expiration time is set to one year, it would be 365 * 24 * 3600.
        int maxVersions = 1; // The maximum number of versions to save, setting it to 1 means that at most one version is saved for each column (saving the latest version).

        TableOptions tableOptions = new TableOptions(timeToLive, maxVersions);

        CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);

        client.createTable(request);
    }

    private static void deleteTable(SyncClient client) {
        DeleteTableRequest request = new DeleteTableRequest(TABLE_NAME);
        client.deleteTable(request);
    }

    private static PrimaryKey putRow(SyncClient client) {
        // Construct the primary key
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromString("chengdu"));
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.AUTO_INCREMENT);
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        RowPutChange rowPutChange = new RowPutChange(TABLE_NAME, primaryKey);
        rowPutChange.setReturnType(ReturnType.RT_PK);

        // Add some property columns
        long ts = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 3; j++) {
                rowPutChange.addColumn(new Column("Col" + i, ColumnValue.fromLong(j), ts + j));
            }
        }

        PutRowResponse response = client.putRow(new PutRowRequest(rowPutChange));
        // Print out the consumed CU
        CapacityUnit  cu = response.getConsumedCapacity().getCapacityUnit();
        System.out.println("Read CapacityUnit:" + cu.getReadCapacityUnit());
        System.out.println("Write CapacityUnit:" + cu.getWriteCapacityUnit());

        // Print out the returned PK column
        PrimaryKey pk = response.getRow().getPrimaryKey();
        System.out.println("PrimaryKey:" + pk.toString());

        return pk;
    }

    private static void updateRow(SyncClient client, PrimaryKey pk) {
        RowUpdateChange rowUpdateChange = new RowUpdateChange(TABLE_NAME, pk);

        // Update some columns
        for (int i = 0; i < 10; i++) {
            rowUpdateChange.put(new Column("Col" + i, ColumnValue.fromLong(i)));
        }

        // Delete a specific version of a column
        rowUpdateChange.deleteColumn("Col10", 1465373223000L);

        // Delete a specific column
        rowUpdateChange.deleteColumns("Col11");
        rowUpdateChange.setCondition(new Condition(RowExistenceExpectation.EXPECT_EXIST));

        client.updateRow(new UpdateRowRequest(rowUpdateChange));
    }

    private static void deleteRow(SyncClient client, PrimaryKey pk) {
        RowDeleteChange rowDeleteChange = new RowDeleteChange(TABLE_NAME, pk);

        client.deleteRow(new DeleteRowRequest(rowDeleteChange));
    }

    private static void batchWriteRow(SyncClient client) {
        BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();

        // Construct rowPutChange1
        PrimaryKeyBuilder pk1Builder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        pk1Builder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromString("Hangzhou"));
        pk1Builder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.AUTO_INCREMENT);
        RowPutChange rowPutChange1 = new RowPutChange(TABLE_NAME, pk1Builder.build());
        rowPutChange1.setReturnType(ReturnType.RT_PK);
        // Add some columns
        rowPutChange1.addColumn(new Column("Column_0", ColumnValue.fromLong(99)));

        // Add to batch operation
        batchWriteRowRequest.addRowChange(rowPutChange1);

        // Construct rowPutChange2
        PrimaryKeyBuilder pk2Builder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        pk2Builder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromString("Hangzhou"));
        pk2Builder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.AUTO_INCREMENT);
        RowPutChange rowPutChange2 = new RowPutChange(TABLE_NAME, pk2Builder.build());
        rowPutChange2.setReturnType(ReturnType.RT_PK);
        // Add some columns
        rowPutChange2.addColumn(new Column("Column_0", ColumnValue.fromLong(100)));

        // Add to batch operation
        batchWriteRowRequest.addRowChange(rowPutChange2);

        // Construct rowUpdateChange
        PrimaryKeyBuilder pk3Builder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        pk3Builder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromString("Hangzhou"));
        pk3Builder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.AUTO_INCREMENT);

        RowUpdateChange rowUpdateChange = new RowUpdateChange(TABLE_NAME, pk3Builder.build());
        rowUpdateChange.setReturnType(ReturnType.RT_PK);
        // Add a column
        rowUpdateChange.put(new Column("Column_0", ColumnValue.fromLong(101)));

        // Delete a column
        rowUpdateChange.deleteColumns("Column_1");
        // Add to batch operation
        batchWriteRowRequest.addRowChange(rowUpdateChange);

        // Construct rowDeleteChange
        PrimaryKeyBuilder pk4Builder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        pk4Builder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromString("Hangzhou"));
        pk4Builder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.fromLong(1));
        RowDeleteChange rowDeleteChange = new RowDeleteChange(TABLE_NAME, pk4Builder.build());
        rowDeleteChange.setReturnType(ReturnType.RT_PK);
        // Add to batch operation
        batchWriteRowRequest.addRowChange(rowDeleteChange);

        BatchWriteRowResponse response = client.batchWriteRow(batchWriteRowRequest);

        System.out.println("isAllSuccess:" + response.isAllSucceed());
        if (!response.isAllSucceed()) {
            for (BatchWriteRowResponse.RowResult rowResult : response.getFailedRows()) {
                System.out.println("failed rows:" + batchWriteRowRequest.getRowChange(rowResult.getTableName(), rowResult.getIndex()).getPrimaryKey());
                System.out.println("failed reason:" + rowResult.getError());
            }
            /*
             * You can reconstruct a request for failed rows to retry by using the createRequestForRetry method. Here, only the part of constructing the retry request is provided.
             * The recommended retry method is to use the SDK's custom retry policy feature, which supports retrying partial row errors in batch operations. After setting the retry policy, there is no need to add retry code at the API call site.
             */
            BatchWriteRowRequest retryRequest = batchWriteRowRequest.createRequestForRetry(response.getFailedRows());
        } else {
            for (BatchWriteRowResponse.RowResult rowResult : response.getSucceedRows()) {
                PrimaryKey pk = rowResult.getRow().getPrimaryKey();
                System.out.println("Return PK:" + pk.jsonize());
            }
        }
    }

    private static void getRow(SyncClient client, PrimaryKey pk) {
        // Read one row
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(TABLE_NAME, pk);
        // Set to read the latest version
        criteria.setMaxVersions(1);
        GetRowResponse getRowResponse = client.getRow(new GetRowRequest(criteria));
        Row row = getRowResponse.getRow();

        System.out.println("read a row, result: ");
        System.out.println(row);

        // Set to read certain columns
        criteria.addColumnsToGet("Col0");
        getRowResponse = client.getRow(new GetRowRequest(criteria));
        row = getRowResponse.getRow();

        System.out.println("read a row, result: ");
        System.out.println(row);
    }

    private static void getRowWithFilter(SyncClient client, PrimaryKey pk) {
        // Read a row
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(TABLE_NAME, pk);
        // Set to read the latest version
        criteria.setMaxVersions(1);

        // Set the filter, return the row when the value of Col0 is 0.
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("Col0",
                SingleColumnValueFilter.CompareOperator.EQUAL, ColumnValue.fromLong(0));
        // If the Col0 column does not exist, it will not return.
        singleColumnValueFilter.setPassIfMissing(false);
        // Judge only the latest version
        singleColumnValueFilter.setLatestVersionsOnly(true);

        criteria.setFilter(singleColumnValueFilter);

        GetRowResponse getRowResponse = client.getRow(new GetRowRequest(criteria));
        Row row = getRowResponse.getRow();

        System.out.println("read a row, result: ");
        System.out.println(row);
    }

    // Implement the optimistic locking mechanism through Condition, increment a column.
    private static void updateRowWithCondition(SyncClient client, PrimaryKey pk) {
        // Read a row
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(TABLE_NAME, pk);
        criteria.setMaxVersions(1);
        GetRowResponse getRowResponse = client.getRow(new GetRowRequest(criteria));
        Row row = getRowResponse.getRow();
        long col0Value = row.getLatestColumn("Col0").getValue().asLong();

        // Conditionally update the Col0 column to increment its value by 1.
        RowUpdateChange rowUpdateChange = new RowUpdateChange(TABLE_NAME, pk);
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
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromString(startPkValue));
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.fromLong(0));
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(primaryKeyBuilder.build());

        // Set the end primary key
        primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromString(endPkValue));
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.INF_MAX);
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
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromString(startPkValue));
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.fromLong(0));
        rangeIteratorParameter.setInclusiveStartPrimaryKey(primaryKeyBuilder.build());

        // Set the end primary key
        primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromString(endPkValue));
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.INF_MAX);
        rangeIteratorParameter.setExclusiveEndPrimaryKey(primaryKeyBuilder.build());

        rangeIteratorParameter.setMaxVersions(1);

        Iterator<Row> iterator = client.createRangeIterator(rangeIteratorParameter);

        System.out.println("use Iterator to GetRange, result:");
        while (iterator.hasNext()) {
            Row row = iterator.next();
            System.out.println(row);
        }
    }

}
