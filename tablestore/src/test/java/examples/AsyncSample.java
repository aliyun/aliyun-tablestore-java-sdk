package examples;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.core.ErrorCode;
import com.alicloud.openservices.tablestore.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class AsyncSample {

    private static final String COLUMN_GID_NAME = "gid";
    private static final String COLUMN_UID_NAME = "uid";
    private static final String COLUMN_NAME_NAME = "name";
    private static final String COLUMN_AGE_NAME = "age";

    public static void main(String args[]) {
        final String endPoint = "";
        final String accessId = "";
        final String accessKey = "";
        final String instanceName = "";

        SyncClientInterface client = new SyncClient(endPoint, accessId, accessKey, instanceName);
        AsyncClientInterface asyncClient = new AsyncClient(endPoint, accessId, accessKey, instanceName);
        final String tableName = "sampleTable";

        try{
            // Create table
            createTable(client, tableName);

            // Note: Creating a table only submits a request; it takes OTS some time to create the table.
            // Here simply wait for 2 seconds, please modify according to your actual logic.
            Thread.sleep(2000);

            listTableWithFuture(asyncClient);
            listTableWithCallback(asyncClient);

            // Asynchronously and concurrently execute multiple batchWriteRow operations
            batchWriteRow(asyncClient, tableName);

            // Asynchronously execute multiple getRange operations concurrently
            batchGetRange(asyncClient, tableName);
        }catch(TableStoreException e){
            System.err.println("operation failed, detail: " + e.getMessage());
            // You can handle errors based on the error code. The ErrorCode definition for OTS is in OTSErrorCode.
            if (ErrorCode.QUOTA_EXHAUSTED.equals(e.getErrorCode())){
                System.err.println("Quota exhausted.");
            }
            // The Request ID can be used to contact customer service for diagnosing exceptions when there are issues.
            System.err.println("Request ID:" + e.getRequestId());
        }catch(ClientException e){
            // It might be due to poor network conditions or issues with the returned results.
            System.err.println("request failed, detail: " + e.getMessage());
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
        finally{
            // No littering.
            try {
                deleteTable(client, tableName);
            } catch (TableStoreException e) {
                System.err.println("delete table failed, detail: " + e.getMessage());
                e.printStackTrace();
            } catch (ClientException e) {
                System.err.println("delete table request failed, detail: " + e.getMessage());
                e.printStackTrace();
            }
            client.shutdown();
            asyncClient.shutdown();
        }
    }

    private static Future<GetRangeResponse> sendGetRangeRequest(AsyncClientInterface asyncClient, String tableName, long start, long end) {
        PrimaryKey startPk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn(COLUMN_GID_NAME, PrimaryKeyValue.fromLong(start))
                .addPrimaryKeyColumn(COLUMN_UID_NAME, PrimaryKeyValue.INF_MIN).build();

        PrimaryKey endPk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn(COLUMN_GID_NAME, PrimaryKeyValue.fromLong(end))
                .addPrimaryKeyColumn(COLUMN_UID_NAME, PrimaryKeyValue.INF_MIN).build();

        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
        criteria.setInclusiveStartPrimaryKey(startPk);
        criteria.setExclusiveEndPrimaryKey(endPk);
        criteria.setLimit(10);

        criteria.setMaxVersions(1);

        GetRangeRequest request = new GetRangeRequest();
        request.setRangeRowQueryCriteria(criteria);
        Future<GetRangeResponse> future = asyncClient.getRange(request, null);
        return future;
    }

    private static void batchGetRange(AsyncClientInterface asyncClient, String tableName) throws ExecutionException, InterruptedException {
        // Query multiple ranges of data at once, set 10 tasks, and each task queries 100 pieces of data.
        // Set the limit to 10 for each range query, and it will take 10 requests to retrieve 100 data entries.
        int count = 10;
        Future<GetRangeResponse>[] futures = new Future[count];
        for (int i = 0; i < count; i++) {
            futures[i] = sendGetRangeRequest(asyncClient, tableName, i * 100, i * 100 + 100);
        }

        // Check if all range queries are completed. If not, continue to send query requests.
        List<Row> allRows = new ArrayList<Row>();
        while (true) {
            boolean completed = true;
            for (int i = 0; i < futures.length; i++) {
                Future<GetRangeResponse> future = futures[i];
                if (future == null) {
                    continue;
                }

                if (future.isDone()) {
                    GetRangeResponse result = future.get();
                    allRows.addAll(result.getRows());

                    if (result.getNextStartPrimaryKey() != null) {
                        // The range has not been fully queried yet, and you need to continue reading from nextStart.
                        long nextStart = result.getNextStartPrimaryKey().getPrimaryKeyColumn(COLUMN_GID_NAME).getValue().asLong();
                        long rangeEnd = i * 100 + 100;
                        futures[i] = sendGetRangeRequest(asyncClient, tableName, nextStart, rangeEnd);
                        completed = false;
                    } else {
                        futures[i] = null; // If a range query is completed, set the corresponding future to null.
                    }
                } else {
                    completed = false;
                }
            }

            if (completed) {
                break;
            } else {
                try {
                    Thread.sleep(10); // Avoid busy waiting, wait for a short period of time after each loop
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        // All data has been read out.
        System.out.println("Total rows scanned: " + allRows.size());
    }

    private static void batchWriteRow(AsyncClientInterface asyncClient, String tableName) {
        // The row limit for BatchWriteRow is 100 rows. Use the asynchronous interface to implement a single batch import of 1000 rows.
        List<Future<BatchWriteRowResponse>> futures = new ArrayList<Future<BatchWriteRowResponse>>();
        int count = 10;
        // Send 10 requests at once, with each request writing 100 rows of data.
        for (int i = 0; i < count; i++) {
            BatchWriteRowRequest request = new BatchWriteRowRequest();
            for (int j = 0; j < 100; j++) {
                RowPutChange rowChange = new RowPutChange(tableName);
                PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn(COLUMN_GID_NAME, PrimaryKeyValue.fromLong(i * 100 + j))
                        .addPrimaryKeyColumn(COLUMN_UID_NAME, PrimaryKeyValue.fromLong(j)).build();
                rowChange.setPrimaryKey(primaryKey);
                rowChange.addColumn(COLUMN_NAME_NAME, ColumnValue.fromString("name" + j));
                rowChange.addColumn(COLUMN_AGE_NAME, ColumnValue.fromLong(j));

                request.addRowChange(rowChange);
            }
            Future<BatchWriteRowResponse> result = asyncClient.batchWriteRow(request, null);
            futures.add(result);
        }

        // Wait for the result to return
        List<BatchWriteRowResponse> results = new ArrayList<BatchWriteRowResponse>();
        for (Future<BatchWriteRowResponse> future : futures) {
            try {
                BatchWriteRowResponse result = future.get(); // Synchronously wait for the result to return
                results.add(result);
            } catch (TableStoreException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // Statistics of the returned results
        int totalSucceedRows = 0;
        int totalFailedRows = 0;
        for (BatchWriteRowResponse result : results) {
            totalSucceedRows += result.getSucceedRows().size();
            totalFailedRows += result.getFailedRows().size();
        }

        System.out.println("Total succeed rows: " + totalSucceedRows);
        System.out.println("Total failed rows: " + totalFailedRows);
    }

    private static void listTableWithCallback(AsyncClientInterface asyncClient) {
        final AtomicBoolean isDone = new AtomicBoolean(false);
        TableStoreCallback<ListTableRequest, ListTableResponse> callback = new TableStoreCallback<ListTableRequest, ListTableResponse>() {
            @Override
            public void onCompleted(ListTableRequest request, ListTableResponse response) {
                isDone.set(true);
                System.out.println("\nList table by listTableWithCallback:");
                for (String tableName : response.getTableNames()) {
                    System.out.println(tableName);
                }
            }

            @Override
            public void onFailed(ListTableRequest request, Exception ex) {
                isDone.set(true);
                ex.printStackTrace();
            }
        };

        asyncClient.listTable(callback); // Pass the callback to the SDK, which will automatically invoke the callback after completing the request and receiving the response.

        // Wait for the callback to be called. In general business processing logic, this step of waiting is not necessary.
        while (!isDone.get()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static void listTableWithFuture(AsyncClientInterface client) {
        // Synchronously wait for the result to return through Future.
        try {
            Future<ListTableResponse> future = client.listTable(null);
            ListTableResponse result = future.get(); // Synchronous wait
            System.out.println("\nList table by listTableWithFuture:");
            for (String tableName : result.getTableNames()) {
                System.out.println(tableName);
            }
        } catch (TableStoreException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Intermittently wait for the result to return through Future.
        try {
            Future<ListTableResponse> future = client.listTable(null);

            while (!future.isDone()) {
                System.out.println("Waiting for result of list table.");
                Thread.sleep(10); // Check if the result is returned every 10ms
            }

            ListTableResponse result = future.get();
            System.out.println("\nList table by listTableWithFuture:");
            for (String tableName : result.getTableNames()) {
                System.out.println(tableName);
            }
        } catch (TableStoreException e) {
            e.printStackTrace();
        } catch (ClientException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createTable(SyncClientInterface client, String tableName)
            throws TableStoreException, ClientException{
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn(COLUMN_GID_NAME, PrimaryKeyType.INTEGER);
        tableMeta.addPrimaryKeyColumn(COLUMN_UID_NAME, PrimaryKeyType.INTEGER);

        TableOptions tableOptions = new TableOptions();
        tableOptions.setTimeToLive(-1);
        tableOptions.setMaxVersions(1);

        // Set both read and write CU of this table to 0
        CapacityUnit capacityUnit = new CapacityUnit(0, 0);

        CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);
        request.setTableMeta(tableMeta);
        request.setReservedThroughput(new ReservedThroughput(capacityUnit));
        client.createTable(request);

        System.out.println("table is created.");
    }

    private static void deleteTable(SyncClientInterface client, String tableName)
            throws TableStoreException, ClientException{
        DeleteTableRequest request = new DeleteTableRequest(tableName);
        client.deleteTable(request);

        System.out.println("table is deleted.");
    }
}
