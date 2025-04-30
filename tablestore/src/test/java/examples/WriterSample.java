package examples;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.core.auth.DefaultCredentials;
import com.alicloud.openservices.tablestore.core.auth.ServiceCredentials;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.writer.RowWriteResult;
import com.alicloud.openservices.tablestore.writer.WriterConfig;
import com.alicloud.openservices.tablestore.writer.WriterResult;
import com.alicloud.openservices.tablestore.writer.enums.BatchRequestType;
import com.alicloud.openservices.tablestore.writer.enums.DispatchMode;
import com.alicloud.openservices.tablestore.writer.enums.WriteMode;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.commons.codec.digest.DigestUtils.md5Hex;


public class WriterSample {

    private static String endpoint = "https://[instanceName].cn-hangzhou.ots.aliyuncs.com";
    private static String instanceName = "instanceName";
    private static String accessKeyId = "XXXXXXXXXX";
    private static String accessKeySecret = "XXXXXXXXXXXXXXXXXXXXXX";
    private static String tableName = "tableName";

    private static AtomicLong succeedRows = new AtomicLong();
    private static AtomicLong failedRows = new AtomicLong();

    public static void main(String[] args) {
        WriterSample sample = new WriterSample();

        /**
         * Make sure the table exists before using the Writer.
         * 1. The writer will verify the existence of the table.
         * 2. It will also verify that the data being written matches the table's fields and types.
         */
        sample.tryCreateTable();

        /**
         * It is recommended to use the following initialization:
         * DefaultTableStoreWriter(
         *      String endpoint,                                                   // Instance domain name
         *      ServiceCredentials credentials,                                    // Authentication information: includes AK, and also supports token
         *      String instanceName,                                               // Instance name
         *      String tableName,                                                  // Table name: one writer is only for one table
         *      WriterConfig config,                                               // Configuration of the writer
         *      TableStoreCallback<RowChange, RowWriteResult> resultCallback       // Row-level callback
         * )
         */
        TableStoreWriter writer = sample.createTablesStoreWriter();

        /**
         * Future usage: Batch write
         * */
        sample.writeRowListWithFuture(writer);

        /**
         * Future usage: Single row write
         * */
        sample.writeSingleRowWithFuture(writer);

        System.out.println("Count by TablestoreCallback: failedRow=" + failedRows.get() + ", succeedRow=" + succeedRows.get());
        System.out.println("Count by WriterStatics: " + writer.getWriterStatistics());

        /**
         * The user needs to actively close the Writer. After all queue data is written, the client and internal thread pool will automatically shut down.
         */
        writer.close();
    }


    private TableStoreWriter createTablesStoreWriter() {

        WriterConfig config = new WriterConfig();
        config.setWriteMode(WriteMode.SEQUENTIAL);                      // Serial write (serial write within each bucket)
        config.setBatchRequestType(BatchRequestType.BULK_IMPORT);       // Underlying construction of BulkImportRequest for batch writing
        config.setDispatchMode(DispatchMode.HASH_PRIMARY_KEY);          // Bucketing based on the hash value of the primary key to ensure that the same primary key falls into one bucket, and then write in order.
        config.setBucketCount(100);                                     // Number of buckets, improves the concurrency of serial writes, and is positively correlated with the write speed when the machine's bottleneck is not reached.
        config.setCallbackThreadCount(16);                              // Set the number of threads in the thread pool for running Callbacks internally in the Writer
        config.setAllowDuplicatedRowInBatchRequest(false);              // Within the underlying constructed batch request, duplicate rows are not allowed (mainly for secondary indexes; if ignoring user settings, false will override).

        /**
         * User-defined row-level callback
         * This example demonstrates the callback capability through a simple success and failure count.
         */
        TableStoreCallback<RowChange, RowWriteResult> resultCallback = new TableStoreCallback<RowChange, RowWriteResult>() {
            @Override
            public void onCompleted(RowChange rowChange, RowWriteResult cc) {
                succeedRows.incrementAndGet();
            }

            @Override
            public void onFailed(RowChange rowChange, Exception ex) {
                failedRows.incrementAndGet();
            }
        };

        ServiceCredentials credentials = new DefaultCredentials(accessKeyId, accessKeySecret);


        /**
         * It is recommended to use the thread pool and Client built internally, which is convenient for users to isolate the initialization and release logic.
         */
        DefaultTableStoreWriter writer = new DefaultTableStoreWriter(
                endpoint, credentials, instanceName, tableName, config, resultCallback);

        return writer;
    }


    private void tryCreateTable() {
        SyncClient ots = new SyncClient(endpoint, accessKeyId, accessKeySecret, instanceName);

        try {
            ots.deleteTable(new DeleteTableRequest(tableName));
        } catch (Exception e) {
        }

        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("pk_0", PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn("pk_1", PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn("pk_2", PrimaryKeyType.INTEGER);
        TableOptions tableOptions = new TableOptions(-1, 1);
        CreateTableRequest request = new CreateTableRequest(
                tableMeta, tableOptions, new ReservedThroughput(new CapacityUnit(0, 0)));

        try {
            CreateTableResponse res = ots.createTable(request);
        } catch (Exception e) {
            throw new ClientException(e);
        } finally {
            ots.shutdown();
        }
    }


    public void writeSingleRowWithFuture(TableStoreWriter writer) {
        System.out.println("=========================================================[Start]");
        System.out.println("Write Single Row With Future");
        int rowsCount = 20;
        int columnsCount = 10;
        String strValue = "1234567890";
        AtomicLong rowIndex = new AtomicLong(-1);

        List<Future<WriterResult>> futures = new LinkedList<Future<WriterResult>>();
        for (long index = rowIndex.incrementAndGet(); index < rowsCount; index = rowIndex.incrementAndGet()) {

            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString(md5Hex(index + "")))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromString("pk" + index))
                    .addPrimaryKeyColumn("pk_2", PrimaryKeyValue.fromLong(index % 5))
                    .build();

            RowUpdateChange rowChange = new RowUpdateChange(tableName, pk);
            for (int j = 0; j < columnsCount; j++) {
                rowChange.put("column_" + j, ColumnValue.fromString(strValue));
            }
            rowChange.put("index", ColumnValue.fromLong(index));
            Future<WriterResult> future = writer.addRowChangeWithFuture(rowChange);
            futures.add(future);
        }

        System.out.println("Write thread finished.");
        writer.flush();

        printFutureResult(futures);

        System.out.println("=========================================================[Finish]");
    }


    public void writeRowListWithFuture(TableStoreWriter writer) {
        System.out.println("=========================================================[Start]");
        System.out.println("Write Row List With Future");

        int rowsCount = 10000;
        int columnsCount = 10;
        String strValue = "1234567890";
        AtomicLong rowIndex = new AtomicLong(-1);

        List<Future<WriterResult>> futures = new LinkedList<Future<WriterResult>>();
        List<RowChange> rowChanges = new LinkedList<RowChange>();
        for (long index = rowIndex.incrementAndGet(); index < rowsCount; index = rowIndex.incrementAndGet()) {

            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString(md5Hex(index + "")))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromString("pk" + index))
                    .addPrimaryKeyColumn("pk_2", PrimaryKeyValue.fromLong(index % 5))
                    .build();

            RowUpdateChange rowChange = new RowUpdateChange(tableName, pk);
            for (int j = 0; j < columnsCount; j++) {
                rowChange.put("column_" + j, ColumnValue.fromString(strValue));
            }
            rowChange.put("index", ColumnValue.fromLong(index));
            rowChanges.add(rowChange);
            if (Math.random() > 0.995 || index == rowsCount - 1) {
                Future<WriterResult> future = writer.addRowChangeWithFuture(rowChanges);
                futures.add(future);
                rowChanges.clear();
            }
        }

        System.out.println("Write thread finished.");
        writer.flush();
        printFutureResult(futures);
        System.out.println("=========================================================[Finish]");
    }

    private void printFutureResult(List<Future<WriterResult>> futures) {
        int totalRow = 0;

        for (int index = 0; index < futures.size(); index++) {
            try {
                WriterResult result = futures.get(index).get();
                totalRow += result.getTotalCount();
                System.out.println(String.format(
                        "Future[%d] finished:\tfailed: %d\tsucceed: %d\tfutureBatch: %d\ttotalFinished: %d",
                        index, result.getFailedRows().size(), result.getSucceedRows().size(),
                        result.getTotalCount(), totalRow));

            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
}
