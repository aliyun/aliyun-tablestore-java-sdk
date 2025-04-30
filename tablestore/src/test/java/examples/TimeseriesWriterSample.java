package examples;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.core.auth.DefaultCredentials;
import com.alicloud.openservices.tablestore.core.auth.ServiceCredentials;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.timeseries.*;
import com.alicloud.openservices.tablestore.DefaultTableStoreTimeseriesWriter;
import com.alicloud.openservices.tablestore.timeserieswriter.TimeseriesWriterResult;
import com.alicloud.openservices.tablestore.timeserieswriter.callback.TimeseriesRowResult;
import com.alicloud.openservices.tablestore.timeserieswriter.config.TimeseriesWriterConfig;
import com.alicloud.openservices.tablestore.timeserieswriter.enums.TSDispatchMode;
import com.alicloud.openservices.tablestore.timeserieswriter.enums.TSWriteMode;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class TimeseriesWriterSample {
    private static String endpoint = "https://[instanceName].cn-hangzhou.ots.aliyuncs.com";
    private static String instanceName = "instanceName";
    private static String accessKeyId = "XXXXXXXXXX";
    private static String accessKeySecret = "XXXXXXXXXXXXXXXXXXXXXX";

    private static String tableName = "tableName";
    private static AtomicLong succeedRows = new AtomicLong();
    private static AtomicLong failedRows = new AtomicLong();

    public static void main(String[] args) {
        TimeseriesWriterSample sample = new TimeseriesWriterSample();

        /**
         * Make sure the table exists before using the Writer.
         * According to the test, writing data to a time-series table will not result in abnormal conditions after the table has been created for 30 seconds.
         * The writer will verify the existence of the table.
         * */
        sample.tryCreateTable();

        /**
         * It is recommended to use the following initialization:
         * DefaultTableStoreTimeseriesWriter(
         *             String endpoint,                                                             // Instance domain name
         *             ServiceCredentials credentials,                                              // Authentication information: includes AK, and token is also supported
         *             String instanceName,                                                         // Instance name
         *             TimeseriesWriterConfig config,                                               // Configuration for the writer
         *             TableStoreCallback<TimeseriesRow, TimeseriesRowResult> resultCallback)       // Row-level callback
         */
        DefaultTableStoreTimeseriesWriter writer = sample.createTableStoreTimeseriesWriter();

        /**
         * Future usage: Batch write
         * */
        sample.writeTimeseriesRowWithFuture(writer);

        System.out.println("Count by TablestoreCallback: failedRow=" + failedRows.get() + ", succeedRow=" + succeedRows.get());
        System.out.println("Count by WriterStatics: " + writer.getTimeseriesWriterStatistics());

        /**
         * The user needs to actively close the Writer. After all queue data is written, the client and internal thread pool will be automatically closed.
         */
        writer.close();
    }

    private void tryCreateTable() {
        TimeseriesClient ots = new TimeseriesClient(endpoint, accessKeyId, accessKeySecret, instanceName);

//         Delete table
        try {
            ots.deleteTimeseriesTable(new DeleteTimeseriesTableRequest(tableName));
        } catch (Exception e) {
        }


        TimeseriesTableMeta timeseriesTableMeta = new TimeseriesTableMeta(tableName);
        int timeToLive = -1;
        timeseriesTableMeta.setTimeseriesTableOptions(new TimeseriesTableOptions(timeToLive));
        CreateTimeseriesTableRequest request = new CreateTimeseriesTableRequest(timeseriesTableMeta);

        try {
            CreateTimeseriesTableResponse res = ots.createTimeseriesTable(request);
            System.out.println("waiting for creating time series table ......");
            TimeUnit.SECONDS.sleep(30);
        } catch (Exception e) {
            throw new ClientException(e);
        } finally {
            ots.shutdown();
        }
    }

    private DefaultTableStoreTimeseriesWriter createTableStoreTimeseriesWriter() {

        /**
         * Construct writer configuration
         */
        TimeseriesWriterConfig config = new TimeseriesWriterConfig();
        config.setWriteMode(TSWriteMode.PARALLEL);                         // Parallel write (parallel write within each bucket)
        config.setDispatchMode(TSDispatchMode.HASH_PRIMARY_KEY);            // Bucketing based on the hash value of the primary key to ensure that the same primary key falls into one bucket, and then write in order.
        config.setBucketCount(4);                                          // Number of buckets, improves the concurrency of serial writes, and is positively correlated with the writing speed when the machine's bottleneck is not reached.
        config.setCallbackThreadCount(16);                                // Set the number of threads in the thread pool for running Callbacks internally in the Writer.
        /**
         * User-defined row-level callback
         * This example demonstrates the callback capability through a simple success and failure count.
         */
        TableStoreCallback<TimeseriesTableRow, TimeseriesRowResult> resultCallback = new TableStoreCallback<TimeseriesTableRow, TimeseriesRowResult>() {
            @Override
            public void onCompleted(TimeseriesTableRow rowChange, TimeseriesRowResult cc) {
                succeedRows.incrementAndGet();
            }

            @Override
            public void onFailed(TimeseriesTableRow rowChange, Exception ex) {
                failedRows.incrementAndGet();
            }
        };

        ServiceCredentials credentials = new DefaultCredentials(accessKeyId, accessKeySecret);

        /**
         * It is recommended to use the internally built thread pool and Client, which makes it convenient for users to isolate the initialization and release logic.
         */
        DefaultTableStoreTimeseriesWriter writer = new DefaultTableStoreTimeseriesWriter(
                endpoint, credentials, instanceName, config, resultCallback);

        return writer;
    }

    private void writeTimeseriesRowWithFuture(DefaultTableStoreTimeseriesWriter writer) {
        System.out.println("=========================================================[Start]");
        System.out.print("Write Timeseries Row With Future, ");

        int rowsCount = 10000;
        int columnsCount = 10;
        AtomicLong rowIndex = new AtomicLong(-1);

        List<Future<TimeseriesWriterResult>> futures = new LinkedList<Future<TimeseriesWriterResult>>();
        List<TimeseriesTableRow> rows = new ArrayList<TimeseriesTableRow>();
        for (long index = rowIndex.incrementAndGet(); index < rowsCount; index = rowIndex.incrementAndGet()) {
            Map<String, String> tags = new HashMap<String, String>();
            tags.put("region", "hangzhou");
            tags.put("os", "Ubuntu16.04");
            // Construct the TimeseriesKey through measurementName, dataSource, and tags.
            TimeseriesKey timeseriesKey = new TimeseriesKey("cpu", "host_" + index, tags);
            // Create a timeseriesRow by specifying the timeseriesKey and timeInUs.
            TimeseriesRow row = new TimeseriesRow(timeseriesKey, System.currentTimeMillis() * 1000 + index);
            // Add data value (field).
            for (int j = 0; j < columnsCount; j++) {
                row.addField("cpu_usage_" + j, ColumnValue.fromDouble(Math.random() * 100));
            }
            row.addField("index", ColumnValue.fromLong(index));
            TimeseriesTableRow rowInTable = new TimeseriesTableRow(row, tableName);
            rows.add(rowInTable);
            if (Math.random() > 0.9995 || index == rowsCount - 1) {
                Future<TimeseriesWriterResult> future = writer.addTimeseriesRowChangeWithFuture(rows);
                futures.add(future);
                rows.clear();
            }
        }
        System.out.println("Write thread finished.");
        writer.flush();
        printFutureResult(futures);
        System.out.println("=========================================================[Finish]");

    }

    private void printFutureResult(List<Future<TimeseriesWriterResult>> futures) {
        int totalRow = 0;
        System.out.println("time series writer results as follow: ");
        for (int index = 0; index < futures.size(); index++) {
            try {
                TimeseriesWriterResult result = futures.get(index).get();
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
