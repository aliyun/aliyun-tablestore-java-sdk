package examples;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.AsyncClientInterface;
import com.alicloud.openservices.tablestore.DefaultTableStoreReader;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.TableStoreReader;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.RowQueryCriteria;
import com.alicloud.openservices.tablestore.reader.PrimaryKeyWithTable;
import com.alicloud.openservices.tablestore.reader.ReaderResult;
import com.alicloud.openservices.tablestore.reader.RowReadResult;
import com.alicloud.openservices.tablestore.reader.TableStoreReaderConfig;

public class ReaderSample {
    private static final String endpoint = "https://xxx.ots.aliyuncs.com";
    private static final String accessKeyId = "";
    private static final String accessKeySecret = "";
    private static final String instanceName = "";
    private static AsyncClientInterface client;
    private static ExecutorService executorService;
    private static AtomicLong succeedRows = new AtomicLong();
    private static AtomicLong failedRows = new AtomicLong();

    public static void main(String[] args) throws InterruptedException, ExecutionException {

        TableStoreReader reader = createReader();

        // Set the query columns of table 'testTableStoreReaderTable' to col1, and the maximum query version to 10.
        // If the table being queried does not set RowQueryCriteria, it will default to querying all attribute columns, and the maximum number of versions queried will be 1.
        RowQueryCriteria criteria = new RowQueryCriteria("testTableStoreReaderTable");
        criteria.addColumnsToGet("col1");
        criteria.setMaxVersions(10);
        reader.setRowQueryCriteria(criteria);

        // Add a column of data to be queried to the memory
        PrimaryKey pk1 = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(0))
                .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(0))
                .build();
        // Query the property column of pk1 in the testTableStoreReaderTable table
        reader.addPrimaryKey("testTableStoreReaderTable", pk1);

        // Asynchronously send the data in memory
        reader.send();

        // Add a column of data to be queried to the memory and get the query result Future
        PrimaryKey pk2 = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(0))
                .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(0))
                .build();

        // Query the attribute columns of pk2 in the table anotherTable
        Future<ReaderResult> readerResult = reader.addPrimaryKeyWithFuture("anotherTable", pk2);

        // Synchronously send the data in memory
        reader.flush();

        // Wait for the callback function to complete processing
        Thread.sleep(1000);

        System.out.println("result:" + readerResult.get().toString());
        System.out.println("succeed rows count:" + succeedRows.get());
        System.out.println("failed rows count:" + failedRows.get());

        reader.close();
        client.shutdown();
        executorService.shutdown();
    }

    public static TableStoreReader createReader() {
        // Construct AsyncClient
        client = new AsyncClient(endpoint, accessKeyId, accessKeySecret, instanceName);

        // Construct the reader configuration class
        TableStoreReaderConfig config = new TableStoreReaderConfig();
        {
            // All the following parameters have default values and do not need to be configured.
            config.setCheckTableMeta(true);                 // Before adding data to the reader, it will first check the table structure.
            config.setMaxBatchRowsCount(100);               // The maximum number of rows that can be requested in a single request, with an upper limit of 100.
            config.setDefaultMaxVersions(1);                // By default, the maximum number of versions for the retrieved columns.
            config.setConcurrency(16);                      // Total number of concurrent requests sent
            config.setBucketCount(4);                       // Number of memory buckets
            config.setFlushInterval(10000);                 // Interval for sending all cached data
            config.setLogInterval(10000);                   // Time interval for logging the reader status
        }

        // Construct the executor for sending requests
        ThreadFactory threadFactory = new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "reader-" + counter.getAndIncrement());
            }
        };
        executorService = new ThreadPoolExecutor(4, 4, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue(1024), threadFactory, new ThreadPoolExecutor.CallerRunsPolicy());

        // Construct the callback function for the reader
        TableStoreCallback<PrimaryKeyWithTable, RowReadResult> callback = new TableStoreCallback<PrimaryKeyWithTable, RowReadResult>() {
            @Override
            public void onCompleted(PrimaryKeyWithTable req, RowReadResult res) {
                succeedRows.incrementAndGet();
                System.out.println(res.getRowResult());
            }

            @Override
            public void onFailed(PrimaryKeyWithTable req, Exception ex) {
                failedRows.incrementAndGet();
            }
        };
        TableStoreReader reader = new DefaultTableStoreReader(client, config, executorService, callback);
        return reader;
    }
}
