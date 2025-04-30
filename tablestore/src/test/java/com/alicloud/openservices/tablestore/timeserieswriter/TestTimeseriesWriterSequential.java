package com.alicloud.openservices.tablestore.timeserieswriter;

import com.alicloud.openservices.tablestore.AsyncTimeseriesClient;
import com.alicloud.openservices.tablestore.DefaultTableStoreTimeseriesWriter;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.core.auth.DefaultCredentials;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.TimeseriesTableMeta;
import com.alicloud.openservices.tablestore.model.TimeseriesTableOptions;
import com.alicloud.openservices.tablestore.model.timeseries.*;
import com.alicloud.openservices.tablestore.timeserieswriter.callback.TimeseriesRowResult;
import com.alicloud.openservices.tablestore.timeserieswriter.config.TimeseriesWriterConfig;
import com.alicloud.openservices.tablestore.timeserieswriter.enums.TSWriteMode;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

public class TestTimeseriesWriterSequential {
    private static final String tableName = "TimeseriesWriterTestSequential";
    private static ServiceSettings serviceSettings = ServiceSettings.load();
    private static long totalTime = 0;
    private volatile boolean stop = false;
    private static AtomicLong succeedRows = new AtomicLong();
    private static AtomicLong failedRows = new AtomicLong();
    private static long rowsCount = 100000;
    private static AtomicLong rowIndex = new AtomicLong(-1);
    private static long columnsCount = 10;
    private static int concurrency = 64;
    private static int queueSize = 4096;
    private static int sendThreads = 1;
    private static int writerCount = 1;
    private static int bucketCount = 8;
    private static TSWriteMode writeMode = TSWriteMode.SEQUENTIAL;
    private AsyncTimeseriesClient ots;
    final String strValue = "0123456789";
    private static AtomicLong retryCount = new AtomicLong();
    private static ExecutorService threadPool;

    public void createTimeseriesTable(AsyncTimeseriesClient client) throws Exception {
        TimeseriesTableMeta timeseriesTableMeta = new TimeseriesTableMeta(tableName);
        int timeToLive = -1;
        timeseriesTableMeta.setTimeseriesTableOptions(new TimeseriesTableOptions(timeToLive));
        CreateTimeseriesTableRequest request = new CreateTimeseriesTableRequest(timeseriesTableMeta);
        Future<CreateTimeseriesTableResponse> future = client.createTimeseriesTable(request, null);
        future.get();
    }

    @Before
    public void setUp() throws Exception {
        succeedRows.getAndSet(0);
        failedRows.getAndSet(0);
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("writer-pool-%d").build();
        threadPool = new ThreadPoolExecutor(10, 10, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue(1024), threadFactory, new ThreadPoolExecutor.CallerRunsPolicy());

        ots = new AsyncTimeseriesClient(serviceSettings.getOTSEndpoint(),
                serviceSettings.getOTSAccessKeyId(), serviceSettings.getOTSAccessKeySecret(),
                serviceSettings.getOTSInstanceName());

        try {
            DeleteTimeseriesTableRequest request = new DeleteTimeseriesTableRequest(tableName);
            Future<DeleteTimeseriesTableResponse> future = ots.deleteTimeseriesTable(request, null);
            future.get();
        } catch (Exception e) {
            // pass
        }

        createTimeseriesTable(ots);
        System.out.println("waiting...");
        Thread.sleep(30000);
        System.out.println("stop waiting");

    }

    @After
    public void after() {
        ots.shutdown();
        threadPool.shutdown();
    }

    public static DefaultTableStoreTimeseriesWriter createWriter(TimeseriesWriterConfig config) {
        TableStoreCallback<TimeseriesTableRow, TimeseriesRowResult> callback = new TableStoreCallback<TimeseriesTableRow, TimeseriesRowResult>() {
            @Override
            public void onCompleted(TimeseriesTableRow rowChange, TimeseriesRowResult cc) {
                succeedRows.incrementAndGet();
            }

            @Override
            public void onFailed(TimeseriesTableRow rowChange, Exception ex) {
                ex.printStackTrace();
                failedRows.incrementAndGet();
            }
        };
        DefaultCredentials credentials = new DefaultCredentials(serviceSettings.getOTSAccessKeyId(), serviceSettings.getOTSAccessKeySecret());

        DefaultTableStoreTimeseriesWriter writer = new DefaultTableStoreTimeseriesWriter(
                serviceSettings.getOTSEndpoint(),
                credentials,
                serviceSettings.getOTSInstanceNameInternal(),
                 config, callback);

        return writer;
    }

    @Test
    public void testSimple() throws Exception {
        final TimeseriesWriterConfig config = new TimeseriesWriterConfig();
        config.setConcurrency(concurrency);
        config.setBufferSize(queueSize);
        config.setWriteMode(writeMode);
        config.setBucketCount(bucketCount);
        config.setFlushInterval(10000);

        long st = System.currentTimeMillis();
        List<Thread> threads = new ArrayList<Thread>();
        List<DefaultTableStoreTimeseriesWriter> writers = new ArrayList<DefaultTableStoreTimeseriesWriter>();
        final CountDownLatch latch = new CountDownLatch(writerCount * sendThreads);
        for (int tc = 0; tc < writerCount; tc++) {
            final DefaultTableStoreTimeseriesWriter writer = createWriter(config);
            writers.add(writer);
            for (int k = 0; k < sendThreads; k++) {
                final int threadId = k;
                threadPool.execute(new Runnable() {
                    public void run() {

                        for (long index = rowIndex.incrementAndGet(); index < rowsCount; index = rowIndex.incrementAndGet()) {
                            Map<String, String> tags = new HashMap<String, String>();
                            tags.put("region", "hangzhou");
                            tags.put("os", "Ubuntu16.04");
                            TimeseriesKey timeseriesKey = new TimeseriesKey("cpu" + index, "host_" + index, tags);
                            TimeseriesRow row = new TimeseriesRow(timeseriesKey, System.currentTimeMillis() * 1000 + index);
                            for (int j = 0; j < columnsCount; j++) {
                                row.addField("column_" + j, ColumnValue.fromString(strValue));
                            }


                            row.addField("thread_" + threadId, ColumnValue.fromLong(threadId));
                            writer.addTimeseriesRowChange(new TimeseriesTableRow(row, tableName));
                        }
                        // System.out.println("Write thread finished.");
                        latch.countDown();
                    }
                });
            }
        }

        latch.await();
        for (DefaultTableStoreTimeseriesWriter writer : writers) {
            writer.flush();
            writer.close();
        }

        long et = System.currentTimeMillis();
        totalTime = et - st;
        stop = true;
        //scanTable(ots);

        System.out.println("TotalTime: " + totalTime);
        System.out.println("Write succeed rows: " + succeedRows.get());
        System.out.println("Write failed rows: " + failedRows.get());
        System.out.println("Retry count: " + retryCount.get());
        double qps = 1.0 * (succeedRows.get() + failedRows.get()) / (totalTime / 1000.0);
        System.out.println("QPS: " + qps);

        // check value
        System.out.println("Start check values...");
        long totalRPCCount = succeedRows.get() + failedRows.get();
        assertEquals(totalRPCCount, rowsCount);
    }
}
