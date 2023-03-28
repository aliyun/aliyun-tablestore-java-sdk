package com.alicloud.openservices.tablestore.timeserieswriter.mocktest;

import com.alicloud.openservices.tablestore.AsyncTimeseriesClient;
import com.alicloud.openservices.tablestore.DefaultTableStoreTimeseriesWriter;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesKey;
import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesRow;
import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesTableRow;
import com.alicloud.openservices.tablestore.timeserieswriter.callback.TimeseriesRowResult;
import com.alicloud.openservices.tablestore.timeserieswriter.config.TimeseriesWriterConfig;
import com.alicloud.openservices.tablestore.timeserieswriter.enums.TSDispatchMode;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

public class TestTswriter {
    private static final String tableName = "TimeseriesWriterTestParallel";
    private static ServiceSettings serviceSettings = ServiceSettings.load();
    private static long totalTime = 0;
    private volatile boolean stop = false;
    private static AtomicLong succeedRows = new AtomicLong();
    private static AtomicLong failedRows = new AtomicLong();
    private static long rowsCount = 1000000;
    private static AtomicLong rowIndex = new AtomicLong(-1);
    private static long columnsCount = 40;
    private static int concurrency = 4096;
    private static int queueSize = 1024;
    private static int sendThreads = 1;
    private static int writerCount = 1;
    private static int bucketCount = 1;
    private static TSWriteMode writeMode = TSWriteMode.PARALLEL;
    private Executor executor;
    private AsyncTimeseriesClient ots = new MockClient(serviceSettings.getOTSEndpoint(),
            serviceSettings.getOTSAccessKeyId(), serviceSettings.getOTSAccessKeySecret(),
            serviceSettings.getOTSInstanceName());
    final String strValue = "0123456789";
    private static AtomicLong retryCount = new AtomicLong();
    private static ExecutorService threadPool;

    @Before
    public void setUp() throws Exception {
        succeedRows.getAndSet(0);
        failedRows.getAndSet(0);
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("writer-pool-%d").build();
        threadPool = new ThreadPoolExecutor(10, 10, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue(1024), threadFactory, new ThreadPoolExecutor.CallerRunsPolicy());


    }

    @After
    public void after() {
        ots.shutdown();
        threadPool.shutdown();
    }

    public DefaultTableStoreTimeseriesWriter createWriter(TimeseriesWriterConfig config) {
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

        int coreThreadCount = Runtime.getRuntime().availableProcessors() + 1;
        int queueSize = 1024;

        ThreadFactory threadFactory = new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(1);

            public Thread newThread(Runnable r) {
                return new Thread(r, "writer-callback-" + this.counter.getAndIncrement());
            }
        };
        executor = new ThreadPoolExecutor(coreThreadCount, coreThreadCount, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue(queueSize), threadFactory, new ThreadPoolExecutor.CallerRunsPolicy());
        DefaultTableStoreTimeseriesWriter writer = new DefaultTableStoreTimeseriesWriter(
                ots, config, callback, executor);

        return writer;
    }

    @Test
    public void testSimple() throws Exception {
        final TimeseriesWriterConfig config = new TimeseriesWriterConfig();
        config.setConcurrency(concurrency);
        config.setBufferSize(queueSize);
        config.setWriteMode(writeMode);
        config.setBucketCount(bucketCount);
        config.setDispatchMode(TSDispatchMode.ROUND_ROBIN);
//        config.setBufferSize(1024 * 1024);
//        config.setMaxBatchRowsCount(1024 * 1024);
//        config.setMaxBatchSize(1024 * 1024 * 1024);
        config.setFlushInterval(10000);


        List<Thread> threads = new ArrayList<Thread>();
        List<DefaultTableStoreTimeseriesWriter> writers = new ArrayList<DefaultTableStoreTimeseriesWriter>();
        final CountDownLatch latch = new CountDownLatch(writerCount * sendThreads);



        long st0 = System.currentTimeMillis();
        for (int tc = 0; tc < writerCount; tc++) {
            final DefaultTableStoreTimeseriesWriter writer = createWriter(config);
            writers.add(writer);
        }
        long st1 = System.currentTimeMillis();
        for (int tc = 0; tc < writerCount; tc++) {

            final DefaultTableStoreTimeseriesWriter writer = writers.get(tc);
            for (int k = 0; k < sendThreads; k++) {
                final int threadId = k;
                threadPool.execute(new Runnable() {
                    public void run() {

                        for (long index = rowIndex.incrementAndGet(); index < rowsCount; index = rowIndex.incrementAndGet()) {
                            Map<String, String> tags = new HashMap<String, String>();
                            tags.put("region", "hangzhou");
                            tags.put("os", "Ubuntu16.04");
                            TimeseriesKey timeseriesKey = new TimeseriesKey("cpu", "host_", tags);
                            TimeseriesRow row = new TimeseriesRow(timeseriesKey, System.currentTimeMillis() * 1000 + index);
                            for (int j = 0; j < columnsCount; j++) {
                                row.addField("column_" + j, ColumnValue.fromString(strValue));
                            }


                            row.addField("thread_" + threadId, ColumnValue.fromLong(threadId));
                            writer.addTimeseriesRowChange(new TimeseriesTableRow(row, tableName));
//                            if(index % 100000 == 0){
//                                int queueSize = ((ThreadPoolExecutor) executor).getQueue().size();
//                                System.out.println("当前排队线程数：" + queueSize);
//
//                                int activeCount = ((ThreadPoolExecutor) executor).getActiveCount();
//                                System.out.println("当前活动线程数：" + activeCount);
//
//                                long completedTaskCount = ((ThreadPoolExecutor) executor).getCompletedTaskCount();
//                                System.out.println("执行完成线程数：" + completedTaskCount);
//
//                                long taskCount = ((ThreadPoolExecutor) executor).getTaskCount();
//                                System.out.println("总线程数：" + taskCount);
//                            }
                        }
                        // System.out.println("Write thread finished.");
                        latch.countDown();
                    }
                });
            }
        }

        latch.await();
        long st2 = System.currentTimeMillis();
        for (DefaultTableStoreTimeseriesWriter writer : writers) {
            writer.flush();
            System.out.println(writer.getTimeseriesWriterStatistics());
            writer.close();
        }
        long et = System.currentTimeMillis();
        totalTime = et - st0;
        stop = true;
        //scanTable(ots);

        System.out.println("TotalTime: " + totalTime);
        System.out.println("buildTime: " + (st1 - st0));
        System.out.println("WriteTime: " + (st2 - st1));
        System.out.println("FlushTime: " + (et - st2));
        System.out.println("Write succeed rows: " + succeedRows.get());
        System.out.println("Write failed rows: " + failedRows.get());
        System.out.println("Retry count: " + retryCount.get());
        double qps = 1.0 * (succeedRows.get() + failedRows.get()) / (totalTime / 1000.0);
        System.out.println("QPS: " + qps);

        // check value

        long totalRPCCount = succeedRows.get() + failedRows.get();
        assertEquals(totalRPCCount, rowsCount);
    }


}
