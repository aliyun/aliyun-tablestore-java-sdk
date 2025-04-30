package com.alicloud.openservices.tablestore.writer;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.core.auth.DefaultCredentials;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.writer.enums.WriteMode;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestWriterSequential {

    private static final String TABLE_NAME = "WriterTestSequential";
    private static ServiceSettings serviceSettings = ServiceSettings.load();
    private static long totalTime = 0;
    private volatile boolean stop = false;
    private static AtomicLong succeedRows = new AtomicLong();
    private static AtomicLong failedRows = new AtomicLong();
    private static long rowsCount = 100000;
    private static AtomicLong rowIndex = new AtomicLong(-1);
    private static long columnsCount = 10;
    private static int concurrency = 100;
    private static int queueSize = 4096;
    private static int sendThreads = 1;
    private static int writerCount = 1;
    private static int bucketCount = 100; // writerConfig default value
    private static WriteMode writeMode = WriteMode.SEQUENTIAL;
    private AsyncClientInterface ots;
    final String strValue = "0123456789";
    private static AtomicLong retryCount = new AtomicLong();
    private static ExecutorService threadPool;

    public void createTable(AsyncClientInterface ots) {
        TableMeta tableMeta = new TableMeta(TABLE_NAME);
        tableMeta.addPrimaryKeyColumn("pk0", PrimaryKeyType.INTEGER);
        tableMeta.addPrimaryKeyColumn("pk1", PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn("pk2", PrimaryKeyType.INTEGER);
        TableOptions tableOptions = new TableOptions(-1, 1);
        CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions, new ReservedThroughput(new CapacityUnit(0, 0)));
        request.setReservedThroughput(new ReservedThroughput(new CapacityUnit(0, 0)));

        Future<CreateTableResponse> future = ots.createTable(request, null);
        try {
            future.get();
        } catch (Exception e) {
            throw new ClientException(e);
        }
    }

    @Before
    public void setUp() throws Exception {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("writer-pool-%d").build();
        threadPool = new ThreadPoolExecutor(10, 10, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue(1024), threadFactory, new ThreadPoolExecutor.CallerRunsPolicy());


        ots = new AsyncClient(serviceSettings.getOTSEndpoint(),
                serviceSettings.getOTSAccessKeyId(), serviceSettings.getOTSAccessKeySecret(),
                serviceSettings.getOTSInstanceName());

        try {
            DeleteTableRequest request = new DeleteTableRequest(TABLE_NAME);
            Future<DeleteTableResponse> future = ots.deleteTable(request, null);
            future.get();
        } catch (Exception e) {
            // pass
        }
        createTable(ots);
        Thread.sleep(3000);
    }

    @After
    public void after() {
        ots.shutdown();
        threadPool.shutdown();
    }

    public static DefaultTableStoreWriter createWriter(WriterConfig config) {
        TableStoreCallback<RowChange, RowWriteResult> callback = new TableStoreCallback<RowChange, RowWriteResult>() {
            @Override
            public void onCompleted(RowChange rowChange, RowWriteResult cc) {
                succeedRows.incrementAndGet();
            }

            @Override
            public void onFailed(RowChange rowChange, Exception ex) {
                ex.printStackTrace();
                failedRows.incrementAndGet();
            }
        };

        DefaultCredentials credentials = new DefaultCredentials(serviceSettings.getOTSAccessKeyId(), serviceSettings.getOTSAccessKeySecret());
        DefaultTableStoreWriter writer = new DefaultTableStoreWriter(
                serviceSettings.getOTSEndpoint(),
                credentials,
                serviceSettings.getOTSInstanceName(),
                TABLE_NAME, config, callback);

        return writer;
    }

    @Test
    public void testSimple() throws Exception {
        final WriterConfig config = new WriterConfig();
        config.setConcurrency(concurrency);
        config.setBufferSize(queueSize);
        config.setWriteMode(writeMode);
        config.setBucketCount(bucketCount);
        config.setFlushInterval(10000);

        long st = System.currentTimeMillis();
        List<Thread> threads = new ArrayList<Thread>();
        List<DefaultTableStoreWriter> writers = new ArrayList<DefaultTableStoreWriter>();
        final CountDownLatch latch = new CountDownLatch(writerCount * sendThreads);
        for (int tc = 0; tc < writerCount; tc++) {
            final DefaultTableStoreWriter writer = createWriter(config);
            writers.add(writer);
            for (int k = 0; k < sendThreads; k++) {
                final int threadId = k;
                threadPool.execute(new Runnable() {
                    public void run() {

                        for (long index = rowIndex.incrementAndGet(); index < rowsCount; index = rowIndex.incrementAndGet()) {
                            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                                    .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(index))
                                    .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("pk_" + index))
                                    .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(index))
                                    .build();

                            RowUpdateChange rowChange = new RowUpdateChange(TABLE_NAME, pk);
                            for (int j = 0; j < columnsCount; j++) {
                                rowChange.put("column_" + j, ColumnValue.fromString(strValue));
                            }

                            rowChange.put("thread_" + threadId, ColumnValue.fromLong(threadId));
                            writer.addRowChange(rowChange);
                        }
                        System.out.println("Write thread finished.");
                        latch.countDown();
                    }
                });
            }
        }

        latch.await();
        for (DefaultTableStoreWriter writer : writers) {
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
        scanTable(ots);
        System.out.println("PASS!");
    }

    private void scanTable(AsyncClientInterface ots) throws ExecutionException, InterruptedException {
        System.out.println("#################### Begin scan table ####################");
        int rowsCountInTable = 0;
        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(TABLE_NAME);
        PrimaryKey start = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk0", PrimaryKeyValue.INF_MIN)
                .addPrimaryKeyColumn("pk1", PrimaryKeyValue.INF_MIN)
                .addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN)
                .build();
        PrimaryKey end = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk0", PrimaryKeyValue.INF_MAX)
                .addPrimaryKeyColumn("pk1", PrimaryKeyValue.INF_MAX)
                .addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MAX)
                .build();

        criteria.setInclusiveStartPrimaryKey(start);
        criteria.setExclusiveEndPrimaryKey(end);
        criteria.setMaxVersions(1);

        GetRangeRequest request = new GetRangeRequest();
        request.setRangeRowQueryCriteria(criteria);

        PrimaryKey nextStart = null;
        do {
            Future<GetRangeResponse> future = ots.getRange(request, null);
            GetRangeResponse result = future.get();
            for (Row row : result.getRows()) {
                PrimaryKeyValue pk0 = row.getPrimaryKey().getPrimaryKeyColumn("pk0").getValue();
                PrimaryKeyValue pk1 = row.getPrimaryKey().getPrimaryKeyColumn("pk1").getValue();
                PrimaryKeyValue pk2 = row.getPrimaryKey().getPrimaryKeyColumn("pk2").getValue();
                assertEquals(pk0.asLong(), rowsCountInTable);
                assertEquals(pk1.asString(), "pk_" + rowsCountInTable);
                assertEquals(pk2.asLong(), rowsCountInTable);

                for (int i = 0; i < columnsCount; i++) {
                    ColumnValue c = row.getLatestColumn("column_" + i).getValue();
                    assertTrue(c != null);
                    assertEquals(c.asString(), strValue);
                }

                rowsCountInTable++;
            }

            nextStart = result.getNextStartPrimaryKey();

            if (nextStart != null) {
                criteria.setInclusiveStartPrimaryKey(nextStart);
            }
        } while (nextStart != null);

        assertEquals(rowsCountInTable, rowsCount);
        System.out.println("TotalRowsCount: " + rowsCountInTable);
    }

    public static void main(String[] args) throws Exception {
        if (args.length >= 6) {
            rowsCount = Long.parseLong(args[0]);
            columnsCount = Long.parseLong(args[1]);
            concurrency = Integer.parseInt(args[2]);
            queueSize = Integer.parseInt(args[3]);
            sendThreads = Integer.parseInt(args[4]);
            writerCount = Integer.parseInt(args[5]);
        }


        System.out.println("RowsCount: " + rowsCount);
        System.out.println("ColumnsCount: " + columnsCount);
        System.out.println("Concurrency: " + concurrency);
        System.out.println("QueueSize: " + queueSize);
        System.out.println("SendThreads: " + sendThreads);
        System.out.println("WriterCount: " + writerCount);
        System.out.println("WriterMode: " + writeMode);
        System.out.println("BucketCount: " + bucketCount);

        TestWriterSequential tw = new TestWriterSequential();
        tw.setUp();
        tw.testSimple();
        tw.after();
    }
}
