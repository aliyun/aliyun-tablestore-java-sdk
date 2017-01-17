package com.aliyun.openservices.ots.writer;

import com.aliyun.openservices.ots.*;
import com.aliyun.openservices.ots.internal.OTSCallback;
import com.aliyun.openservices.ots.internal.OTSRetryStrategy;
import com.aliyun.openservices.ots.internal.writer.WriterConfig;
import com.aliyun.openservices.ots.model.*;
import com.aliyun.openservices.ots.utils.ServiceSettings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestWriterPerf {

    private static final String tableName = "WriterTest";
    private static ServiceSettings serviceSettings = ServiceSettings.load();
    private static long totalTime = 0;
    private volatile boolean stop = false;
    private static AtomicLong succeedRows = new AtomicLong();
    private static AtomicLong failedRows = new AtomicLong();
    private static long rowsCount = 1000000;
    private static long columnsCount = 10;
    private static int concurrency = 100;
    private static int queueSize = 4096;
    private static int sendThreads = 1;
    private static int writerCount = 1;
    private OTSAsync ots;
    final String strValue = "0123456789";
    private static AtomicLong retryCount = new AtomicLong();

    public void createTable(OTSAsync ots) {
        CreateTableRequest request = new CreateTableRequest();
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("pk0", PrimaryKeyType.INTEGER);
        tableMeta.addPrimaryKeyColumn("pk1", PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn("pk2", PrimaryKeyType.INTEGER);
        request.setTableMeta(tableMeta);
        request.setReservedThroughput(new CapacityUnit(5000000, 5000000));

        OTSFuture<CreateTableResult> future = ots.createTable(request);
        future.get();
    }

    @Before
    public void setUp() throws Exception {
        ClientConfiguration cc = new ClientConfiguration();
        cc.setMaxConnections(1000);
        OTSServiceConfiguration ss = new OTSServiceConfiguration();
        ss.setRetryStrategy(new OTSRetryStrategy() {
            @Override
            public boolean shouldRetry(String action, Exception ex, int retries) {
                if (action.equals(OTSActionNames.ACTION_DELETE_TABLE)) {
                    return false;
                }
                retryCount.incrementAndGet();
                return true;
            }

            @Override
            public long getPauseDelay(String action, Exception ex, int retries) {
                return 10;
            }
        });
        ots = new OTSClientAsync(serviceSettings.getOTSEndpoint(),
                serviceSettings.getOTSAccessKeyId(), serviceSettings.getOTSAccessKeySecret(),
                serviceSettings.getOTSInstanceName(), cc, ss);

        try {
            DeleteTableRequest request = new DeleteTableRequest();
            request.setTableName(tableName);
            OTSFuture<DeleteTableResult> future = ots.deleteTable(request);
            future.get();
        } catch (OTSException e) {
            // pass
        }
        createTable(ots);
        Thread.sleep(3000);
    }

    @After
    public void after() {
        ots.shutdown();
    }

    public static DefaultOTSWriter createWriter(OTSAsync ots, WriterConfig config, Executor executor) {
        OTSCallback<RowChange, ConsumedCapacity> callback = new OTSCallback<RowChange, ConsumedCapacity>() {
            @Override
            public void onCompleted(OTSContext<RowChange, ConsumedCapacity> otsContext) {
                succeedRows.incrementAndGet();
            }

            @Override
            public void onFailed(OTSContext<RowChange, ConsumedCapacity> otsContext, OTSException ex) {
                failedRows.incrementAndGet();
            }

            @Override
            public void onFailed(OTSContext<RowChange, ConsumedCapacity> otsContext, ClientException ex) {
                failedRows.incrementAndGet();
            }
        };
        DefaultOTSWriter writer = new DefaultOTSWriter(ots, tableName, config, callback, executor);

        return writer;
    }

    @Test
    public void testSimple() throws Exception {
        final WriterConfig config = new WriterConfig();
        config.setConcurrency(concurrency);
        config.setBufferSize(queueSize);
        config.setFlushInterval(1000000);
        final ExecutorService executor = Executors.newFixedThreadPool(10);

        long st = System.currentTimeMillis();
        List<Thread> threads = new ArrayList<Thread>();
        List<DefaultOTSWriter> writers = new ArrayList<DefaultOTSWriter>();
        for (int tc = 0; tc < writerCount; tc++) {
            final DefaultOTSWriter writer = createWriter(ots, config, executor);
            writers.add(writer);
            for (int k = 0; k < sendThreads; k++) {
                final int threadId = k;
                Thread th = new Thread(new Runnable() {
                    public void run() {

                        for (int i = 0; i < rowsCount; i++) {
                            RowUpdateChange rowChange = new RowUpdateChange(tableName);
                            RowPrimaryKey pk = new RowPrimaryKey();
                            pk.addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(i)).addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("pk_" + i))
                                    .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(i));

                            rowChange.setPrimaryKey(pk);
                            for (int j = 0; j < columnsCount; j++) {
                                rowChange.addAttributeColumn("column_" + j, ColumnValue.fromString(strValue));
                            }

                            rowChange.addAttributeColumn("thread_" + threadId, ColumnValue.fromLong(threadId));
                            writer.addRowChange(rowChange);
                        }
                        System.out.println("Write thread finished.");
                    }
                });
                threads.add(th);
            }
        }

        for (Thread th : threads) {
            th.start();
        }

        for (Thread th : threads) {
            th.join();
        }

        for (DefaultOTSWriter writer : writers) {
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
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        // check value
        System.out.println("Start check values...");
        long totalRPCCount = succeedRows.get() + failedRows.get();
        assertEquals(totalRPCCount, sendThreads * rowsCount * writerCount);
        scanTable(ots);
        System.out.println("PASS!");
    }

    private void scanTable(OTSAsync ots) {
        System.out.println("#################### Begin scan table ####################");
        int rowsCountInTable = 0;
        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
        RowPrimaryKey start = new RowPrimaryKey();
        start.addPrimaryKeyColumn("pk0", PrimaryKeyValue.INF_MIN).addPrimaryKeyColumn("pk1", PrimaryKeyValue.INF_MIN)
                .addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
        RowPrimaryKey end = new RowPrimaryKey();
        end.addPrimaryKeyColumn("pk0", PrimaryKeyValue.INF_MAX).addPrimaryKeyColumn("pk1", PrimaryKeyValue.INF_MAX)
                .addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MAX);
        criteria.setInclusiveStartPrimaryKey(start);
        criteria.setExclusiveEndPrimaryKey(end);

        GetRangeRequest request = new GetRangeRequest();
        request.setRangeRowQueryCriteria(criteria);

        RowPrimaryKey nextStart = null;
        do {
            OTSFuture<GetRangeResult> future = ots.getRange(request);
            GetRangeResult result = future.get();
            for (Row row : result.getRows()) {
                ColumnValue pk0 = row.getColumns().get("pk0");
                ColumnValue pk1 = row.getColumns().get("pk1");
                ColumnValue pk2 = row.getColumns().get("pk2");
                assertEquals(pk0.asLong(), rowsCountInTable);
                assertEquals(pk1.asString(), "pk_" + rowsCountInTable);
                assertEquals(pk2.asLong(), rowsCountInTable);

                for (int i = 0; i < columnsCount; i++) {
                    ColumnValue c = row.getColumns().get("column_" + i);
                    assertTrue(c != null);
                    assertEquals(c.asString(), strValue);
                }

                for (int i = 0; i < sendThreads; i++) {
                    ColumnValue c = row.getColumns().get("thread_" + i);
                    assertTrue(c != null);
                    assertEquals(c.asLong(), i);
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
        if (args.length < 6) {
            System.out.println("Arguments not enough.");
            System.exit(-1);
        }

        rowsCount = Long.parseLong(args[0]);
        columnsCount = Long.parseLong(args[1]);
        concurrency = Integer.parseInt(args[2]);
        queueSize = Integer.parseInt(args[3]);
        sendThreads = Integer.parseInt(args[4]);
        writerCount = Integer.parseInt(args[5]);
        System.out.println("RowsCount: " + rowsCount);
        System.out.println("ColumnsCount: " + columnsCount);
        System.out.println("Concurrency: " + concurrency);
        System.out.println("QueueSize: " + queueSize);
        System.out.println("SendThreads: " + sendThreads);
        System.out.println("WriterCount: " + writerCount);

        TestWriterPerf tw = new TestWriterPerf();
        tw.setUp();
        tw.testSimple();
        tw.after();
    }
}
