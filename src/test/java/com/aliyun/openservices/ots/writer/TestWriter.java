package com.aliyun.openservices.ots.writer;

import com.aliyun.openservices.ots.*;
import com.aliyun.openservices.ots.internal.OTSAlwaysRetryStrategy;
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
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestWriter {
    private static final String tableName = "WriterTest";
    private static ServiceSettings serviceSettings = ServiceSettings.load();
    private static AtomicLong succeedRows = new AtomicLong();
    private static AtomicLong failedRows = new AtomicLong();
    private static long rowsCount = 100000;
    private static long columnsCount = 10;
    private static int concurrency = 100;
    private static int queueSize = 4096;
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
        request.setReservedThroughput(new CapacityUnit(0, 0));

        OTSFuture<CreateTableResult> future = ots.createTable(request);
        future.get();
    }

    @Before
    public void setUp() throws Exception {
        succeedRows.getAndSet(0);
        failedRows.getAndSet(0);

        ClientConfiguration cc = new ClientConfiguration();
        cc.setMaxConnections(1000);
        OTSServiceConfiguration ss = new OTSServiceConfiguration();
        ss.setRetryStrategy(new OTSRetryStrategy() {
            @Override
            public boolean shouldRetry(String action, Exception ex, int retries) {
                if (action.equals(OTSActionNames.ACTION_DELETE_TABLE)) {
                    return false;
                }

                if (ex instanceof OTSException) {
                    OTSException otsException = (OTSException) ex;
                    if (otsException.getErrorCode().equals(OTSErrorCode.INVALID_PARAMETER)) {
                        return false;
                    }
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
    public void testWriteSameRow() throws Exception {
        final WriterConfig config = new WriterConfig();
        config.setConcurrency(concurrency);
        config.setBufferSize(queueSize);
        config.setFlushInterval(1000000);
        final ExecutorService executor = Executors.newFixedThreadPool(10);

        DefaultOTSWriter writer = createWriter(ots, config, executor);

        try {
            for (int i = 0; i < 100; i++) {
                RowUpdateChange rowChange = new RowUpdateChange(tableName);
                RowPrimaryKey pk = new RowPrimaryKey();
                pk.addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(0)).addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("pk_" + 0))
                        .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(0));

                rowChange.setPrimaryKey(pk);
                for (int j = 0; j < columnsCount; j++) {
                    rowChange.addAttributeColumn("column_" + j, ColumnValue.fromString(strValue));
                }

                writer.addRowChange(rowChange);
            }

            writer.flush();

        } finally {
            writer.close();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        assertEquals(succeedRows.get(), 100);
        assertEquals(writer.getTotalRPCCount(), 100);
    }

    @Test
    public void testMixOperation() throws Exception {
        final WriterConfig config = new WriterConfig();
        config.setConcurrency(concurrency);
        config.setBufferSize(queueSize);
        config.setFlushInterval(1000000);
        final ExecutorService executor = Executors.newFixedThreadPool(10);

        final DefaultOTSWriter writer = createWriter(ots, config, executor);

        final int threadsCount = 5;
        List<Thread> threads = new ArrayList<Thread>();
        try {
            for (int i = 0; i < threadsCount; i++) {
                final int finalI = i;
                Thread th = new Thread(new Runnable() {
                    public void run() {
                        Random random = new Random(System.currentTimeMillis() + finalI * 1000);
                        for (int i = 0; i < rowsCount; i++) {
                            int id = Math.abs(random.nextInt()) % 2;
                            if (id == 0) {
                                RowUpdateChange rowChange = new RowUpdateChange(tableName);
                                RowPrimaryKey pk = new RowPrimaryKey();
                                pk.addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(i)).addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("pk_" + i))
                                        .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(i));

                                rowChange.setPrimaryKey(pk);
                                for (int j = 0; j < columnsCount; j++) {
                                    rowChange.addAttributeColumn("update_" + j, ColumnValue.fromString(strValue));
                                }

                                writer.addRowChange(rowChange);
                            } else {
                                RowPutChange rowChange = new RowPutChange(tableName);
                                RowPrimaryKey pk = new RowPrimaryKey();
                                pk.addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(i)).addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("pk_" + i))
                                        .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(i));

                                rowChange.setPrimaryKey(pk);
                                for (int j = 0; j < columnsCount; j++) {
                                    rowChange.addAttributeColumn("put_" + j, ColumnValue.fromString(strValue));
                                }

                                writer.addRowChange(rowChange);
                            }
                        }
                        System.out.println("Write thread finished.");
                    }
                });
                threads.add(th);
            }

            for (Thread thread : threads) {
                thread.start();
            }

            for (Thread thread : threads) {
                thread.join();
            }

            writer.flush();

            assertEquals(succeedRows.get(), threadsCount * rowsCount);
            scanTable(ots);
        } finally {
            writer.close();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    private void scanTable(OTSAsync ots) {
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

                if (row.getColumns().containsKey("put_0")) {
                    for (int i = 0; i < columnsCount; i++) {
                        ColumnValue c = row.getColumns().get("put_" + i);
                        assertTrue(c != null);
                        assertEquals(c.asString(), strValue);
                    }
                } else if (row.getColumns().containsKey("update_0")) {
                    for (int i = 0; i < columnsCount; i++) {
                        ColumnValue c = row.getColumns().get("update_" + i);
                        assertTrue(c != null);
                        assertEquals(c.asString(), strValue);
                    }
                }

                rowsCountInTable++;
            }

            nextStart = result.getNextStartPrimaryKey();

            if (nextStart != null) {
                criteria.setInclusiveStartPrimaryKey(nextStart);
            }
        } while (nextStart != null);

        assertEquals(rowsCountInTable, rowsCount);
    }

    @Test
    public void testWriteDirtyRows() throws Exception {
        final WriterConfig config = new WriterConfig();
        config.setConcurrency(concurrency);
        config.setBufferSize(queueSize);
        config.setFlushInterval(1000000);
        config.setMaxAttrColumnSize(3 * 1024 * 1024); // max attribute length set to 100KB
        config.setMaxBatchSize(4 * 1024 * 1024);
        StringBuilder longStr = new StringBuilder();
        for (int i = 0; i < 2 * 1024 * 1024 + 1; i++) {
            longStr.append('a');
        }
        final ExecutorService executor = Executors.newFixedThreadPool(10);

        DefaultOTSWriter writer = createWriter(ots, config, executor);

        int dirtyRowId = 25;
        try {
            for (int i = 0; i < 100; i++) {
                RowUpdateChange rowChange = new RowUpdateChange(tableName);
                RowPrimaryKey pk = new RowPrimaryKey();
                pk.addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(i)).addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("pk_" + i))
                        .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(i));

                rowChange.setPrimaryKey(pk);
                for (int j = 0; j < columnsCount; j++) {
                    rowChange.addAttributeColumn("column_" + j, ColumnValue.fromString(strValue));
                }

                if (i == dirtyRowId) {
                    rowChange.addAttributeColumn("longattr", ColumnValue.fromString(longStr.toString()));
                }

                writer.addRowChange(rowChange);
            }

            writer.flush();

        } finally {
            writer.close();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        assertEquals(succeedRows.get(), 99);
        assertEquals(failedRows.get(), 1);

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
                if (rowsCountInTable == dirtyRowId) {
                    rowsCountInTable++;
                }
                ColumnValue pk0 = row.getColumns().get("pk0");
                ColumnValue pk1 = row.getColumns().get("pk1");
                ColumnValue pk2 = row.getColumns().get("pk2");
                assertEquals(pk0.asLong(), rowsCountInTable);
                assertEquals(pk1.asString(), "pk_" + rowsCountInTable);
                assertEquals(pk2.asLong(), rowsCountInTable);

                if (row.getColumns().containsKey("put_0")) {
                    for (int i = 0; i < columnsCount; i++) {
                        ColumnValue c = row.getColumns().get("put_" + i);
                        assertTrue(c != null);
                        assertEquals(c.asString(), strValue);
                    }
                } else if (row.getColumns().containsKey("update_0")) {
                    for (int i = 0; i < columnsCount; i++) {
                        ColumnValue c = row.getColumns().get("update_" + i);
                        assertTrue(c != null);
                        assertEquals(c.asString(), strValue);
                    }
                }

                rowsCountInTable++;
            }

            nextStart = result.getNextStartPrimaryKey();

            if (nextStart != null) {
                criteria.setInclusiveStartPrimaryKey(nextStart);
            }
        } while (nextStart != null);

        assertEquals(rowsCountInTable, 100);
    }
}
