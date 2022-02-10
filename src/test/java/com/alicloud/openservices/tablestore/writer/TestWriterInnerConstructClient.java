package com.alicloud.openservices.tablestore.writer;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.core.auth.DefaultCredentials;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.writer.enums.BatchRequestType;
import com.alicloud.openservices.tablestore.writer.enums.WriteMode;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

public class TestWriterInnerConstructClient {
    private static final String TABLE_NAME = "WriterTestNewConstructor";
    private static final String TABLE_NAME_AI = "WriterTestNewConstructorAI";
    private static ServiceSettings serviceSettings = ServiceSettings.load();
    private static AtomicLong succeedRows = new AtomicLong();
    private static AtomicLong failedRows = new AtomicLong();
    private static long rowsCount = 10000;
    private static long columnsCount = 10;
    private static int concurrency = 100;
    private static int queueSize = 1024;
    private static int bucketCount = 100;
    private static WriteMode writeMode = WriteMode.SEQUENTIAL;
    private AsyncClientInterface ots;
    private final String strValue = "0123456789";
    private ExecutorService threadPool;


    public void createTable(AsyncClientInterface ots) throws Exception {
        TableMeta tableMeta = new TableMeta(TABLE_NAME);
        tableMeta.addPrimaryKeyColumn("pk0", PrimaryKeyType.INTEGER);
        tableMeta.addPrimaryKeyColumn("pk1", PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn("pk2", PrimaryKeyType.INTEGER);
        TableOptions tableOptions = new TableOptions();
        tableOptions.setMaxVersions(1);
        tableOptions.setTimeToLive(-1);
        CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);
        request.setReservedThroughput(new ReservedThroughput(new CapacityUnit(0, 0)));
        request.setTableOptions(tableOptions);

        Future<CreateTableResponse> future = ots.createTable(request, null);
        future.get();
    }

    public void createTableAI(AsyncClientInterface ots) throws Exception {
        TableMeta tableMeta = new TableMeta(TABLE_NAME_AI);
        tableMeta.addPrimaryKeyColumn("pk0", PrimaryKeyType.INTEGER);
        tableMeta.addPrimaryKeyColumn("pk1", PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT);
        tableMeta.addPrimaryKeyColumn("pk2", PrimaryKeyType.INTEGER);
        TableOptions tableOptions = new TableOptions();
        tableOptions.setMaxVersions(1);
        tableOptions.setTimeToLive(-1);
        CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);
        request.setReservedThroughput(new ReservedThroughput(new CapacityUnit(0, 0)));
        request.setTableOptions(tableOptions);

        Future<CreateTableResponse> future = ots.createTable(request, null);
        future.get();
    }


    @Before
    public void setUp() throws Exception {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("writer-pool-%d").build();
        threadPool = new ThreadPoolExecutor(10, 10, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue(1024), threadFactory, new ThreadPoolExecutor.CallerRunsPolicy());

        succeedRows.getAndSet(0);
        failedRows.getAndSet(0);

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

        try {
            DeleteTableRequest request = new DeleteTableRequest(TABLE_NAME_AI);
            Future<DeleteTableResponse> future = ots.deleteTable(request, null);
            future.get();
        } catch (Exception e) {
            // pass
        }

        createTable(ots);
        createTableAI(ots);
        Thread.sleep(3000);
    }

    @After
    public void after() {
        threadPool.shutdown();
        ots.shutdown();
    }

    public static DefaultTableStoreWriter createWriter(WriterConfig config, String tableName) {
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
                tableName, config, callback);

        return writer;
    }

    @Test
    public void testWriteCallback() throws Exception {
        final WriterConfig config = new WriterConfig();
        config.setConcurrency(concurrency);
        config.setBufferSize(queueSize);
        config.setWriteMode(writeMode);
        config.setBucketCount(bucketCount);
        config.setFlushInterval(1000000);

        DefaultTableStoreWriter writer = createWriter(config, TABLE_NAME_AI);

        final AtomicLong gotRowCount = new AtomicLong(0);
        writer.setResultCallback(new TableStoreCallback<RowChange, RowWriteResult>() {
            @Override
            public void onCompleted(RowChange req, RowWriteResult res) {
                gotRowCount.incrementAndGet();
                succeedRows.incrementAndGet();
            }

            @Override
            public void onFailed(RowChange req, Exception ex) {
                failedRows.incrementAndGet();
            }
        });

        try {
            for (int i = 0; i < 100; i++) {
                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(i))
                        .addPrimaryKeyColumn("pk1", PrimaryKeyValue.AUTO_INCREMENT)
                        .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(0)).build();

                RowPutChange rowChange = new RowPutChange(TABLE_NAME_AI, pk);
                for (int j = 0; j < columnsCount; j++) {
                    rowChange.addColumn("column_" + j, ColumnValue.fromString(strValue));
                }
                rowChange.setReturnType(ReturnType.RT_PK);

                writer.addRowChange(rowChange);
            }

            writer.flush();

        } finally {
            writer.close();
        }

        assertEquals(succeedRows.get(), 100);
        assertEquals(writer.getWriterStatistics().getTotalSucceedRowsCount(), 100);
        assertEquals(writer.getWriterStatistics().getTotalFailedRowsCount(), 0);
        assertEquals(writer.getWriterStatistics().getTotalRowsCount(), 100);
        assertEquals(writer.getWriterStatistics().getTotalSingleRowRequestCount(), 0);
        assertEquals(gotRowCount.get(), 100);
    }

    @Test
    public void testWriteSameRow() throws Exception {
        final WriterConfig config = new WriterConfig();
        config.setConcurrency(concurrency);
        config.setBufferSize(queueSize);
        config.setWriteMode(writeMode);
        config.setBucketCount(bucketCount);
        config.setFlushInterval(1000000);
        config.setBatchRequestType(BatchRequestType.BULK_IMPORT);

        DefaultTableStoreWriter writer = createWriter(config, TABLE_NAME);

        try {
            for (int i = 0; i < 100; i++) {
                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(i))
                        .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("pk_" + 0))
                        .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(0)).build();

                RowUpdateChange rowChange = new RowUpdateChange(TABLE_NAME, pk);
                for (int j = 0; j < columnsCount; j++) {
                    rowChange.put("column_" + j, ColumnValue.fromString(strValue));
                }

                writer.addRowChange(rowChange);
            }

            writer.flush();

        } finally {
            writer.close();
        }

        assertEquals(succeedRows.get(), 100);
        assertEquals(writer.getWriterStatistics().getTotalSucceedRowsCount(), 100);
        assertEquals(writer.getWriterStatistics().getTotalFailedRowsCount(), 0);
        assertEquals(writer.getWriterStatistics().getTotalRowsCount(), 100);
        assertEquals(writer.getWriterStatistics().getTotalSingleRowRequestCount(), 0);
    }

    @Test
    public void testCloseWriter() throws Exception {
        final WriterConfig config = new WriterConfig();
        config.setConcurrency(concurrency);
        config.setBufferSize(queueSize);
        config.setFlushInterval(1000000);
        config.setWriteMode(writeMode);
        config.setBucketCount(bucketCount);

        DefaultTableStoreWriter writer = createWriter(config, TABLE_NAME);

        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(0))
                .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("pk_" + 0))
                .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(0)).build();

        RowUpdateChange rowChange = new RowUpdateChange(TABLE_NAME, pk);
        for (int j = 0; j < columnsCount; j++) {
            rowChange.put("column_" + j, ColumnValue.fromString(strValue));
        }

        writer.addRowChange(rowChange);
        writer.close();

        try {
            writer.addRowChange(rowChange);
            assertTrue(false);
        } catch (ClientException e) {
            assertEquals(e.getMessage(), "The writer has been closed.");
        }

        try {
            writer.close();
            assertTrue(false);
        } catch (ClientException e) {
            assertEquals(e.getMessage(), "The writer has already been closed.");
        }

        try {
            writer.flush();
            assertTrue(false);
        } catch (ClientException e) {
            assertEquals(e.getMessage(), "The writer has been closed.");
        }
    }

    @Test
    public void testMixOperation() throws Exception {
        final WriterConfig config = new WriterConfig();
        config.setConcurrency(concurrency);
        config.setBufferSize(queueSize);
        config.setFlushInterval(1000000);
        config.setWriteMode(writeMode);
        config.setBucketCount(bucketCount);

        final DefaultTableStoreWriter writer = createWriter(config, TABLE_NAME);

        final int threadsCount = 5;
        final CountDownLatch countDownLatch = new CountDownLatch(threadsCount);
        try {
            for (int i = 0; i < threadsCount; i++) {
                final int finalI = i;
                threadPool.execute(new Runnable() {
                    public void run() {
                        Random random = new Random(System.currentTimeMillis() + finalI * 1000);
                        for (int i = 0; i < rowsCount; i++) {
                            int id = Math.abs(random.nextInt()) % 2;
                            if (id == 0) {
                                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                                        .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(i))
                                        .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("pk_" + i))
                                        .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(i))
                                        .build();

                                RowUpdateChange rowChange = new RowUpdateChange(TABLE_NAME, pk);
                                for (int j = 0; j < columnsCount; j++) {
                                    rowChange.put("update_" + j, ColumnValue.fromString(strValue));
                                }

                                writer.addRowChange(rowChange);
                            } else {
                                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                                        .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(i))
                                        .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("pk_" + i))
                                        .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(i))
                                        .build();

                                RowPutChange rowChange = new RowPutChange(TABLE_NAME, pk);
                                for (int j = 0; j < columnsCount; j++) {
                                    rowChange.addColumn("put_" + j, ColumnValue.fromString(strValue));
                                }

                                writer.addRowChange(rowChange);
                            }
                        }
                        System.out.println("Write thread finished.");
                        countDownLatch.countDown();
                    }
                });
            }

            countDownLatch.await();

            writer.flush();
            Thread.sleep(1000);

            assertEquals(failedRows.get(), 0);
            assertEquals(succeedRows.get(), threadsCount * rowsCount);
            assertEquals(writer.getWriterStatistics().getTotalRowsCount(), succeedRows.get());
            assertEquals(writer.getWriterStatistics().getTotalSucceedRowsCount(), succeedRows.get());
            assertEquals(writer.getWriterStatistics().getTotalFailedRowsCount(), 0);
            assertEquals(writer.getWriterStatistics().getTotalSingleRowRequestCount(), 0);
            //scanTable(ots);
        } finally {
            writer.close();
        }
    }

    @Test
    public void testMixOperationBulkImport() throws Exception {
        final WriterConfig config = new WriterConfig();
        config.setConcurrency(concurrency);
        config.setBufferSize(queueSize);
        config.setFlushInterval(1000000);
        config.setWriteMode(writeMode);
        config.setBatchRequestType(BatchRequestType.BULK_IMPORT);
        config.setBucketCount(100);
        final ExecutorService executor = Executors.newFixedThreadPool(10);

        final DefaultTableStoreWriter writer = createWriter(config, TABLE_NAME);

        final int threadsCount = 2;
        final CountDownLatch countDownLatch = new CountDownLatch(threadsCount);
        try {
            for (int i = 0; i < threadsCount; i++) {
                final int finalI = i;
                threadPool.execute(new Runnable() {
                    public void run() {
                        Random random = new Random(System.currentTimeMillis() + finalI * 1000);
                        for (int i = 0; i < rowsCount; i++) {
                            int id = Math.abs(random.nextInt()) % 2;
                            if (id == 0) {
                                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                                        .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(i))
                                        .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("pk_" + i))
                                        .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(i))
                                        .build();

                                RowUpdateChange rowChange = new RowUpdateChange(TABLE_NAME, pk);
                                for (int j = 0; j < columnsCount; j++) {
                                    rowChange.put("update_" + j, ColumnValue.fromString(strValue));
                                }

                                writer.addRowChange(rowChange);
                            } else {
                                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                                        .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(i))
                                        .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("pk_" + i))
                                        .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(i))
                                        .build();

                                RowPutChange rowChange = new RowPutChange(TABLE_NAME, pk);
                                for (int j = 0; j < columnsCount; j++) {
                                    rowChange.addColumn("put_" + j, ColumnValue.fromString(strValue));
                                }

                                writer.addRowChange(rowChange);
                            }
                        }
                        System.out.println("Write thread finished.");
                        countDownLatch.countDown();
                    }
                });
            }

            countDownLatch.await();

            writer.flush();
            Thread.sleep(1000);

            assertEquals(failedRows.get(), 0);
            assertEquals(succeedRows.get(), threadsCount * rowsCount);
            assertEquals(writer.getWriterStatistics().getTotalRowsCount(), succeedRows.get());
            assertEquals(writer.getWriterStatistics().getTotalSucceedRowsCount(), succeedRows.get());
            assertEquals(writer.getWriterStatistics().getTotalFailedRowsCount(), 0);
            assertEquals(writer.getWriterStatistics().getTotalSingleRowRequestCount(), 0);
            //scanTable(ots);
        } finally {
            writer.close();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    private void scanTable(AsyncClientInterface ots) {
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
            GetRangeResponse result = null;
            try {
                result = future.get();
            } catch (Exception e) {
                throw new ClientException(e);
            }
            for (Row row : result.getRows()) {
                PrimaryKeyValue pk0 = row.getPrimaryKey().getPrimaryKeyColumn("pk0").getValue();
                PrimaryKeyValue pk1 = row.getPrimaryKey().getPrimaryKeyColumn("pk1").getValue();
                PrimaryKeyValue pk2 = row.getPrimaryKey().getPrimaryKeyColumn("pk2").getValue();
                assertEquals(pk0.asLong(), rowsCountInTable);
                assertEquals(pk1.asString(), "pk_" + rowsCountInTable);
                assertEquals(pk2.asLong(), rowsCountInTable);

                if (row.contains("put_0")) {
                    for (int i = 0; i < columnsCount; i++) {
                        ColumnValue c = row.getLatestColumn("put_" + i).getValue();
                        assertTrue(c != null);
                        assertEquals(c.asString(), strValue);
                    }
                } else if (row.contains("update_0")) {
                    for (int i = 0; i < columnsCount; i++) {
                        ColumnValue c = row.getLatestColumn("update_" + i).getValue();
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
        config.setMaxAttrColumnSize(3 * 1024 * 1024); // max attribute length set to 3MB
        config.setMaxBatchSize(4 * 1024 * 1024);
        config.setWriteMode(writeMode);
        config.setBucketCount(1);
        StringBuilder longStr = new StringBuilder();
        for (int i = 0; i < 2 * 1024 * 1024 + 1; i++) {
            longStr.append('a');
        }
        DefaultTableStoreWriter writer = createWriter(config, TABLE_NAME);

        int dirtyRowId = 25;
        try {
            for (int i = 0; i < 100; i++) {
                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(i))
                        .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("pk_" + i))
                        .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(i))
                        .build();

                RowUpdateChange rowChange = new RowUpdateChange(TABLE_NAME, pk);
                for (int j = 0; j < columnsCount; j++) {
                    rowChange.put("column_" + j, ColumnValue.fromString(strValue));
                }

                if (i == dirtyRowId) {
                    rowChange.put("longattr", ColumnValue.fromString(longStr.toString()));
                }

                writer.addRowChange(rowChange);
            }

            writer.flush();

        } finally {
            writer.close();
        }

        assertEquals(succeedRows.get(), 99);
        assertEquals(failedRows.get(), 1);
        assertEquals(writer.getWriterStatistics().getTotalRowsCount(), 100);
        assertEquals(writer.getWriterStatistics().getTotalSucceedRowsCount(), 99);
        assertEquals(writer.getWriterStatistics().getTotalFailedRowsCount(), 1);
        assertEquals(writer.getWriterStatistics().getTotalSingleRowRequestCount(), 100);  //only when bucket = 1
        assertEquals(writer.getWriterStatistics().getTotalRequestCount(), 101);

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
                if (rowsCountInTable == dirtyRowId) {
                    rowsCountInTable++;
                }
                PrimaryKeyValue pk0 = row.getPrimaryKey().getPrimaryKeyColumn("pk0").getValue();
                PrimaryKeyValue pk1 = row.getPrimaryKey().getPrimaryKeyColumn("pk1").getValue();
                PrimaryKeyValue pk2 = row.getPrimaryKey().getPrimaryKeyColumn("pk2").getValue();
                assertEquals(pk0.asLong(), rowsCountInTable);
                assertEquals(pk1.asString(), "pk_" + rowsCountInTable);
                assertEquals(pk2.asLong(), rowsCountInTable);

                if (row.contains("put_0")) {
                    for (int i = 0; i < columnsCount; i++) {
                        ColumnValue c = row.getLatestColumn("put_" + i).getValue();
                        assertTrue(c != null);
                        assertEquals(c.asString(), strValue);
                    }
                } else if (row.contains("update_0")) {
                    for (int i = 0; i < columnsCount; i++) {
                        ColumnValue c = row.getLatestColumn("update_" + i).getValue();
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

    @Test
    public void testSingleRowFuture() {
        int rowCount = 101;
        final WriterConfig config = new WriterConfig();
        config.setConcurrency(concurrency);
        config.setBufferSize(queueSize);
        config.setFlushInterval(1000000);
        config.setMaxAttrColumnSize(3 * 1024 * 1024); // max attribute length set to 3MB
        config.setMaxBatchSize(4 * 1024 * 1024);
        config.setWriteMode(writeMode);
        config.setBucketCount(1);

        DefaultTableStoreWriter writer = createWriter(config, TABLE_NAME);

        List<Future<WriterResult>> futureList = new LinkedList<Future<WriterResult>>();
        try {
            for (int i = 0; i < rowCount; i++) {
                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(i))
                        .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("pk_" + i))
                        .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(i))
                        .build();

                RowUpdateChange rowChange = new RowUpdateChange(TABLE_NAME, pk);
                for (int j = 0; j < columnsCount; j++) {
                    rowChange.put("column_" + j, ColumnValue.fromString(strValue));
                }

                Future<WriterResult> future = writer.addRowChangeWithFuture(rowChange);
                futureList.add(future);
            }

            writer.flush();

        } finally {
            writer.close();
        }
        assertEquals(futureList.size(), rowCount);
        for (Future<WriterResult> future : futureList) {
            try {
                WriterResult result = future.get();

                assertTrue(result.isAllFinished());
                assertEquals(result.getTotalCount(), 1);
                assertEquals(result.getSucceedRows().size(), 1);
            } catch (Exception e) {
                fail();
            }
        }
        assertEquals(succeedRows.get(), rowCount);
        assertEquals(failedRows.get(), 0);
        assertEquals(writer.getWriterStatistics().getTotalRowsCount(), rowCount);
        assertEquals(writer.getWriterStatistics().getTotalSucceedRowsCount(), rowCount);
        assertEquals(writer.getWriterStatistics().getTotalFailedRowsCount(), 0);
        assertEquals(writer.getWriterStatistics().getTotalSingleRowRequestCount(), 0);
        assertEquals(writer.getWriterStatistics().getTotalRequestCount(), 1); // 101行 单筒下请求1次
    }

    @Test
    public void testRowListFuture() {
        int batchCount = 10;
        int rowCount = 101;
        final WriterConfig config = new WriterConfig();
        config.setConcurrency(concurrency);
        config.setBufferSize(queueSize);
        config.setFlushInterval(1000000);
        config.setMaxAttrColumnSize(3 * 1024 * 1024); // max attribute length set to 3MB
        config.setMaxBatchSize(4 * 1024 * 1024);
        config.setWriteMode(writeMode);
        config.setBucketCount(1);

        DefaultTableStoreWriter writer = createWriter(config, TABLE_NAME);

        List<Future<WriterResult>> futureList = new LinkedList<Future<WriterResult>>();
        try {
            for (int batch = 0; batch < batchCount; batch++) {
                List<RowChange> rowChanges = new ArrayList<RowChange>(rowCount);
                for (int i = 0; i < rowCount; i++) {
                    PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                            .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(i))
                            .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("pk_" + batch + "_" + i))
                            .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(i))
                            .build();

                    RowUpdateChange rowChange = new RowUpdateChange(TABLE_NAME, pk);
                    for (int j = 0; j < columnsCount; j++) {
                        rowChange.put("column_" + j, ColumnValue.fromString(strValue));
                    }
                    rowChanges.add(rowChange);
                }
                Future<WriterResult> future = writer.addRowChangeWithFuture(rowChanges);
                futureList.add(future);
            }

            writer.flush();

        } finally {
            writer.close();
        }
        assertEquals(futureList.size(), batchCount);
        for (Future<WriterResult> future : futureList) {
            try {
                WriterResult result = future.get();

                assertTrue(result.isAllFinished());
                assertEquals(result.getTotalCount(), rowCount);
                assertEquals(result.getSucceedRows().size(), rowCount);
            } catch (Exception e) {
                fail();
            }
        }
        assertEquals(succeedRows.get(), batchCount * rowCount);
        assertEquals(failedRows.get(), 0);
        assertEquals(writer.getWriterStatistics().getTotalRowsCount(), batchCount * rowCount);
        assertEquals(writer.getWriterStatistics().getTotalSucceedRowsCount(), batchCount * rowCount);
        assertEquals(writer.getWriterStatistics().getTotalFailedRowsCount(), 0);
        assertEquals(writer.getWriterStatistics().getTotalSingleRowRequestCount(), 0);
        assertEquals(writer.getWriterStatistics().getTotalRequestCount(), 6); // 1010行 单筒下请求6次
    }


    @Test
    public void testWriteDirtySingleRowsFuture() throws Exception {
        int rowCount = 99;
        final WriterConfig config = new WriterConfig();
        config.setConcurrency(concurrency);
        config.setBufferSize(queueSize);
        config.setFlushInterval(1000000);
        config.setMaxAttrColumnSize(3 * 1024 * 1024); // max attribute length set to 3MB
        config.setMaxBatchSize(4 * 1024 * 1024);
        config.setWriteMode(writeMode);
        config.setBucketCount(1);
        StringBuilder longStr = new StringBuilder();
        for (int i = 0; i < 2 * 1024 * 1024 + 1; i++) {
            longStr.append('a');
        }
        DefaultTableStoreWriter writer = createWriter(config, TABLE_NAME);
        List<Future<WriterResult>> futureList = new LinkedList<Future<WriterResult>>();

        int dirtyRowId = 25;
        try {
            for (int i = 0; i < rowCount; i++) {
                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(i))
                        .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("pk_" + i))
                        .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(i))
                        .build();

                RowUpdateChange rowChange = new RowUpdateChange(TABLE_NAME, pk);
                for (int j = 0; j < columnsCount; j++) {
                    rowChange.put("column_" + j, ColumnValue.fromString(strValue));
                }

                if (i == dirtyRowId) {
                    rowChange.put("longattr", ColumnValue.fromString(longStr.toString()));
                }

                Future<WriterResult> future = writer.addRowChangeWithFuture(rowChange);
                futureList.add(future);
            }

            writer.flush();

        } finally {
            writer.close();
        }

        int failedCounter = 0;
        for (int i = 0; i < futureList.size(); i++) {
            try {
                Future<WriterResult> future = futureList.get(i);
                WriterResult result = future.get();

                assertEquals(result.getTotalCount(), 1);
                assertEquals(result.isAllFinished(), true);
                if (result.isAllSucceed()) {
                    assertEquals(result.getSucceedRows().size(), 1);
                } else {
                    failedCounter++;
                    assertEquals(result.getFailedRows().size(), 1);
                    assertEquals(result.getFailedRows().get(0).getException().getMessage(),
                            "The length of attribute column: 'longattr' exceeds the MaxLength:2097152 with CurrentLength:2097153.");
                }
            } catch (Exception e) {
                fail();
            }
        }
        assertEquals(failedCounter, 1);

        assertEquals(succeedRows.get(), rowCount - 1);
        assertEquals(failedRows.get(), 1);
        assertEquals(writer.getWriterStatistics().getTotalRowsCount(), rowCount);
        assertEquals(writer.getWriterStatistics().getTotalSucceedRowsCount(), rowCount - 1);
        assertEquals(writer.getWriterStatistics().getTotalFailedRowsCount(), 1);
        assertEquals(writer.getWriterStatistics().getTotalSingleRowRequestCount(), rowCount);  //only when bucket = 1
        assertEquals(writer.getWriterStatistics().getTotalRequestCount(), rowCount + 1);

    }

    @Test
    public void testWriteDirtyRowsListFuture() throws Exception {
        int batchCount = 2;
        int rowCount = 101;
        final WriterConfig config = new WriterConfig();
        config.setConcurrency(concurrency);
        config.setBufferSize(queueSize);
        config.setFlushInterval(1000000);
        config.setMaxAttrColumnSize(3 * 1024 * 1024); // max attribute length set to 3MB
        config.setMaxBatchSize(4 * 1024 * 1024);
        config.setWriteMode(writeMode);
        config.setBucketCount(1);
        StringBuilder longStr = new StringBuilder();
        for (int i = 0; i < 2 * 1024 * 1024 + 1; i++) {
            longStr.append('a');
        }
        DefaultTableStoreWriter writer = createWriter(config, TABLE_NAME);

        List<Future<WriterResult>> futureList = new LinkedList<Future<WriterResult>>();
        try {
            int dirtyRowId = 25;
            for (int batch = 0; batch < batchCount; batch++) {
                List<RowChange> rowChanges = new ArrayList<RowChange>(rowCount);
                for (int i = 0; i < rowCount; i++) {
                    PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                            .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(i))
                            .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("pk_" + batch + "_" + i))
                            .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(i))
                            .build();

                    RowUpdateChange rowChange = new RowUpdateChange(TABLE_NAME, pk);
                    for (int j = 0; j < columnsCount; j++) {
                        rowChange.put("column_" + j, ColumnValue.fromString(strValue));
                    }

                    if (i == dirtyRowId) {
                        rowChange.put("longattr", ColumnValue.fromString(longStr.toString()));
//                        rowChange.setCondition(new Condition(RowExistenceExpectation.EXPECT_EXIST));
                    }

                    rowChanges.add(rowChange);
                }
                Future<WriterResult> future = writer.addRowChangeWithFuture(rowChanges);
                futureList.add(future);
            }

            writer.flush();

        } finally {
            writer.close();
        }
        assertEquals(futureList.size(), batchCount);

        int failedCounter = 0;
        for (Future<WriterResult> future : futureList) {
            try {
                WriterResult result = future.get();

                assertTrue(result.isAllFinished());
                assertEquals(result.getTotalCount(), rowCount);
                if (result.isAllSucceed()) {
                    assertEquals(result.getSucceedRows().size(), rowCount);
                    assertEquals(result.getFailedRows().size(), 0);
                } else {
                    failedCounter += result.getFailedRows().size();
                    assertFalse(result.getSucceedRows().size() == rowCount);
                    assertEquals(result.getFailedRows().size(), 1);
                    assertEquals(result.getFailedRows().get(0).getException().getMessage(),
                            "The length of attribute column: 'longattr' exceeds the MaxLength:2097152 with CurrentLength:2097153.");
                }
            } catch (Exception e) {
                fail();
            }
        }
        // 每一批次的第25行都是错的
        assertEquals(failedCounter, batchCount);

        assertEquals(succeedRows.get(), batchCount * rowCount - batchCount);
        assertEquals(failedRows.get(), batchCount);
        assertEquals(writer.getWriterStatistics().getTotalRowsCount(), batchCount * rowCount);
        assertEquals(writer.getWriterStatistics().getTotalSucceedRowsCount(), batchCount * rowCount - batchCount);
        assertEquals(writer.getWriterStatistics().getTotalFailedRowsCount(), batchCount);
        assertEquals(writer.getWriterStatistics().getTotalSingleRowRequestCount(), batchCount * rowCount);  //only when bucket = 1
        assertEquals(writer.getWriterStatistics().getTotalRequestCount(), batchCount * (rowCount + 1) );

    }
}
