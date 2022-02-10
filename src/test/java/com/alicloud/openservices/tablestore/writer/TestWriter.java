package com.alicloud.openservices.tablestore.writer;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.core.ErrorCode;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.writer.enums.BatchRequestType;
import com.alicloud.openservices.tablestore.writer.enums.WriteMode;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

public class TestWriter {
    private static final String tableName = "WriterTest";
    private static final String tableNameAI = "WriterTestAI";
    private static ServiceSettings serviceSettings = ServiceSettings.load();
    private static AtomicLong succeedRows = new AtomicLong();
    private static AtomicLong failedRows = new AtomicLong();
    private static long rowsCount = 100000;
    private static long columnsCount = 10;
    private static int concurrency = 100;
    private static int queueSize = 4096;
    private AsyncClientInterface ots;
    final String strValue = "0123456789";
    private static AtomicLong retryCount = new AtomicLong();

    public void createTable(AsyncClientInterface ots) throws Exception {
        TableMeta tableMeta = new TableMeta(tableName);
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

    public void createTable2(AsyncClientInterface ots) throws Exception {
        TableMeta tableMeta = new TableMeta(tableNameAI);
        tableMeta.addPrimaryKeyColumn("pk0", PrimaryKeyType.INTEGER);
        tableMeta.addAutoIncrementPrimaryKeyColumn("pk1");
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
        succeedRows.getAndSet(0);
        failedRows.getAndSet(0);

        ClientConfiguration cc = new ClientConfiguration();
        cc.setMaxConnections(1000);
        cc.setRetryStrategy(new RetryStrategy() {
            private int retries = 0;

            @Override
            public RetryStrategy clone() {
                return this;
            }

            @Override
            public int getRetries() {
                return ++retries;
            }

            @Override
            public long nextPause(String action, Exception ex) {
                if ("DeleteRow".equals(action)) {
                    return 0;
                }

                if ("DeleteTable".equals(action)) {
                    return 0;
                }

                if (ex instanceof TableStoreException) {
                    TableStoreException otsException = (TableStoreException) ex;
                    if (otsException.getErrorCode().equals(ErrorCode.INVALID_PARAMETER)) {
                        return 0;
                    }
                }

                retryCount.incrementAndGet();
                return 10;
            }
        });
        ots = new AsyncClient(serviceSettings.getOTSEndpoint(),
                serviceSettings.getOTSAccessKeyId(), serviceSettings.getOTSAccessKeySecret(),
                serviceSettings.getOTSInstanceName(), cc);

        try {
            DeleteTableRequest request = new DeleteTableRequest(tableName);
            Future<DeleteTableResponse> future = ots.deleteTable(request, null);
            future.get();
        } catch (Exception e) {
            // pass
        }

        try {
            DeleteTableRequest request = new DeleteTableRequest(tableNameAI);
            Future<DeleteTableResponse> future = ots.deleteTable(request, null);
            future.get();
        } catch (Exception e) {
            // pass
        }
        createTable(ots);
        createTable2(ots);
        Thread.sleep(3000);
    }

    @After
    public void after() {
        ots.shutdown();
    }

    public static DefaultTableStoreWriter createWriter(AsyncClientInterface ots, WriterConfig config, Executor executor) {
        TableStoreCallback<RowChange, ConsumedCapacity> callback = new TableStoreCallback<RowChange, ConsumedCapacity>() {
            @Override
            public void onCompleted(RowChange rowChange, ConsumedCapacity cc) {
                succeedRows.incrementAndGet();
            }

            @Override
            public void onFailed(RowChange rowChange, Exception ex) {
                ex.printStackTrace();
                failedRows.incrementAndGet();
            }
        };
        DefaultTableStoreWriter writer = new DefaultTableStoreWriter(ots, tableName, config, callback, executor);

        return writer;
    }

    @Test
    public void testWriteCallback() throws Exception {
        final WriterConfig config = new WriterConfig();
        config.setConcurrency(concurrency);
        config.setBufferSize(queueSize);
        config.setFlushInterval(1000000);
        final ExecutorService executor = Executors.newFixedThreadPool(10);

        DefaultTableStoreWriter writer = new DefaultTableStoreWriter(ots, tableNameAI, config, null, executor);

        final AtomicLong gotRowCount = new AtomicLong(0);
        writer.setResultCallback(new TableStoreCallback<RowChange, RowWriteResult>() {
            @Override
            public void onCompleted(RowChange req, RowWriteResult res) {
                if (res.getRow() != null) {
                    gotRowCount.incrementAndGet();
                }
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
                        .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(0))
                        .addPrimaryKeyColumn("pk1", PrimaryKeyValue.AUTO_INCREMENT)
                        .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(0)).build();

                RowPutChange rowChange = new RowPutChange(tableNameAI, pk);
                for (int j = 0; j < columnsCount; j++) {
                    rowChange.addColumn("column_" + j, ColumnValue.fromString(strValue));
                }
                rowChange.setReturnType(ReturnType.RT_PK);

                writer.addRowChange(rowChange);
            }

            writer.flush();

        } finally {
            writer.close();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        assertEquals(succeedRows.get(), 100);
        assertEquals(writer.getWriterStatistics().getTotalSucceedRowsCount(), 100);
        assertEquals(writer.getWriterStatistics().getTotalFailedRowsCount(), 0);
        assertEquals(writer.getWriterStatistics().getTotalRequestCount(), 1);
        assertEquals(writer.getWriterStatistics().getTotalRowsCount(), 100);
        assertEquals(writer.getWriterStatistics().getTotalSingleRowRequestCount(), 0);
        assertEquals(gotRowCount.get(), 100);
    }

    @Test
    public void testWriteSameRow() throws Exception {
        final WriterConfig config = new WriterConfig();
        config.setConcurrency(concurrency);
        config.setBufferSize(queueSize);
        config.setFlushInterval(1000000);
        final ExecutorService executor = Executors.newFixedThreadPool(10);

        DefaultTableStoreWriter writer = createWriter(ots, config, executor);

        try {
            for (int i = 0; i < 100; i++) {
                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(0))
                        .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("pk_" + 0))
                        .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(0)).build();

                RowUpdateChange rowChange = new RowUpdateChange(tableName, pk);
                for (int j = 0; j < columnsCount; j++) {
                    rowChange.put("column_" + j, ColumnValue.fromString(strValue));
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
        assertEquals(writer.getWriterStatistics().getTotalSucceedRowsCount(), 100);
        assertEquals(writer.getWriterStatistics().getTotalFailedRowsCount(), 0);
        assertEquals(writer.getWriterStatistics().getTotalRequestCount(), 1);
        assertEquals(writer.getWriterStatistics().getTotalRowsCount(), 100);
        assertEquals(writer.getWriterStatistics().getTotalSingleRowRequestCount(), 0);
    }

    @Test
    public void testCloseWriter() throws Exception {
        final WriterConfig config = new WriterConfig();
        config.setConcurrency(concurrency);
        config.setBufferSize(queueSize);
        config.setFlushInterval(1000000);
        final ExecutorService executor = Executors.newFixedThreadPool(10);

        DefaultTableStoreWriter writer = createWriter(ots, config, executor);

        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(0))
                .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("pk_" + 0))
                .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(0)).build();

        RowUpdateChange rowChange = new RowUpdateChange(tableName, pk);
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


        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test
    public void testMixOperation() throws Exception {
        final WriterConfig config = new WriterConfig();
        config.setConcurrency(concurrency);
        config.setBufferSize(queueSize);
        config.setFlushInterval(1000000);
        final ExecutorService executor = Executors.newFixedThreadPool(10);

        final DefaultTableStoreWriter writer = createWriter(ots, config, executor);

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
                                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                                        .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(i))
                                        .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("pk_" + i))
                                        .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(i))
                                        .build();

                                RowUpdateChange rowChange = new RowUpdateChange(tableName, pk);
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

                                RowPutChange rowChange = new RowPutChange(tableName, pk);
                                for (int j = 0; j < columnsCount; j++) {
                                    rowChange.addColumn("put_" + j, ColumnValue.fromString(strValue));
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
        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
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
        config.setBucketCount(1);
        StringBuilder longStr = new StringBuilder();
        for (int i = 0; i < 2 * 1024 * 1024 + 1; i++) {
            longStr.append('a');
        }
        final ExecutorService executor = Executors.newFixedThreadPool(10);

        DefaultTableStoreWriter writer = createWriter(ots, config, executor);

        int putDirtyRowId = 25;
        int updateDirtyRowId = 75;
        try {
            for (int i = -1; i < 0; i++) {
                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(i))
                        .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("pk_not_exist_" + i ))
                        .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(i))
                        .build();

                RowDeleteChange rowChange = new RowDeleteChange(tableName, pk);

                Condition condition = new Condition();
                condition.setRowExistenceExpectation(RowExistenceExpectation.EXPECT_EXIST);
                rowChange.setCondition(condition);

                writer.addRowChange(rowChange);
            }
            for (int i = 0; i < 50; i++) {
                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(i))
                        .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("pk_" + i))
                        .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(i))
                        .build();

                RowPutChange rowChange = new RowPutChange(tableName, pk);
                for (int j = 0; j < columnsCount; j++) {
                    rowChange.addColumn("column_" + j, ColumnValue.fromString(strValue));
                }

                if (i == putDirtyRowId) {
                    rowChange.addColumn("longattr", ColumnValue.fromString(longStr.toString()));
                }

                writer.addRowChange(rowChange);
            }
            for (int i = 50; i < 100; i++) {
                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(i))
                        .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("pk_" + i))
                        .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(i))
                        .build();

                RowUpdateChange rowChange = new RowUpdateChange(tableName, pk);
                for (int j = 0; j < columnsCount; j++) {
                    rowChange.put("column_" + j, ColumnValue.fromString(strValue));
                }

                if (i == updateDirtyRowId) {
                    rowChange.put("longattr", ColumnValue.fromString(longStr.toString()));
                }

                writer.addRowChange(rowChange);
            }

            writer.flush();

        } catch (Exception e) {
            fail();
        } finally {
            writer.close();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        assertEquals(succeedRows.get(), 98);
        assertEquals(failedRows.get(), 3);
        assertEquals(writer.getWriterStatistics().getTotalRowsCount(), 101);
        assertEquals(writer.getWriterStatistics().getTotalSucceedRowsCount(), 98);
        assertEquals(writer.getWriterStatistics().getTotalFailedRowsCount(), 3);
        assertEquals(writer.getWriterStatistics().getTotalSingleRowRequestCount(), 101);  //only when bucket = 1
        assertEquals(writer.getWriterStatistics().getTotalRequestCount(), 103);

        int rowsCountInTable = 0;
        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
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
                if (rowsCountInTable == putDirtyRowId || rowsCountInTable == updateDirtyRowId) {
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
    public void testBulkImportWriteDirtyRows() throws Exception {
        final WriterConfig config = new WriterConfig();
        config.setConcurrency(concurrency);
        config.setBufferSize(queueSize);
        config.setFlushInterval(1000000);
        config.setMaxAttrColumnSize(3 * 1024 * 1024); // max attribute length set to 3MB
        config.setMaxBatchSize(4 * 1024 * 1024);
        config.setBucketCount(1);
        config.setBatchRequestType(BatchRequestType.BULK_IMPORT);
        config.setWriteMode(WriteMode.SEQUENTIAL);
        StringBuilder longStr = new StringBuilder();
        for (int i = 0; i < 2 * 1024 * 1024 + 1; i++) {
            longStr.append('a');
        }
        final ExecutorService executor = Executors.newFixedThreadPool(10);

        DefaultTableStoreWriter writer = createWriter(ots, config, executor);

        int putDirtyRowId = 25;
        int updateDirtyRowId = 75;
        try {
            for (int i = -1; i < 0; i++) {
                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(i))
                        .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("pk_not_exist_" + i ))
                        .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(i))
                        .build();

                RowDeleteChange rowChange = new RowDeleteChange(tableName, pk);

                Condition condition = new Condition();
                condition.setRowExistenceExpectation(RowExistenceExpectation.EXPECT_EXIST);
                rowChange.setCondition(condition);

                writer.addRowChange(rowChange);
            }
            for (int i = 0; i < 50; i++) {
                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(i))
                        .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("pk_" + i))
                        .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(i))
                        .build();

                RowPutChange rowChange = new RowPutChange(tableName, pk);
                for (int j = 0; j < columnsCount; j++) {
                    rowChange.addColumn("column_" + j, ColumnValue.fromString(strValue));
                }

                if (i == putDirtyRowId) {
                    rowChange.addColumn("longattr", ColumnValue.fromString(longStr.toString()));
                }

                writer.addRowChange(rowChange);
            }
            for (int i = 50; i < 100; i++) {
                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(i))
                        .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("pk_" + i))
                        .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(i))
                        .build();

                RowUpdateChange rowChange = new RowUpdateChange(tableName, pk);
                for (int j = 0; j < columnsCount; j++) {
                    rowChange.put("column_" + j, ColumnValue.fromString(strValue));
                }

                if (i == updateDirtyRowId) {
                    rowChange.put("longattr", ColumnValue.fromString(longStr.toString()));
                }

                writer.addRowChange(rowChange);
            }

            writer.flush();

        } catch (Exception e) {
            fail();
        } finally {
            writer.close();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        assertEquals(succeedRows.get(), 98);
        assertEquals(failedRows.get(), 3);
        assertEquals(writer.getWriterStatistics().getTotalRowsCount(), 101);
        assertEquals(writer.getWriterStatistics().getTotalSucceedRowsCount(), 98);
        assertEquals(writer.getWriterStatistics().getTotalFailedRowsCount(), 3);
        assertEquals(writer.getWriterStatistics().getTotalSingleRowRequestCount(), 101);  //only when bucket = 1
        assertEquals(writer.getWriterStatistics().getTotalRequestCount(), 103);
    }
}
