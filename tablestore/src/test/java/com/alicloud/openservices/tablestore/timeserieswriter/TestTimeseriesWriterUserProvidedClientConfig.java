package com.alicloud.openservices.tablestore.timeserieswriter;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.core.ErrorCode;
import com.alicloud.openservices.tablestore.core.auth.DefaultCredentials;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.TimeseriesTableMeta;
import com.alicloud.openservices.tablestore.model.TimeseriesTableOptions;
import com.alicloud.openservices.tablestore.model.timeseries.*;
import com.alicloud.openservices.tablestore.timeserieswriter.callback.TimeseriesRowResult;
import com.alicloud.openservices.tablestore.timeserieswriter.config.TimeseriesWriterConfig;
import com.alicloud.openservices.tablestore.writer.retry.CertainCodeRetryStrategy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

public class TestTimeseriesWriterUserProvidedClientConfig {
    private static final String tableName = "WriterTest";
    private static final String tableNameAI = "WriterTestAI";
    private static ServiceSettings serviceSettings = ServiceSettings.load();
    private static AtomicLong succeedRows = new AtomicLong();
    private static AtomicLong failedRows = new AtomicLong();
    private static long rowsCount = 100000;
    private static long columnsCount = 10;
    private static int concurrency = 100;
    private static int queueSize = 4096;
    private AsyncTimeseriesClient ots;
    final String strValue = "0123456789";
    private static AtomicLong retryCount = new AtomicLong();

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
        ots = new AsyncTimeseriesClient(serviceSettings.getOTSEndpoint(),
                serviceSettings.getOTSAccessKeyId(), serviceSettings.getOTSAccessKeySecret(),
                serviceSettings.getOTSInstanceName(), cc);

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
    }

    public static DefaultTableStoreTimeseriesWriter createWriter(AsyncTimeseriesClient ots, TimeseriesWriterConfig config, Executor executor) {
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

        RetryStrategy retryStrategy = new CertainCodeRetryStrategy(1, TimeUnit.SECONDS);
        ClientConfiguration configuration = new ClientConfiguration();
        configuration.setRetryStrategy(retryStrategy);

        DefaultCredentials credentials = new DefaultCredentials(serviceSettings.getOTSAccessKeyId(), serviceSettings.getOTSAccessKeySecret());

        DefaultTableStoreTimeseriesWriter writer = new DefaultTableStoreTimeseriesWriter(serviceSettings.getOTSEndpoint(), credentials, serviceSettings.getOTSInstanceName(), config, configuration, callback);

        return writer;
    }

    @Test
    public void testWriteCallback() throws Exception {
        final TimeseriesWriterConfig config = new TimeseriesWriterConfig();
        config.setConcurrency(concurrency);
        config.setBufferSize(queueSize);
        config.setFlushInterval(1000000);
        final ExecutorService executor = Executors.newFixedThreadPool(10);

        DefaultTableStoreTimeseriesWriter writer = new DefaultTableStoreTimeseriesWriter(ots, config, null, executor);

        final AtomicLong gotRowCount = new AtomicLong(0);
        writer.setResultCallback(new TableStoreCallback<TimeseriesTableRow, TimeseriesRowResult>() {
            @Override
            public void onCompleted(TimeseriesTableRow rowChange, TimeseriesRowResult cc) {
                if (cc.isSuccess()) {
                    gotRowCount.incrementAndGet();
                }
                succeedRows.incrementAndGet();
            }

            @Override
            public void onFailed(TimeseriesTableRow rowChange, Exception ex) {
                failedRows.incrementAndGet();
            }
        });

        try {
            for (int i = 0; i < 100; i++) {
                Map<String, String> tags = new HashMap<String, String>();
                tags.put("region", "hangzhou");
                tags.put("os", "Ubuntu16.04");
                TimeseriesKey timeseriesKey = new TimeseriesKey("cpu" + i, "host_" + i, tags);
                TimeseriesRow row = new TimeseriesRow(timeseriesKey, System.currentTimeMillis() * 1000 + i);
                for (int j = 0; j < columnsCount; j++) {
                    row.addField("column_" + j, ColumnValue.fromString(strValue));
                }

                writer.addTimeseriesRowChange(new TimeseriesTableRow(row, tableName));
            }

            writer.flush();

        } finally {
            writer.close();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        assertEquals(succeedRows.get(), 100);
        assertEquals(writer.getTimeseriesWriterStatistics().getTotalSucceedRowsCount(), 100);
        assertEquals(writer.getTimeseriesWriterStatistics().getTotalFailedRowsCount(), 0);
        assertEquals(writer.getTimeseriesWriterStatistics().getTotalRequestCount(), 4);
        assertEquals(writer.getTimeseriesWriterStatistics().getTotalRowsCount(), 100);
        assertEquals(writer.getTimeseriesWriterStatistics().getTotalSingleRowRequestCount(), 0);
        assertEquals(gotRowCount.get(), 100);
    }

    @Test
    public void testWriteSameRow() throws Exception {
        final TimeseriesWriterConfig config = new TimeseriesWriterConfig();
        config.setConcurrency(concurrency);
        config.setBufferSize(queueSize);
        config.setFlushInterval(1000000);
        final ExecutorService executor = Executors.newFixedThreadPool(10);

        DefaultTableStoreTimeseriesWriter writer = createWriter(ots, config, executor);

        try {
            for (int i = 0; i < 100; i++) {
                Map<String, String> tags = new HashMap<String, String>();
                tags.put("region", "hangzhou");
                tags.put("os", "Ubuntu16.04");
                TimeseriesKey timeseriesKey = new TimeseriesKey("cpu", "host_0", tags);
                TimeseriesRow row = new TimeseriesRow(timeseriesKey, System.currentTimeMillis() * 1000 + i);
                for (int j = 0; j < columnsCount; j++) {
                    row.addField("column_" + j, ColumnValue.fromString(strValue));
                }

                writer.addTimeseriesRowChange(new TimeseriesTableRow(row, tableName));
            }

            writer.flush();

        } finally {
            writer.close();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        assertEquals(succeedRows.get(), 100);
        assertEquals(writer.getTimeseriesWriterStatistics().getTotalSucceedRowsCount(), 100);
        assertEquals(writer.getTimeseriesWriterStatistics().getTotalFailedRowsCount(), 0);
        assertEquals(writer.getTimeseriesWriterStatistics().getTotalRequestCount(), 1);
        assertEquals(writer.getTimeseriesWriterStatistics().getTotalRowsCount(), 100);
        assertEquals(writer.getTimeseriesWriterStatistics().getTotalSingleRowRequestCount(), 0);
    }

    @Test
    public void testCloseWriter() throws Exception {
        final TimeseriesWriterConfig config = new TimeseriesWriterConfig();
        config.setConcurrency(concurrency);
        config.setBufferSize(queueSize);
        config.setFlushInterval(1000000);
        final ExecutorService executor = Executors.newFixedThreadPool(10);

        DefaultTableStoreTimeseriesWriter writer = createWriter(ots, config, executor);

        Map<String, String> tags = new HashMap<String, String>();
        tags.put("region", "hangzhou");
        tags.put("os", "Ubuntu16.04");
        TimeseriesKey timeseriesKey = new TimeseriesKey("cpu", "host_0", tags);
        TimeseriesRow row = new TimeseriesRow(timeseriesKey, System.currentTimeMillis() * 1000);
        for (int j = 0; j < columnsCount; j++) {
            row.addField("column_" + j, ColumnValue.fromString(strValue));
        }

        writer.addTimeseriesRowChange(new TimeseriesTableRow(row, tableName));
        writer.close();

        try {
            writer.addTimeseriesRowChange(new TimeseriesTableRow(row, tableName));
            assertTrue(false);
        } catch (ClientException e) {
            assertEquals(e.getMessage(), "The time series writer has been closed.");
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
    public void testWriteDirtyRows() throws Exception {
        final TimeseriesWriterConfig config = new TimeseriesWriterConfig();
        config.setConcurrency(concurrency);
        config.setBufferSize(queueSize);
        config.setFlushInterval(1000000);
        config.setMaxBatchSize(4 * 1024 * 1024);
        config.setBucketCount(1);
        StringBuilder longStr = new StringBuilder();
        for (int i = 0; i < 2 * 1024 * 1024 + 1; i++) {
            longStr.append('a');
        }
        final ExecutorService executor = Executors.newFixedThreadPool(10);

        DefaultTableStoreTimeseriesWriter writer = createWriter(ots, config, executor);

        int putDirtyRowId_1 = 25;
        int putDirtyRowId_2 = 75;
        try {
            for (int i = 0; i < 50; i++) {

                Map<String, String> tags = new HashMap<String, String>();
                tags.put("region", "hangzhou");
                tags.put("os", "Ubuntu16.04");
                TimeseriesKey timeseriesKey = new TimeseriesKey("cpu" + i, "host_" + i, tags);
                TimeseriesRow row = new TimeseriesRow(timeseriesKey, System.currentTimeMillis() * 1000 + i);
                for (int j = 0; j < columnsCount; j++) {
                    row.addField("column_" + j, ColumnValue.fromString(strValue));
                }
                if (i == putDirtyRowId_1) {
                    row.addField("longattr", ColumnValue.fromString(longStr.toString()));
                }

                writer.addTimeseriesRowChange(new TimeseriesTableRow(row, tableName));
            }
            for (int i = 50; i < 100; i++) {
                Map<String, String> tags = new HashMap<String, String>();
                tags.put("region", "hangzhou");
                tags.put("os", "Ubuntu16.04");
                TimeseriesKey timeseriesKey = new TimeseriesKey("cpu" + i, "host_" + i, tags);
                TimeseriesRow row = new TimeseriesRow(timeseriesKey, System.currentTimeMillis() * 1000 + i);
                for (int j = 0; j < columnsCount; j++) {
                    row.addField("column_" + j, ColumnValue.fromString(strValue));
                }
                if (i == putDirtyRowId_2) {
                    row.addField("longattr", ColumnValue.fromString(longStr.toString()));
                }

                writer.addTimeseriesRowChange(new TimeseriesTableRow(row, tableName));
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
        assertEquals(failedRows.get(), 2);
        assertEquals(writer.getTimeseriesWriterStatistics().getTotalRowsCount(), 100);
        assertEquals(writer.getTimeseriesWriterStatistics().getTotalSucceedRowsCount(), 98);
        assertEquals(writer.getTimeseriesWriterStatistics().getTotalFailedRowsCount(), 2);
        assertEquals(writer.getTimeseriesWriterStatistics().getTotalSingleRowRequestCount(), 100);  //only when bucket = 1
        assertEquals(writer.getTimeseriesWriterStatistics().getTotalRequestCount(), 102);
    }

}
