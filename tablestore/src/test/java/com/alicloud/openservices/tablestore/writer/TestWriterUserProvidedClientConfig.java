package com.alicloud.openservices.tablestore.writer;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.AsyncClientInterface;
import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.DefaultTableStoreWriter;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.core.auth.DefaultCredentials;
import com.alicloud.openservices.tablestore.model.CapacityUnit;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.CreateTableRequest;
import com.alicloud.openservices.tablestore.model.CreateTableResponse;
import com.alicloud.openservices.tablestore.model.DeleteTableRequest;
import com.alicloud.openservices.tablestore.model.DeleteTableResponse;
import com.alicloud.openservices.tablestore.model.GetRangeRequest;
import com.alicloud.openservices.tablestore.model.GetRangeResponse;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyOption;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.RangeRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.ReservedThroughput;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.ReturnType;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.RowChange;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.alicloud.openservices.tablestore.model.RowUpdateChange;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.model.TableOptions;
import com.alicloud.openservices.tablestore.writer.enums.BatchRequestType;
import com.alicloud.openservices.tablestore.writer.enums.WriteMode;
import com.alicloud.openservices.tablestore.writer.retry.CertainCodeRetryStrategy;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestWriterUserProvidedClientConfig {
    private static final String TABLE_NAME = "WriterTestNewConstructorWithUserDefinedClientConfig";
    private static ServiceSettings serviceSettings = ServiceSettings.load();
    private static AtomicLong succeedRows = new AtomicLong();
    private static AtomicLong failedRows = new AtomicLong();
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
        tableMeta.addPrimaryKeyColumn("pk_0", PrimaryKeyType.INTEGER);
        tableMeta.addPrimaryKeyColumn("pk_1", PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn("pk_2", PrimaryKeyType.INTEGER);
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

        createTable(ots);
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

        RetryStrategy retryStrategy = new CertainCodeRetryStrategy(1, TimeUnit.SECONDS);
        ClientConfiguration configuration = new ClientConfiguration();
        configuration.setRetryStrategy(retryStrategy);

        DefaultCredentials credentials = new DefaultCredentials(serviceSettings.getOTSAccessKeyId(), serviceSettings.getOTSAccessKeySecret());
        DefaultTableStoreWriter writer = new DefaultTableStoreWriter(
                serviceSettings.getOTSEndpoint(),
                credentials,
                serviceSettings.getOTSInstanceName(),
                tableName, config, configuration, callback);

        return writer;
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
                        .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(i))
                        .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromString("pk_" + 0))
                        .addPrimaryKeyColumn("pk_2", PrimaryKeyValue.fromLong(0)).build();

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
                .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(0))
                .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromString("pk_" + 0))
                .addPrimaryKeyColumn("pk_2", PrimaryKeyValue.fromLong(0)).build();

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
}
