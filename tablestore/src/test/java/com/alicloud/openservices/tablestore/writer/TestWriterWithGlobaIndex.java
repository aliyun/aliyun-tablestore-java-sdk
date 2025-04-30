package com.alicloud.openservices.tablestore.writer;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.core.auth.DefaultCredentials;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.writer.enums.WriteMode;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

public class TestWriterWithGlobaIndex {
    private static final String TABLE_NAME_WITH_INDEX = "WriterTestWithIndex";
    private static final String TABLE_GLOBAL_INDEX = "WriterTestGlobalIndex";
    private static final String TABLE_NAME_NO_INDEX = "WriterTestNoIndex";
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

    public void createTable(AsyncClientInterface ots) throws Exception {
        TableMeta tableMeta = new TableMeta(TABLE_NAME_NO_INDEX);
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

    public void createTableWithIndex(AsyncClientInterface ots) throws Exception {
        TableMeta tableMeta = new TableMeta(TABLE_NAME_WITH_INDEX);
        tableMeta.addPrimaryKeyColumn("pk0", PrimaryKeyType.INTEGER);
        tableMeta.addPrimaryKeyColumn("pk1", PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn("pk2", PrimaryKeyType.INTEGER);
        TableOptions tableOptions = new TableOptions();
        tableOptions.setMaxVersions(1);
        tableOptions.setTimeToLive(-1);
        CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);

        IndexMeta indexMeta = new IndexMeta(TABLE_GLOBAL_INDEX);
        indexMeta.addPrimaryKeyColumn("pk2");
        indexMeta.addPrimaryKeyColumn("pk1");
        indexMeta.addPrimaryKeyColumn("pk0");

        request.addIndex(indexMeta);

        request.setReservedThroughput(new ReservedThroughput(new CapacityUnit(0, 0)));
        request.setTableOptions(tableOptions);

        Future<CreateTableResponse> future = ots.createTable(request, null);
        future.get();
    }


    @Before
    public void setUp() throws Exception {
        succeedRows.getAndSet(0);
        failedRows.getAndSet(0);

        ots = new AsyncClient(serviceSettings.getOTSEndpoint(),
                serviceSettings.getOTSAccessKeyId(), serviceSettings.getOTSAccessKeySecret(),
                serviceSettings.getOTSInstanceName());

        try {
            DeleteIndexRequest request = new DeleteIndexRequest(TABLE_NAME_WITH_INDEX, TABLE_GLOBAL_INDEX);
            Future<DeleteIndexResponse> future = ots.deleteIndex(request, null);
            future.get();
        } catch (Exception e) {
            // pass
        }

        try {
            DeleteTableRequest request = new DeleteTableRequest(TABLE_NAME_WITH_INDEX);
            Future<DeleteTableResponse> future = ots.deleteTable(request, null);
            future.get();
        } catch (Exception e) {
            // pass
        }


        try {
            DeleteTableRequest request = new DeleteTableRequest(TABLE_NAME_NO_INDEX);
            Future<DeleteTableResponse> future = ots.deleteTable(request, null);
            future.get();
        } catch (Exception e) {
            // pass
        }

        createTable(ots);
        createTableWithIndex(ots);
        Thread.sleep(3000);
    }

    @After
    public void after() {
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
    public void testNotAllowDuplicatedRow() throws Exception {
        int rowCount = 101;
        final WriterConfig config = new WriterConfig();
        config.setConcurrency(concurrency);
        config.setBufferSize(queueSize);
        config.setFlushInterval(1000000);
        config.setMaxAttrColumnSize(3 * 1024 * 1024); // max attribute length set to 3MB
        config.setMaxBatchSize(4 * 1024 * 1024);
        config.setWriteMode(writeMode);
        config.setBucketCount(1);
        config.setAllowDuplicatedRowInBatchRequest(true);
        DefaultTableStoreWriter writer = createWriter(config, TABLE_NAME_WITH_INDEX);

        try {
            for (int i = 0; i < rowCount; i++) {
                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(0))
                        .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("pk_1"))
                        .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(2))
                        .build();

                RowUpdateChange rowChange = new RowUpdateChange(TABLE_NAME_WITH_INDEX, pk);
                for (int j = 0; j < columnsCount; j++) {
                    rowChange.put("column_" + j, ColumnValue.fromString(strValue));
                }

                writer.addRowChange(rowChange);
            }

            writer.flush();

        } finally {
            writer.close();
        }

        assertEquals(writer.getWriterStatistics().getTotalRowsCount(), rowCount);
        assertEquals(writer.getWriterStatistics().getTotalSucceedRowsCount(), rowCount);
        assertEquals(writer.getWriterStatistics().getTotalSingleRowRequestCount(), 0);
        assertEquals(writer.getWriterStatistics().getTotalRequestCount(), rowCount);

    }

    @Test
    public void testAllowDuplicatedRow() throws Exception {
        int rowCount = 101;
        final WriterConfig config = new WriterConfig();
        config.setConcurrency(concurrency);
        config.setBufferSize(queueSize);
        config.setFlushInterval(1000000);
        config.setMaxAttrColumnSize(3 * 1024 * 1024); // max attribute length set to 3MB
        config.setMaxBatchSize(4 * 1024 * 1024);
        config.setWriteMode(writeMode);
        config.setBucketCount(1);
        DefaultTableStoreWriter writer = createWriter(config, TABLE_NAME_NO_INDEX);

        try {
            for (int i = 0; i < rowCount; i++) {
                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(0))
                        .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("pk_1"))
                        .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(2))
                        .build();

                RowUpdateChange rowChange = new RowUpdateChange(TABLE_NAME_NO_INDEX, pk);
                for (int j = 0; j < columnsCount; j++) {
                    rowChange.put("column_" + j, ColumnValue.fromString(strValue));
                }

                writer.addRowChange(rowChange);
            }

            writer.flush();

        } finally {
            writer.close();
        }

        assertEquals(writer.getWriterStatistics().getTotalRowsCount(), rowCount);
        assertEquals(writer.getWriterStatistics().getTotalSucceedRowsCount(), rowCount);
        assertEquals(writer.getWriterStatistics().getTotalSingleRowRequestCount(), 0);
        assertEquals(writer.getWriterStatistics().getTotalRequestCount(), 1);

    }

    @Test
    public void testNotAllowDuplicatedRowAlthoughNoGlobalIndex() throws Exception {
        int rowCount = 101;
        final WriterConfig config = new WriterConfig();
        config.setAllowDuplicatedRowInBatchRequest(false);
        DefaultTableStoreWriter writer = createWriter(config, TABLE_NAME_NO_INDEX);

        try {
            for (int i = 0; i < rowCount; i++) {
                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(0))
                        .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("pk_1"))
                        .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(2))
                        .build();

                RowUpdateChange rowChange = new RowUpdateChange(TABLE_NAME_NO_INDEX, pk);
                for (int j = 0; j < columnsCount; j++) {
                    rowChange.put("column_" + j, ColumnValue.fromString(strValue));
                }

                writer.addRowChange(rowChange);
            }

            writer.flush();

        } finally {
            writer.close();
        }

        assertEquals(writer.getWriterStatistics().getTotalRowsCount(), rowCount);
        assertEquals(writer.getWriterStatistics().getTotalSucceedRowsCount(), rowCount);
        assertEquals(writer.getWriterStatistics().getTotalSingleRowRequestCount(), 0);
        assertEquals(writer.getWriterStatistics().getTotalRequestCount(), rowCount);

    }
}
