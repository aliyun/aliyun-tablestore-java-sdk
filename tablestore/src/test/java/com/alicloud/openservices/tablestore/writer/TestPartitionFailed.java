package com.alicloud.openservices.tablestore.writer;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.DefaultTableStoreWriter;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.TableStoreWriter;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.Error;
import com.alicloud.openservices.tablestore.writer.enums.BatchRequestType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;


public class TestPartitionFailed {
    private static ServiceSettings serviceSettings = ServiceSettings.load();
    private static String MOCK_TABLE_NAME = "mockTableName";
    private static String MOCK_INDEX_NAME = "mockIndexName";
    private static String PK = "pk";
    private static String COL_1 = "col1";
    private static String COL_2 = "col2";

    private static AtomicLong succeedRows = new AtomicLong();
    private static AtomicLong failedRows = new AtomicLong();
    final ExecutorService executor = Executors.newFixedThreadPool(10);

    private int rowSize = 200;
    private AsyncClient ots;
    private TableStoreWriter batchWriteRowWriter;
    private TableStoreWriter bulkImportWriter;


    @Before
    public void setUp() throws Exception  {
        succeedRows.getAndSet(0);
        failedRows.getAndSet(0);
        ots = new MockClient(
                serviceSettings.getOTSEndpoint(),
                serviceSettings.getOTSAccessKeyId(),
                serviceSettings.getOTSAccessKeySecret(),
                serviceSettings.getOTSInstanceName());
        final WriterConfig batchWriteRowConfig = new WriterConfig();
        batchWriteRowConfig.setBucketCount(1);
        batchWriteRowWriter = createMockWriter(ots, batchWriteRowConfig, executor);

        final WriterConfig bulkImportConfig = new WriterConfig();
        bulkImportConfig.setBucketCount(1);
        bulkImportConfig.setBatchRequestType(BatchRequestType.BULK_IMPORT);
        bulkImportWriter = createMockWriter(ots, bulkImportConfig, executor);

    }

    @After
    public void after() throws Exception {
        ots.shutdown();
        executor.shutdown();
    }


    @Test
    public void testBatchWriteRowPartitionFailed() throws Exception {

        for (int i = 0; i < rowSize; i++) {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn(PK, PrimaryKeyValue.fromLong(i))
                    .build();

            RowUpdateChange rowChange = new RowUpdateChange(MOCK_TABLE_NAME, pk);
            rowChange.put(COL_1, ColumnValue.fromLong(i));

            batchWriteRowWriter.addRowChange(rowChange);
        }

        batchWriteRowWriter.flush();

        Assert.assertEquals(succeedRows.get(), rowSize / 2);
    }

    @Test
    public void testBulkImportPartitionFailed() throws Exception {

        for (int i = 0; i < rowSize; i++) {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn(PK, PrimaryKeyValue.fromLong(i))
                    .build();

            RowUpdateChange rowChange = new RowUpdateChange(MOCK_TABLE_NAME, pk);
            rowChange.put(COL_1, ColumnValue.fromLong(i));
            rowChange.put(COL_2, ColumnValue.fromLong(i));

            bulkImportWriter.addRowChange(rowChange);
        }

        bulkImportWriter.flush();

        Assert.assertEquals(succeedRows.get(), rowSize / 2);
    }


    private static TableStoreWriter createMockWriter(AsyncClient ots, WriterConfig config, ExecutorService executor) {


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
        return new DefaultTableStoreWriter(ots, MOCK_TABLE_NAME, config, callback, executor);
    }



    public class MockClient extends AsyncClient {
        public MockClient(String endpoint, String accessKeyId, String accessKeySecret, String instanceName) {
            super(endpoint, accessKeyId, accessKeySecret, instanceName);
        }

        @Override
        public void shutdown() {
            super.shutdown();
        }

        @Override
        public Future<DescribeTableResponse> describeTable(
                DescribeTableRequest request,
                TableStoreCallback<DescribeTableRequest, DescribeTableResponse> callback)
        {
            Response meta = new Response();
            meta.setRequestId("mockRequestId");

            final DescribeTableResponse response = new DescribeTableResponse(meta);
            TableMeta tableMeta = new TableMeta(MOCK_TABLE_NAME);
            tableMeta.addPrimaryKeyColumn(PK, PrimaryKeyType.INTEGER);

            IndexMeta indexMeta = new IndexMeta(MOCK_INDEX_NAME);
            response.addIndexMeta(indexMeta);

            response.setTableMeta(tableMeta);

            if (callback != null) {
                callback.onCompleted(request, response);
            }

            return new Future<DescribeTableResponse>() {
                @Override
                public boolean cancel(boolean mayInterruptIfRunning) {
                    return false;
                }

                @Override
                public boolean isCancelled() {
                    return false;
                }

                @Override
                public boolean isDone() {
                    return false;
                }

                @Override
                public DescribeTableResponse get() throws InterruptedException, ExecutionException {
                    return response;
                }

                @Override
                public DescribeTableResponse get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                    return response;
                }
            };
        }


        @Override
        public Future<BatchWriteRowResponse> batchWriteRow(
                final BatchWriteRowRequest request,
                TableStoreCallback<BatchWriteRowRequest, BatchWriteRowResponse> callback) {

            Response meta = new Response();
            meta.setRequestId("mockRequestId");

            final BatchWriteRowResponse response = new BatchWriteRowResponse(meta);
            for (int i = 0; i < rowSize; i++) {
                PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(i))
                        .build();
                List<Column> columns = Arrays.asList(
                        new Column("col1", ColumnValue.fromLong(i)),
                        new Column("col2", ColumnValue.fromString(i + "")));

                Row row = new Row(primaryKey, columns);

                CapacityUnit capacityUnit = new CapacityUnit();
                capacityUnit.setWriteCapacityUnit(0);
                capacityUnit.setReadCapacityUnit(1);

                ConsumedCapacity consumedCapacity = new ConsumedCapacity(capacityUnit);

                BatchWriteRowResponse.RowResult status = null;
                if (i % 2 == 0) {
                    status = new BatchWriteRowResponse.RowResult(MOCK_TABLE_NAME, row, consumedCapacity, i);
                } else {
                    Error error = new Error("500", "mock partition failed");
                    status = new BatchWriteRowResponse.RowResult(MOCK_TABLE_NAME, row, error, i);
                }
                response.addRowResult(status);
            }

            if (callback != null) {
                callback.onCompleted(request, response);
            }

            return new Future<BatchWriteRowResponse>() {
                @Override
                public boolean cancel(boolean mayInterruptIfRunning) {
                    return false;
                }

                @Override
                public boolean isCancelled() {
                    return false;
                }

                @Override
                public boolean isDone() {
                    return true;
                }

                @Override
                public BatchWriteRowResponse get() throws InterruptedException, ExecutionException {
                    return response;
                }

                @Override
                public BatchWriteRowResponse get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                    return response;
                }
            };
        }

        @Override
        public Future<BulkImportResponse> bulkImport(
                BulkImportRequest request,
                TableStoreCallback<BulkImportRequest, BulkImportResponse> callback) {

            Response meta = new Response();
            meta.setRequestId("mockRequestId");

            final BulkImportResponse response = new BulkImportResponse(meta);
            for (int i = 0; i < rowSize; i++) {
                CapacityUnit capacityUnit = new CapacityUnit();
                capacityUnit.setWriteCapacityUnit(0);
                capacityUnit.setReadCapacityUnit(1);

                ConsumedCapacity consumedCapacity = new ConsumedCapacity(capacityUnit);

                BulkImportResponse.RowResult status = null;
                if (i % 2 == 0) {
                    status = new BulkImportResponse.RowResult(consumedCapacity, i);
                } else {
                    Error error = new Error("500", "mock partition failed");
                    status = new BulkImportResponse.RowResult(error, i);
                }
                response.addRowResult(status);
            }

            if (callback != null) {
                callback.onCompleted(request, response);
            }

            return new Future<BulkImportResponse>() {
                @Override
                public boolean cancel(boolean mayInterruptIfRunning) {
                    return false;
                }

                @Override
                public boolean isCancelled() {
                    return false;
                }

                @Override
                public boolean isDone() {
                    return true;
                }

                @Override
                public BulkImportResponse get() throws InterruptedException, ExecutionException {
                    return response;
                }

                @Override
                public BulkImportResponse get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                    return response;
                }
            };
        }
    }
}
