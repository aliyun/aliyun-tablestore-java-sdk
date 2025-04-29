package com.alicloud.openservices.tablestore.writer;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.model.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;


public class TestExceptionFailedSingleRetry {
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
    private TableStoreWriter writer;


    @Before
    public void setUp() throws Exception  {
        succeedRows.getAndSet(0);
        failedRows.getAndSet(0);
        ots = new UnknownExceptionMockClient(
                serviceSettings.getOTSEndpoint(),
                serviceSettings.getOTSAccessKeyId(),
                serviceSettings.getOTSAccessKeySecret(),
                serviceSettings.getOTSInstanceName());
        final WriterConfig batchWriteRowConfig = new WriterConfig();
        batchWriteRowConfig.setBucketCount(1);
        writer = createMockWriter(ots, batchWriteRowConfig, executor);
    }

    @After
    public void after() throws Exception {
        ots.shutdown();
        executor.shutdown();
    }


    @Test
    public void testUnknownExceptionFailed() throws Exception {

        for (int i = 0; i < rowSize / 3; i++) {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn(PK, PrimaryKeyValue.fromLong(i))
                    .build();

            RowPutChange rowChange = new RowPutChange(MOCK_TABLE_NAME, pk);
            rowChange.addColumn(COL_1, ColumnValue.fromLong(i));

            writer.addRowChange(rowChange);
        }

        for (int i = rowSize / 3; i < 2 * (rowSize / 3); i++) {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn(PK, PrimaryKeyValue.fromLong(i))
                    .build();

            RowUpdateChange rowChange = new RowUpdateChange(MOCK_TABLE_NAME, pk);
            rowChange.put(COL_1, ColumnValue.fromLong(i));

            writer.addRowChange(rowChange);
        }

        for (int i = 2 * (rowSize / 3); i < rowSize; i++) {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn(PK, PrimaryKeyValue.fromLong(i))
                    .build();

            RowDeleteChange rowChange = new RowDeleteChange(MOCK_TABLE_NAME, pk);

            writer.addRowChange(rowChange);
        }

        writer.flush();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Assert.fail();
        }

        Assert.assertEquals(succeedRows.get(), 0);
        Assert.assertEquals(failedRows.get(), rowSize);
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



    public class UnknownExceptionMockClient extends AsyncClient {
        public UnknownExceptionMockClient(String endpoint, String accessKeyId, String accessKeySecret, String instanceName) {
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

            if (callback != null) {
                callback.onFailed(request, new TableStoreException("UnknownException", "retry"));
            }

            return null;
        }

        @Override
        public Future<PutRowResponse> putRow(
                final PutRowRequest request,
                TableStoreCallback<PutRowRequest, PutRowResponse> callback) {

            if (callback != null) {
                callback.onFailed(request, new Exception("UnknownException"));
            }

            return null;
        }

        @Override
        public Future<UpdateRowResponse> updateRow(
                final UpdateRowRequest request,
                TableStoreCallback<UpdateRowRequest, UpdateRowResponse> callback) {

            if (callback != null) {
                callback.onFailed(request, new Exception("UnknownException"));
            }

            return null;
        }

        @Override
        public Future<DeleteRowResponse> deleteRow(
                final DeleteRowRequest request,
                TableStoreCallback<DeleteRowRequest, DeleteRowResponse> callback) {

            if (callback != null) {
                callback.onFailed(request, new Exception("UnknownException"));
            }

            return null;
        }
    }
}
