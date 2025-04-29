package com.alicloud.openservices.tablestore.timeserieswriter;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.core.ResourceManager;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.model.Error;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.timeseries.*;
import com.alicloud.openservices.tablestore.timeserieswriter.callback.TimeseriesRowResult;
import com.alicloud.openservices.tablestore.timeserieswriter.config.TimeseriesWriterConfig;
import com.alicloud.openservices.tablestore.writer.WriterConfig;
import com.alicloud.openservices.tablestore.writer.enums.BatchRequestType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;


public class TestTsWPartitionFailed {
    private static ServiceSettings serviceSettings = ServiceSettings.load();
    private static String MOCK_TABLE_NAME = "mockTableName";
    private static String MOCK_INDEX_NAME = "mockIndexName";
    private static AtomicLong succeedRows = new AtomicLong();
    private static AtomicLong failedRows = new AtomicLong();
    final ExecutorService executor = Executors.newFixedThreadPool(10);

    private int rowSize = 200;
    private AsyncTimeseriesClient ots;
    private TableStoreTimeseriesWriter writer;

    @Before
    public void setUp() throws Exception {
        succeedRows.getAndSet(0);
        failedRows.getAndSet(0);
        ots = new MockClient(
                serviceSettings.getOTSEndpoint(),
                serviceSettings.getOTSAccessKeyId(),
                serviceSettings.getOTSAccessKeySecret(),
                serviceSettings.getOTSInstanceName());
        final TimeseriesWriterConfig config = new TimeseriesWriterConfig();
        config.setBucketCount(1);
        writer = createMockWriter(ots, config, executor);
    }

    @After
    public void after() throws Exception {
        ots.shutdown();
        executor.shutdown();
    }


    @Test
    public void testTimeseriesRowPartitionFailed() throws Exception {

        for (int i = 0; i < rowSize; i++) {
            Map<String, String> tags = new HashMap<String, String>();
            tags.put("region", "hangzhou");
            tags.put("os", "Ubuntu16.04");
            TimeseriesKey timeseriesKey = new TimeseriesKey("cpu" + i, "host_" + i, tags);
            TimeseriesRow row = new TimeseriesRow(timeseriesKey, System.currentTimeMillis() * 1000 + i);

            writer.addTimeseriesRowChange(new TimeseriesTableRow(row, MOCK_TABLE_NAME));
        }

        writer.flush();

        Thread.sleep(100);

        Assert.assertEquals(failedRows.get(), rowSize / 2);
        Assert.assertEquals(succeedRows.get(), rowSize / 2);
    }


    private static DefaultTableStoreTimeseriesWriter createMockWriter(AsyncTimeseriesClient ots, TimeseriesWriterConfig config, ExecutorService executor) {


        TableStoreCallback<TimeseriesTableRow, TimeseriesRowResult> callback = new TableStoreCallback<TimeseriesTableRow, TimeseriesRowResult>() {
            @Override
            public void onCompleted(TimeseriesTableRow timeseriesRow, TimeseriesRowResult result) {
                succeedRows.incrementAndGet();
            }

            @Override
            public void onFailed(TimeseriesTableRow timeseriesRow, Exception ex) {
                ex.printStackTrace();
                failedRows.incrementAndGet();
            }
        };
        return new DefaultTableStoreTimeseriesWriter(ots, config, callback, executor);
    }


    public class MockClient extends AsyncTimeseriesClient {
        public MockClient(String endpoint, String accessKeyId, String accessKeySecret, String instanceName) {
            super(endpoint, accessKeyId, accessKeySecret, instanceName);
        }

        @Override
        public void shutdown() {
            super.shutdown();
        }

        @Override
        public void setExtraHeaders(Map<String, String> extraHeaders) {
            super.setExtraHeaders(extraHeaders);
        }

        @Override
        public Future<CreateTimeseriesTableResponse> createTimeseriesTable(CreateTimeseriesTableRequest request, TableStoreCallback<CreateTimeseriesTableRequest, CreateTimeseriesTableResponse> callback) {
            return super.createTimeseriesTable(request, callback);
        }

        @Override
        public Future<ListTimeseriesTableResponse> listTimeseriesTable(TableStoreCallback<ListTimeseriesTableRequest, ListTimeseriesTableResponse> callback) {
            return super.listTimeseriesTable(callback);
        }

        @Override
        public Future<DeleteTimeseriesTableResponse> deleteTimeseriesTable(DeleteTimeseriesTableRequest request, TableStoreCallback<DeleteTimeseriesTableRequest, DeleteTimeseriesTableResponse> callback) {
            return super.deleteTimeseriesTable(request, callback);
        }

        @Override
        public Future<DescribeTimeseriesTableResponse> describeTimeseriesTable(DescribeTimeseriesTableRequest request, TableStoreCallback<DescribeTimeseriesTableRequest, DescribeTimeseriesTableResponse> callback) {
            Response meta = new Response();
            meta.setRequestId("mockRequestId");

            final DescribeTimeseriesTableResponse response = new DescribeTimeseriesTableResponse(meta);
            TimeseriesTableMeta tableMeta = new TimeseriesTableMeta(MOCK_TABLE_NAME);

            IndexMeta indexMeta = new IndexMeta(MOCK_INDEX_NAME);

            response.setTimeseriesTableMeta(tableMeta);

            if (callback != null) {
                callback.onCompleted(request, response);
            }

            return new Future<DescribeTimeseriesTableResponse>() {
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
                public DescribeTimeseriesTableResponse get() throws InterruptedException, ExecutionException {
                    return response;
                }

                @Override
                public DescribeTimeseriesTableResponse get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                    return response;
                }
            };
        }

        @Override
        public Future<UpdateTimeseriesTableResponse> updateTimeseriesTable(UpdateTimeseriesTableRequest request, TableStoreCallback<UpdateTimeseriesTableRequest, UpdateTimeseriesTableResponse> callback) {
            return super.updateTimeseriesTable(request, callback);
        }

        @Override
        public Future<GetTimeseriesDataResponse> getTimeseriesData(GetTimeseriesDataRequest request, TableStoreCallback<GetTimeseriesDataRequest, GetTimeseriesDataResponse> callback) throws TableStoreException, ClientException {
            return super.getTimeseriesData(request, callback);
        }

        @Override
        public Future<QueryTimeseriesMetaResponse> queryTimeseriesMeta(QueryTimeseriesMetaRequest request, TableStoreCallback<QueryTimeseriesMetaRequest, QueryTimeseriesMetaResponse> callback) throws TableStoreException, ClientException {
            return super.queryTimeseriesMeta(request, callback);
        }

        @Override
        public Future<UpdateTimeseriesMetaResponse> updateTimeseriesMeta(UpdateTimeseriesMetaRequest request, TableStoreCallback<UpdateTimeseriesMetaRequest, UpdateTimeseriesMetaResponse> callback) throws TableStoreException, ClientException {
            return super.updateTimeseriesMeta(request, callback);
        }

        @Override
        public Future<DeleteTimeseriesMetaResponse> deleteTimeseriesMeta(DeleteTimeseriesMetaRequest request, TableStoreCallback<DeleteTimeseriesMetaRequest, DeleteTimeseriesMetaResponse> callback) throws TableStoreException, ClientException {
            return super.deleteTimeseriesMeta(request, callback);
        }

        @Override
        public Future<SplitTimeseriesScanTaskResponse> splitTimeseriesScanTask(SplitTimeseriesScanTaskRequest request, TableStoreCallback<SplitTimeseriesScanTaskRequest, SplitTimeseriesScanTaskResponse> callback) throws TableStoreException, ClientException {
            return super.splitTimeseriesScanTask(request, callback);
        }

        @Override
        public Future<ScanTimeseriesDataResponse> scanTimeseriesData(ScanTimeseriesDataRequest request, TableStoreCallback<ScanTimeseriesDataRequest, ScanTimeseriesDataResponse> callback) throws TableStoreException, ClientException {
            return super.scanTimeseriesData(request, callback);
        }

        @Override
        public TimeseriesClientInterface asTimeseriesClientInterface() {
            return super.asTimeseriesClientInterface();
        }

        @Override
        public SyncClient asSyncClient() {
            return super.asSyncClient();
        }

        @Override
        public AsyncClient asAsyncClient() {
            return super.asAsyncClient();
        }

        @Override
        public TimeseriesClient asTimeseriesClient() {
            return super.asTimeseriesClient();
        }

        @Override
        public Future<PutTimeseriesDataResponse> putTimeseriesData(final PutTimeseriesDataRequest request, TableStoreCallback<PutTimeseriesDataRequest, PutTimeseriesDataResponse> callback) throws TableStoreException, ClientException {
            {
                Response meta = new Response();
                meta.setRequestId("mockRequestId");

                final PutTimeseriesDataResponse response = new PutTimeseriesDataResponse(meta);
                List<PutTimeseriesDataResponse.FailedRowResult> rowResultList = new ArrayList<PutTimeseriesDataResponse.FailedRowResult>();
                for (int i = 0; i < rowSize; i++) {

                    if (i % 2 == 0) {
                        Error error = new Error("500", "mock partition failed");
                        PutTimeseriesDataResponse.FailedRowResult result = new PutTimeseriesDataResponse.FailedRowResult(i, error);
                        rowResultList.add(result);
                    }

                }
                response.setFailedRows(rowResultList);

                if (callback != null) {
                    callback.onCompleted(request, response);
                }

                return new Future<PutTimeseriesDataResponse>() {
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
                    public PutTimeseriesDataResponse get() throws InterruptedException, ExecutionException {
                        return response;
                    }

                    @Override
                    public PutTimeseriesDataResponse get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                        return response;
                    }
                };
            }

        }


    }
}
