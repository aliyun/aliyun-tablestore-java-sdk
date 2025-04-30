package com.alicloud.openservices.tablestore.timeserieswriter.mocktest;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.model.IndexMeta;
import com.alicloud.openservices.tablestore.model.Response;
import com.alicloud.openservices.tablestore.model.TimeseriesTableMeta;
import com.alicloud.openservices.tablestore.model.timeseries.*;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MockClient extends AsyncTimeseriesClient {

    private String MOCK_INDEX_NAME = "MOCK_INDEX_NAME";
    private String MOCK_TABLE_NAME = "MOCK_TABLE_NAME";
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

            try{
                Thread.sleep(10);
            }catch (Exception e){

            }
            Response meta = new Response();
            meta.setRequestId("mockRequestId");

            final PutTimeseriesDataResponse response = new PutTimeseriesDataResponse(meta);


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
