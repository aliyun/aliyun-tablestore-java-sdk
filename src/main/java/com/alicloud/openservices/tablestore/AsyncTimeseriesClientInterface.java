package com.alicloud.openservices.tablestore;

import com.alicloud.openservices.tablestore.model.timeseries.*;

import java.util.concurrent.Future;

public interface AsyncTimeseriesClientInterface {

    Future<CreateTimeseriesTableResponse> createTimeseriesTable(CreateTimeseriesTableRequest request,
                                                                TableStoreCallback<CreateTimeseriesTableRequest, CreateTimeseriesTableResponse> callback);

    Future<ListTimeseriesTableResponse> listTimeseriesTable(TableStoreCallback<ListTimeseriesTableRequest, ListTimeseriesTableResponse> callback);

    Future<DeleteTimeseriesTableResponse> deleteTimeseriesTable(DeleteTimeseriesTableRequest request,
                                                                TableStoreCallback<DeleteTimeseriesTableRequest, DeleteTimeseriesTableResponse> callback);

    Future<DescribeTimeseriesTableResponse> describeTimeseriesTable(DescribeTimeseriesTableRequest request,
                                                                    TableStoreCallback<DescribeTimeseriesTableRequest, DescribeTimeseriesTableResponse> callback);

    Future<UpdateTimeseriesTableResponse> updateTimeseriesTable(UpdateTimeseriesTableRequest request,
                                                                TableStoreCallback<UpdateTimeseriesTableRequest, UpdateTimeseriesTableResponse> callback);

    Future<PutTimeseriesDataResponse> putTimeseriesData(PutTimeseriesDataRequest request,
                                                        TableStoreCallback<PutTimeseriesDataRequest, PutTimeseriesDataResponse> callback)
            throws TableStoreException, ClientException;

    Future<GetTimeseriesDataResponse> getTimeseriesData(GetTimeseriesDataRequest request,
                                                        TableStoreCallback<GetTimeseriesDataRequest, GetTimeseriesDataResponse> callback)
            throws TableStoreException, ClientException;

    Future<QueryTimeseriesMetaResponse> queryTimeseriesMeta(QueryTimeseriesMetaRequest request,
                                                            TableStoreCallback<QueryTimeseriesMetaRequest, QueryTimeseriesMetaResponse> callback)
            throws TableStoreException, ClientException;

    Future<UpdateTimeseriesMetaResponse> updateTimeseriesMeta(UpdateTimeseriesMetaRequest request,
                                                              TableStoreCallback<UpdateTimeseriesMetaRequest, UpdateTimeseriesMetaResponse> callback)
            throws TableStoreException, ClientException;

    Future<DeleteTimeseriesMetaResponse> deleteTimeseriesMeta(DeleteTimeseriesMetaRequest request,
                                                              TableStoreCallback<DeleteTimeseriesMetaRequest, DeleteTimeseriesMetaResponse> callback)
            throws TableStoreException, ClientException;

    Future<SplitTimeseriesScanTaskResponse> splitTimeseriesScanTask(SplitTimeseriesScanTaskRequest request,
                                                                    TableStoreCallback<SplitTimeseriesScanTaskRequest, SplitTimeseriesScanTaskResponse> callback)
            throws TableStoreException, ClientException;

    Future<ScanTimeseriesDataResponse> scanTimeseriesData(ScanTimeseriesDataRequest request,
                                                          TableStoreCallback<ScanTimeseriesDataRequest, ScanTimeseriesDataResponse> callback)
            throws TableStoreException, ClientException;

    Future<CreateTimeseriesAnalyticalStoreResponse> createTimeseriesAnalyticalStore(CreateTimeseriesAnalyticalStoreRequest request,
                                                                                    TableStoreCallback<CreateTimeseriesAnalyticalStoreRequest, CreateTimeseriesAnalyticalStoreResponse> callback)
            throws TableStoreException, ClientException;

    Future<DeleteTimeseriesAnalyticalStoreResponse> deleteTimeseriesAnalyticalStore(DeleteTimeseriesAnalyticalStoreRequest request,
                                                                                    TableStoreCallback<DeleteTimeseriesAnalyticalStoreRequest, DeleteTimeseriesAnalyticalStoreResponse> callback)
            throws TableStoreException, ClientException;

    Future<DescribeTimeseriesAnalyticalStoreResponse> describeTimeseriesAnalyticalStore(DescribeTimeseriesAnalyticalStoreRequest request,
                                                                                        TableStoreCallback<DescribeTimeseriesAnalyticalStoreRequest, DescribeTimeseriesAnalyticalStoreResponse> callback)
            throws TableStoreException, ClientException;

    Future<UpdateTimeseriesAnalyticalStoreResponse> updateTimeseriesAnalyticalStore(UpdateTimeseriesAnalyticalStoreRequest request,
                                                                                    TableStoreCallback<UpdateTimeseriesAnalyticalStoreRequest, UpdateTimeseriesAnalyticalStoreResponse> callback)
            throws TableStoreException, ClientException;

    Future<CreateTimeseriesLastpointIndexResponse> createTimeseriesLastpointIndex(CreateTimeseriesLastpointIndexRequest request,
                                                                                  TableStoreCallback<CreateTimeseriesLastpointIndexRequest, CreateTimeseriesLastpointIndexResponse> callback)
            throws TableStoreException, ClientException;

    Future<DeleteTimeseriesLastpointIndexResponse> deleteTimeseriesLastpointIndex(DeleteTimeseriesLastpointIndexRequest request,
                                                                                  TableStoreCallback<DeleteTimeseriesLastpointIndexRequest, DeleteTimeseriesLastpointIndexResponse> callback)
            throws TableStoreException, ClientException;

    public TimeseriesClientInterface asTimeseriesClientInterface();

    /**
     * 释放资源。
     * <p>请确保在所有请求执行完毕之后释放资源。释放资源之后将不能再发送请求，正在执行的请求可能无法返回结果。</p>
     */
    public void shutdown();
}
