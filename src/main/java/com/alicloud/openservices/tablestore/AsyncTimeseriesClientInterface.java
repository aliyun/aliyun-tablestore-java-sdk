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
}
