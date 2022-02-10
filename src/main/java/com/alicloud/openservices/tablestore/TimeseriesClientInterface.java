package com.alicloud.openservices.tablestore;

import com.alicloud.openservices.tablestore.model.timeseries.*;

public interface TimeseriesClientInterface {

    CreateTimeseriesTableResponse createTimeseriesTable(CreateTimeseriesTableRequest request);

    ListTimeseriesTableResponse listTimeseriesTable();

    DeleteTimeseriesTableResponse deleteTimeseriesTable(DeleteTimeseriesTableRequest request);

    DescribeTimeseriesTableResponse describeTimeseriesTable(DescribeTimeseriesTableRequest request);

    UpdateTimeseriesTableResponse updateTimeseriesTable(UpdateTimeseriesTableRequest request);

    PutTimeseriesDataResponse putTimeseriesData(PutTimeseriesDataRequest request)
            throws TableStoreException, ClientException;

    GetTimeseriesDataResponse getTimeseriesData(GetTimeseriesDataRequest request)
            throws TableStoreException, ClientException;

    QueryTimeseriesMetaResponse queryTimeseriesMeta(QueryTimeseriesMetaRequest request)
            throws TableStoreException, ClientException;

    UpdateTimeseriesMetaResponse updateTimeseriesMeta(UpdateTimeseriesMetaRequest request)
            throws TableStoreException, ClientException;

    DeleteTimeseriesMetaResponse deleteTimeseriesMeta(DeleteTimeseriesMetaRequest request)
            throws TableStoreException, ClientException;
}
