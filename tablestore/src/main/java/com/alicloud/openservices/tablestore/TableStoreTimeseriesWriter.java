package com.alicloud.openservices.tablestore;

import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesTableRow;
import com.alicloud.openservices.tablestore.timeserieswriter.TimeseriesWriterResult;
import com.alicloud.openservices.tablestore.timeserieswriter.callback.TimeseriesRowResult;
import com.alicloud.openservices.tablestore.timeserieswriter.config.TimeseriesWriterConfig;
import com.alicloud.openservices.tablestore.timeserieswriter.handle.TimeseriesWriterHandleStatistics;


import java.util.List;
import java.util.concurrent.Future;

public interface TableStoreTimeseriesWriter {

    void addTimeseriesRowChange(TimeseriesTableRow timeseriesTableRow) throws ClientException;

    Future<TimeseriesWriterResult> addTimeseriesRowChangeWithFuture(TimeseriesTableRow timeseriesTableRow) throws ClientException;

    boolean tryAddTimeseriesRowChange(TimeseriesTableRow timeseriesTableRow) throws ClientException;

    void addTimeseriesRowChange(List<TimeseriesTableRow> timeseriesTableRows, List<TimeseriesTableRow> ditryRows) throws ClientException;

    Future<TimeseriesWriterResult> addTimeseriesRowChangeWithFuture(List<TimeseriesTableRow> timeseriesTableRows) throws ClientException;

    void setResultCallback(TableStoreCallback<TimeseriesTableRow, TimeseriesRowResult> callback);

    TableStoreCallback<TimeseriesTableRow, TimeseriesRowResult> getResultCallback();

    TimeseriesWriterConfig getTimeseriesWriterConfig();

    TimeseriesWriterHandleStatistics getTimeseriesWriterStatistics();

    void flush() throws ClientException;

    void close();
}
