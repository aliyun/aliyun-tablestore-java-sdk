package com.alicloud.openservices.tablestore.timeserieswriter.callback;

import com.alicloud.openservices.tablestore.AsyncTimeseriesClientInterface;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.model.timeseries.PutTimeseriesDataRequest;
import com.alicloud.openservices.tablestore.model.timeseries.PutTimeseriesDataResponse;
import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesTableRow;
import com.alicloud.openservices.tablestore.timeserieswriter.config.TimeseriesBucketConfig;
import com.alicloud.openservices.tablestore.timeserieswriter.config.TimeseriesWriterConfig;
import com.alicloud.openservices.tablestore.timeserieswriter.group.TimeseriesGroup;
import com.alicloud.openservices.tablestore.timeserieswriter.handle.TimeseriesWriterHandleStatistics;


import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

public class TimeseriesFlushCallbackFactory implements TimeseriesCallbackFactory {

    protected AsyncTimeseriesClientInterface ots;
    protected TimeseriesBucketConfig timeseriesBucketConfig;
    protected TimeseriesWriterConfig timeseriesWriterConfig;
    protected Semaphore callbackSemaphore;
    protected TableStoreCallback<TimeseriesTableRow, TimeseriesRowResult> callback;


    protected Executor executor;
    protected TimeseriesWriterHandleStatistics timeseriesWriterHandleStatistics;
    protected Semaphore bucketSemaphore;

    public TimeseriesFlushCallbackFactory(AsyncTimeseriesClientInterface ots, Semaphore callbackSemaphore,
                                          TableStoreCallback<TimeseriesTableRow, TimeseriesRowResult> callback,
                                          Executor executor, TimeseriesWriterHandleStatistics timeseriesWriterHandleStatistics,
                                          TimeseriesBucketConfig timeseriesBucketConfig, Semaphore bucketSemaphore) {
        this.ots = ots;
        this.callbackSemaphore = callbackSemaphore;
        this.timeseriesBucketConfig = timeseriesBucketConfig;
        this.callback = callback;
        this.executor = executor;
        this.timeseriesWriterHandleStatistics = timeseriesWriterHandleStatistics;
        this.bucketSemaphore = bucketSemaphore;
    }


    @Override
    public TableStoreCallback newInstance(List<TimeseriesGroup> groupList) {
        return new TimeseriesFlushCallback<PutTimeseriesDataRequest, PutTimeseriesDataResponse>(
                ots, new AtomicInteger(1), callbackSemaphore, callback, executor,
                timeseriesWriterHandleStatistics, timeseriesBucketConfig, bucketSemaphore, groupList);
    }
}
