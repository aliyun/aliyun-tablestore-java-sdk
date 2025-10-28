package com.alicloud.openservices.tablestore.timeserieswriter.manager;

import com.alicloud.openservices.tablestore.AsyncTimeseriesClientInterface;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesKey;
import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesTableRow;
import com.alicloud.openservices.tablestore.timeserieswriter.callback.TimeseriesCallbackFactory;
import com.alicloud.openservices.tablestore.timeserieswriter.callback.TimeseriesFlushCallbackFactory;
import com.alicloud.openservices.tablestore.timeserieswriter.callback.TimeseriesRowResult;
import com.alicloud.openservices.tablestore.timeserieswriter.group.TimeseriesRowWithGroup;
import com.alicloud.openservices.tablestore.timeserieswriter.config.TimeseriesBucketConfig;
import com.alicloud.openservices.tablestore.timeserieswriter.config.TimeseriesWriterConfig;
import com.alicloud.openservices.tablestore.timeserieswriter.handle.TimeseriesWriterHandleStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

public abstract class TimeseriesBaseRequestManager implements TimeseriesRequestManager {
    private static Logger logger = LoggerFactory.getLogger(TimeseriesBaseRequestManager.class);

    protected AsyncTimeseriesClientInterface ots;
    protected TimeseriesBucketConfig timeseriesBucketConfig;
    protected TimeseriesWriterConfig timeseriesWriterConfig;
    protected Semaphore callbackSemaphore;
    protected TableStoreCallback<TimeseriesTableRow, TimeseriesRowResult> callback;
    protected Executor executor;
    protected TimeseriesWriterHandleStatistics timeseriesWriterHandleStatistics;
    protected Semaphore bucketSemaphore;

    protected TimeseriesCallbackFactory callbackFactory;
    protected List<TimeseriesRowWithGroup> timeseriesRowWithGroups = new LinkedList<TimeseriesRowWithGroup>();

    protected int totalSize;
    protected int totalRowsCount;



    protected boolean allowDuplicatedRowInBatchRequest;
    protected ConcurrentSkipListSet<TimeseriesKey> sendingTimeseriesKeys = new ConcurrentSkipListSet<TimeseriesKey>();

    public TimeseriesBaseRequestManager(AsyncTimeseriesClientInterface ots, TimeseriesWriterConfig timeseriesWriterConfig, TimeseriesBucketConfig timeseriesBucketConfig, Executor executor,
                                        TimeseriesWriterHandleStatistics timeseriesWriterHandleStatistics,
                                        TableStoreCallback<TimeseriesTableRow, TimeseriesRowResult> callback,
                                        Semaphore callbackSemaphore, Semaphore bucketSemaphore) {
        this.ots = ots;
        this.timeseriesWriterConfig = timeseriesWriterConfig;
        this.timeseriesBucketConfig = timeseriesBucketConfig;
        this.executor = executor;
        this.timeseriesWriterHandleStatistics = timeseriesWriterHandleStatistics;
        this.callback = callback;
        this.callbackSemaphore = callbackSemaphore;
        this.bucketSemaphore = bucketSemaphore;
        this.totalSize = 0;
        this.totalRowsCount = 0;
        this.allowDuplicatedRowInBatchRequest = timeseriesBucketConfig.isAllowDuplicateRowInBatchRequest();
        this.callbackFactory =  new TimeseriesFlushCallbackFactory(ots, callbackSemaphore, callback, executor,
                timeseriesWriterHandleStatistics, timeseriesBucketConfig, bucketSemaphore);
    }

    @Override
    public boolean appendTimeseriesRow(TimeseriesRowWithGroup timeseriesRowWithGroup) {
        try {
            if (totalSize + timeseriesRowWithGroup.timeseriesTableRow.getTimeseriesRow().getTimeseriesRowDataSize() > timeseriesWriterConfig.getMaxBatchSize()) {
                return false;
            }

            if (totalRowsCount >= timeseriesWriterConfig.getMaxBatchRowsCount()) {
                return false;
            }

            if (!allowDuplicatedRowInBatchRequest) {
                if (sendingTimeseriesKeys.contains(timeseriesRowWithGroup.timeseriesTableRow.getTimeseriesRow().getTimeseriesKey())) {
                    return false;
                } else {
                    sendingTimeseriesKeys.add(timeseriesRowWithGroup.timeseriesTableRow.getTimeseriesRow().getTimeseriesKey());
                }
            }

            timeseriesRowWithGroups.add(timeseriesRowWithGroup);
            this.totalSize += timeseriesRowWithGroup.timeseriesTableRow.getTimeseriesRow().getTimeseriesRowDataSize();
            this.totalRowsCount += 1;
            return true;
        } catch (Exception e) {
            logger.error("Failed while append TimeseriesRow:", e);
            return false;
        }
    }


    @Override
    public int getTotalRowsCount() {
        return totalRowsCount;
    }

    @Override
    public void clear() {
        timeseriesRowWithGroups.clear();
        sendingTimeseriesKeys.clear();
        totalSize = 0;
        totalRowsCount = 0;
    }
}
