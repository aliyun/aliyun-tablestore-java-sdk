package com.alicloud.openservices.tablestore.timeserieswriter.handle;

import com.alicloud.openservices.tablestore.AsyncTimeseriesClientInterface;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesTableRow;
import com.alicloud.openservices.tablestore.timeserieswriter.TimeseriesRowEvent;
import com.alicloud.openservices.tablestore.timeserieswriter.callback.TimeseriesRowResult;
import com.alicloud.openservices.tablestore.timeserieswriter.config.TimeseriesBucketConfig;
import com.alicloud.openservices.tablestore.timeserieswriter.config.TimeseriesWriterConfig;
import com.alicloud.openservices.tablestore.timeserieswriter.group.TimeseriesRequestWithGroups;
import com.alicloud.openservices.tablestore.timeserieswriter.group.TimeseriesRowWithGroup;
import com.alicloud.openservices.tablestore.timeserieswriter.manager.TimeseriesBatchRequestManager;
import com.alicloud.openservices.tablestore.timeserieswriter.manager.TimeseriesRequestManager;
import com.lmax.disruptor.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

public class TimeseriesRowEventHandler implements EventHandler<TimeseriesRowEvent> {

    private final Logger logger = LoggerFactory.getLogger(TimeseriesRowEventHandler.class);

    private final AsyncTimeseriesClientInterface ots;
    private final Executor executor;
    private final TimeseriesWriterHandleStatistics timeseriesWriterHandleStatistics;
    private final Semaphore callbackSemaphore;
    private final Semaphore bucketSemaphore;
    private final Map<String, TimeseriesRequestManager> map;
    private final int bucketConcurrency;
    private final TimeseriesWriterConfig timeseriesWriterConfig;
    private final TimeseriesBucketConfig timeseriesBucketConfig;
    private TableStoreCallback<TimeseriesTableRow, TimeseriesRowResult> callback;

    public TimeseriesRowEventHandler(
            AsyncTimeseriesClientInterface ots,
            TimeseriesWriterConfig timeseriesWriterConfig,
            TimeseriesBucketConfig timeseriesBucketConfig,
            TableStoreCallback<TimeseriesTableRow, TimeseriesRowResult> callback,
            Executor executor,
            TimeseriesWriterHandleStatistics timeseriesWriterHandleStatistics,
            Semaphore semaphore) {
        this.ots = ots;
        this.timeseriesWriterConfig = timeseriesWriterConfig;
        this.timeseriesBucketConfig = timeseriesBucketConfig;
        int concurrency = timeseriesWriterConfig.getConcurrency();
        this.callbackSemaphore = semaphore;
        this.callback = callback;
        this.executor = executor;
        this.timeseriesWriterHandleStatistics = timeseriesWriterHandleStatistics;


        switch (timeseriesWriterConfig.getWriteMode()) {
            case SEQUENTIAL:
                bucketConcurrency = 1;
                break;
            case PARALLEL:
            default:
                bucketConcurrency = concurrency;
                break;
        }
        bucketSemaphore = new Semaphore(bucketConcurrency);

        map = new HashMap<String, TimeseriesRequestManager>();
    }

    public void setCallback(TableStoreCallback<TimeseriesTableRow, TimeseriesRowResult> callback) {
        this.callback = callback;
        for (Map.Entry<String, TimeseriesRequestManager> entry : map.entrySet()) {
            entry.setValue(new TimeseriesBatchRequestManager(this.ots, timeseriesWriterConfig, this.timeseriesBucketConfig, executor,
                    timeseriesWriterHandleStatistics,
                    callback,
                    callbackSemaphore, bucketSemaphore));
        }
    }

    @Override
    public void onEvent(TimeseriesRowEvent timeseriesRowEvent, long l, boolean b) throws Exception {
        boolean shouldWaitFlush = false;
        CountDownLatch latch = null;
        Map<String, TimeseriesRequestWithGroups> timeseriesRequestWithGroupsMap = new HashMap<String, TimeseriesRequestWithGroups>();

        if (timeseriesRowEvent.type == TimeseriesRowEvent.EventType.FLUSH) {
            int totalRowsCount = 0;
            for (Map.Entry<String, TimeseriesRequestManager> entry : map.entrySet()) {
                if (entry.getValue().getTotalRowsCount() > 0) {
                    timeseriesRequestWithGroupsMap.put(entry.getKey(), entry.getValue().makeRequest(entry.getKey()));
                    totalRowsCount += entry.getValue().getTotalRowsCount();
                }
            }

            logger.debug("FlushSignal with QueueSize: {}", totalRowsCount);
            shouldWaitFlush = true;
            latch = timeseriesRowEvent.latch;
        } else {
            timeseriesWriterHandleStatistics.totalRowsCount.incrementAndGet();
            final TimeseriesTableRow timeseriesTableRow = timeseriesRowEvent.timeseriesTableRow;
            final TimeseriesRowWithGroup timeseriesRowWithGroup = new TimeseriesRowWithGroup(timeseriesRowEvent.timeseriesTableRow, timeseriesRowEvent.timeseriesGroup);
            if (!map.containsKey(timeseriesTableRow.getTableName())) {
                map.put(timeseriesTableRow.getTableName(), new TimeseriesBatchRequestManager(this.ots, timeseriesWriterConfig, this.timeseriesBucketConfig, executor,
                        timeseriesWriterHandleStatistics,
                        callback,
                        callbackSemaphore, bucketSemaphore));
            }
            boolean succeed = map.get(timeseriesTableRow.getTableName()).appendTimeseriesRow(timeseriesRowWithGroup);
            // The first failure may be due to:
            // 1. The number of rows has reached the maximum limit
            // 2. The size of rows has reached the maximum limit
            // 3. Duplicate timeseries key in cache
            // 4. The format of the row is incorrect
            if (!succeed) {
                // For the case of 1、2、3 above, make request and try to add row to the cache again
                TimeseriesRequestWithGroups timeseriesRequestWithGroups = map.get(timeseriesTableRow.getTableName()).makeRequest(timeseriesTableRow.getTableName());
                if (timeseriesRequestWithGroups != null) {
                    timeseriesRequestWithGroupsMap.put(timeseriesTableRow.getTableName(), timeseriesRequestWithGroups);
                }
                // The second failure may be due to:
                // 1. Only one row size reaches the maximum limit
                // 2. The format of the row is incorrect
                succeed = map.get(timeseriesTableRow.getTableName()).appendTimeseriesRow(timeseriesRowWithGroup);
                if (!succeed) {
                    executor.execute(new Runnable() {
                        @Override
                        public void run() {
                            timeseriesWriterHandleStatistics.totalFailedRowsCount.incrementAndGet();
                            ClientException exception = new ClientException("Failed to append timeseries row into buffer.");
                            logger.error("RowChange Failed: ", exception);
                            timeseriesRowWithGroup.timeseriesGroup.failedOneRow(timeseriesRowWithGroup.timeseriesTableRow, exception);
                            if (callback != null) {
                                callback.onFailed(timeseriesTableRow, exception);
                            }
                        }
                    });
                }
            }
        }

        if (!timeseriesRequestWithGroupsMap.isEmpty()) {

            for (final Map.Entry<String, TimeseriesRequestWithGroups> entry : timeseriesRequestWithGroupsMap.entrySet()) {

                bucketSemaphore.acquire();      // 先阻塞等候桶信号量
                callbackSemaphore.acquire();    // 后阻塞等候线程池信号量
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        timeseriesWriterHandleStatistics.totalRequestCount.incrementAndGet();
                        map.get(entry.getKey()).sendRequest(entry.getValue());
                    }
                });
            }
        }

        if (shouldWaitFlush) {
            bucketSemaphore.acquire(bucketConcurrency);
            bucketSemaphore.release(bucketConcurrency);
            logger.debug("Finish bucket waitFlush.");
            latch.countDown();
        }
    }
}
