package com.alicloud.openservices.tablestore.timeserieswriter.callback;

import com.alicloud.openservices.tablestore.AsyncTimeseriesClientInterface;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.timeseries.PutTimeseriesDataRequest;
import com.alicloud.openservices.tablestore.model.timeseries.PutTimeseriesDataResponse;
import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesRow;
import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesTableRow;
import com.alicloud.openservices.tablestore.timeserieswriter.TimeseriesWriterException;
import com.alicloud.openservices.tablestore.timeserieswriter.config.TimeseriesBucketConfig;
import com.alicloud.openservices.tablestore.timeserieswriter.enums.TSWriteMode;
import com.alicloud.openservices.tablestore.timeserieswriter.group.TimeseriesGroup;
import com.alicloud.openservices.tablestore.timeserieswriter.handle.TimeseriesWriterHandleStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TimeseriesFlushCallback<Req, Res> implements TableStoreCallback<Req, Res> {

    private Logger logger = LoggerFactory.getLogger(TimeseriesFlushCallback.class);

    private final AsyncTimeseriesClientInterface ots;

    private final AtomicInteger count;
    private final Semaphore semaphore;
    private final TimeseriesBucketConfig timeseriesBucketConfig;
    private final TableStoreCallback<TimeseriesTableRow, TimeseriesRowResult> callback;
    private final Executor executor;
    private final TimeseriesWriterHandleStatistics timeseriesWriterHandleStatistics;
    private final Semaphore bucketSemaphore;
    private final List<TimeseriesGroup> groupList;
    public static AtomicLong counter = new AtomicLong(0);

    public TimeseriesFlushCallback(AsyncTimeseriesClientInterface ots, AtomicInteger count, Semaphore semaphore,
                                   TableStoreCallback<TimeseriesTableRow, TimeseriesRowResult> callback,
                                   Executor executor, TimeseriesWriterHandleStatistics timeseriesWriterHandleStatistics,
                                   TimeseriesBucketConfig timeseriesBucketConfig, Semaphore bucketSemaphore, List<TimeseriesGroup> groupList) {
        this.ots = ots;
        this.count = count;
        this.semaphore = semaphore;
        this.timeseriesBucketConfig = timeseriesBucketConfig;
        this.executor = executor;
        this.callback = callback;
        this.timeseriesWriterHandleStatistics = timeseriesWriterHandleStatistics;
        this.bucketSemaphore = bucketSemaphore;
        this.groupList = groupList;

    }

    /*
    当前TimeseriesDataResponse 只返回错误信息，不返回正确的列信息
     */
    private void triggerSucceedCallback(final TimeseriesTableRow timeseriesTableRow, final TimeseriesGroup timeseriesGroup) {
        timeseriesWriterHandleStatistics.incrementAndGetTotalSucceedRowsCount();
        timeseriesGroup.succeedOneRow(timeseriesTableRow);
        if (callback == null) {
            return;
        }
        this.executor.execute(new Runnable() {
            public void run() {
                TimeseriesFlushCallback.this.callback.onCompleted(timeseriesTableRow, new TimeseriesRowResult(true, null));
            }
        });
    }

    private void triggerFailedCallback(final TimeseriesTableRow timeseriesTableRow, final Exception exp, final TimeseriesGroup group) {
        timeseriesWriterHandleStatistics.incrementAndGetTotalFailedRowsCount();
        group.failedOneRow(timeseriesTableRow, exp);
        logger.error("timeseriesRow Failed: ", exp);
        if (callback == null) {
            return;
        }

        executor.execute(new Runnable() {
            @Override
            public void run() {
                callback.onFailed(timeseriesTableRow, exp);
            }
        });
    }

    private void triggerFailedCallback(final List<TimeseriesTableRow> timeseriesTableRows, final Exception exp, final List<TimeseriesGroup> timeseriesGroupList) {
        timeseriesWriterHandleStatistics.addAndGetTotalFailedRowsCount(timeseriesTableRows.size());
        for (int i = 0; i < timeseriesTableRows.size(); i++) {
            TimeseriesTableRow timeseriesTableRow = timeseriesTableRows.get(i);
            TimeseriesGroup group = timeseriesGroupList.get(i);
            group.failedOneRow(timeseriesTableRow, exp);
            logger.error("timeseriesRow Failed: ", exp);
        }
        if (callback == null) {
            return;
        }

        executor.execute(new Runnable() {
            @Override
            public void run() {
                for (TimeseriesTableRow timeseriesTableRow : timeseriesTableRows) {
                    callback.onFailed(timeseriesTableRow, exp);
                }
            }
        });
    }

    /**
     * 当请求结束后，需要主动释放信号量。<br/>
     * 但是注意：这里不是每个请求完成都能去释放信号量。<br/>
     * 例如一次BatchWriteRow请求申请了一个信号量，但是由于其包含脏数据，所以需要对每一行数据单独发送一次请求来重试，此时一个并发会分裂为
     * N个并发，这N个并发请求会有自己独立的callback。而其申请的那一个信号量需要由这N个并发中的其中一个来释放，如何选择哪个来释放？
     * 这里采取的策略是用一个原子计数器，哪个请求最后一个完成，则由其来释放这个信号量。
     */
    private void requestComplete() {
        int remain = this.count.decrementAndGet();
        if (remain == 0) {
            logger.debug("BucketSemaphore Release: " + counter.incrementAndGet());
            semaphore.release();
            bucketSemaphore.release();
            logger.debug("Release semaphore.");
        }
    }

    public void onCompleted(PutTimeseriesDataRequest originRequest, PutTimeseriesDataResponse result) {

        List<PutTimeseriesDataResponse.FailedRowResult> failed = result.getFailedRows();

        List<Integer> failed_index = new ArrayList<Integer>();
        String requestTableName = originRequest.getTimeseriesTableName();

        for (PutTimeseriesDataResponse.FailedRowResult status : failed) {
            failed_index.add(status.getIndex());
            com.alicloud.openservices.tablestore.model.Error error = status.getError();
            TimeseriesGroup group = groupList.get(status.getIndex());
            triggerFailedCallback(new TimeseriesTableRow(originRequest.getRows().get(status.getIndex()), requestTableName),
                    new TableStoreException(error.getMessage(), null, error.getCode(), result.getRequestId(), 0),
                    group);
        }
        List<TimeseriesRow> allRows = originRequest.getRows();
        for (int i = 0; i < allRows.size(); i++) {
            if (!failed_index.contains(i)) {    //没有失败的TimeseriesRow即为成功
                triggerSucceedCallback(new TimeseriesTableRow(allRows.get(i), requestTableName), groupList.get(i));
            }
        }
    }


    @Override
    public void onCompleted(Req request, Res response) {
        try {
            if (request instanceof PutTimeseriesDataRequest) {
                onCompleted((PutTimeseriesDataRequest) request, (PutTimeseriesDataResponse) response);
            }
        } catch (Exception e) {
            logger.error("Failed while handling onCompleted function: {}", e.getMessage());
        } finally {
            requestComplete();
        }
    }

    @Override
    public void onFailed(Req request, Exception ex) {
        try {
            if (ex instanceof TableStoreException &&
                    ((TableStoreException)ex).getErrorCode().equals("OTSParameterInvalid") &&
                    ((PutTimeseriesDataRequest) request).getRows().size() > 1) {
                retryTimeseriesRow((PutTimeseriesDataRequest) request);
            } else if (ex instanceof TimeseriesWriterException &&
                    ((PutTimeseriesDataRequest) request).getRows().size() > 1) {
                retryTimeseriesRow((PutTimeseriesDataRequest) request);
            } else {
                failedOnException(request, ex);
            }
        } catch (Exception e) {
            logger.error("Failed while handling onFailed function: {}", e.getMessage());
        } finally {
            requestComplete();
        }
    }

    public void failedOnException(Req request, Exception ex) {
        List<TimeseriesTableRow> failedRows = new ArrayList<TimeseriesTableRow>();
        if (request instanceof PutTimeseriesDataRequest) {
            PutTimeseriesDataRequest bwr = (PutTimeseriesDataRequest) request;
            String tableName = bwr.getTimeseriesTableName();
            for(TimeseriesRow timeseriesRow:  bwr.getRows()){
                failedRows.add(new TimeseriesTableRow(timeseriesRow, tableName));
            }
        }

        triggerFailedCallback(failedRows, ex, groupList);
    }

    private void retryTimeseriesRow(PutTimeseriesDataRequest request) {
        if (TSWriteMode.SEQUENTIAL.equals(timeseriesBucketConfig.getWriteMode())) {
            retrySequentialWriteSingleTimeseries(request.getRows(), groupList, request.getTimeseriesTableName(), 0);
        } else {
            for (int i = 0; i < request.getRows().size(); i++) {
                TimeseriesGroup timeseriesGroup = groupList.get(i);
                timeseriesWriterHandleStatistics.incrementAndGetTotalSingleRowRequestCount();
                timeseriesWriterHandleStatistics.incrementAndGetTotalRequestCount();
                retryParallelWriteSingleTimeseries(request.getRows().get(i), timeseriesGroup, request.getTimeseriesTableName());
            }
        }
    }

    private void retryParallelWriteSingleTimeseries(TimeseriesRow timeseriesRow, TimeseriesGroup timeseriesGroup, String tableName) {
        final List<TimeseriesGroup> subGroupList = new ArrayList<TimeseriesGroup>(1);
        subGroupList.add(timeseriesGroup);

        this.count.incrementAndGet();

        PutTimeseriesDataRequest request = new PutTimeseriesDataRequest(tableName);

        List<TimeseriesRow> list = new ArrayList<TimeseriesRow>();
        list.add(timeseriesRow);
        request.addRows(list);


        TableStoreCallback tableStoreCallback = new TimeseriesFlushCallback<PutTimeseriesDataRequest, PutTimeseriesDataResponse>(ots, count, semaphore, callback,
                executor, timeseriesWriterHandleStatistics, timeseriesBucketConfig, bucketSemaphore, subGroupList);

        tryPutTimeseriesData(request, tableStoreCallback);
    }

    /**
     * 调用异步putTimeseriesData接口，串行地重试数据
     * @param timeseriesRows    待重试时序数据List
     * @param timeseriesGroups  待重试时序数据的GroupList
     * @param tableName         时序表名
     * @param index             要重试的数据在List中的index
     */
    private void retrySequentialWriteSingleTimeseries(final List<TimeseriesRow> timeseriesRows, final List<TimeseriesGroup> timeseriesGroups, final String tableName, final int index) {
        if (index >= timeseriesRows.size()) {
            return;
        }
        final List<TimeseriesGroup> subGroupList = new ArrayList<TimeseriesGroup>(1);
        subGroupList.add(timeseriesGroups.get(index));

        this.count.incrementAndGet();
        timeseriesWriterHandleStatistics.incrementAndGetTotalSingleRowRequestCount();
        timeseriesWriterHandleStatistics.incrementAndGetTotalRequestCount();

        final PutTimeseriesDataRequest request = new PutTimeseriesDataRequest(tableName);
        List<TimeseriesRow> list = new ArrayList<TimeseriesRow>();
        list.add(timeseriesRows.get(index));
        request.addRows(list);

        final TableStoreCallback<PutTimeseriesDataRequest, PutTimeseriesDataResponse> timeseriesFlushCallback = new TimeseriesFlushCallback<PutTimeseriesDataRequest, PutTimeseriesDataResponse>(ots, count, semaphore, callback,
                executor, timeseriesWriterHandleStatistics, timeseriesBucketConfig, bucketSemaphore, subGroupList);

        TableStoreCallback<PutTimeseriesDataRequest, PutTimeseriesDataResponse> tableStoreCallback = new TableStoreCallback<PutTimeseriesDataRequest, PutTimeseriesDataResponse>() {
            public void onCompleted(PutTimeseriesDataRequest req, PutTimeseriesDataResponse res) {
                // 迭代遍历
                retrySequentialWriteSingleTimeseries(timeseriesRows, timeseriesGroups, tableName, index + 1);
                timeseriesFlushCallback.onCompleted(req, res);
            }
            public void onFailed(PutTimeseriesDataRequest req, Exception ex) {
                // 迭代遍历
                retrySequentialWriteSingleTimeseries(timeseriesRows, timeseriesGroups, tableName, index + 1);
                timeseriesFlushCallback.onFailed(req, ex);
            }
        };

        tryPutTimeseriesData(request, tableStoreCallback);
    }

    private void tryPutTimeseriesData(PutTimeseriesDataRequest request,
            TableStoreCallback<PutTimeseriesDataRequest, PutTimeseriesDataResponse> callback) {
        try{
            ots.putTimeseriesData(request, callback);
        } catch (Exception e) {
            logger.error("Failed while send request:", e);
            callback.onFailed(request, new TimeseriesWriterException(e.getMessage(), e, "SendRequestError"));
        }
    }
}
