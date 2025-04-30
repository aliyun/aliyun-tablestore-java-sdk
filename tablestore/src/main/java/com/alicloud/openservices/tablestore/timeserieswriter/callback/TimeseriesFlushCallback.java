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
    The current TimeseriesDataResponse only returns error information and does not return correct column information.
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
     * After the request ends, the semaphore needs to be actively released.<br/>
     * However, note that not every completed request can release the semaphore.<br/>
     * For example, a BatchWriteRow request acquires one semaphore, but due to containing dirty data, it is necessary to send a separate request for each row of data to retry. 
     * In this case, one concurrency splits into N concurrencies, and these N concurrent requests will have their own independent callbacks. The semaphore that was applied for should be released by one of these N concurrencies, but which one should do it?
     * The strategy adopted here is to use an atomic counter, and whichever request completes last will release this semaphore.
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
            if (!failed_index.contains(i)) {    // If there is no failed TimeseriesRow, it is considered successful.
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
     * Call the asynchronous putTimeseriesData interface to serially retry the data.
     * @param timeseriesRows    List of timeseries data to be retried
     * @param timeseriesGroups  GroupList of timeseries data to be retried
     * @param tableName         Name of the timeseries table
     * @param index             The index in the List of the data to be retried
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
                // Iterate through the loop
                retrySequentialWriteSingleTimeseries(timeseriesRows, timeseriesGroups, tableName, index + 1);
                timeseriesFlushCallback.onCompleted(req, res);
            }
            public void onFailed(PutTimeseriesDataRequest req, Exception ex) {
                // Iterative traversal
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
