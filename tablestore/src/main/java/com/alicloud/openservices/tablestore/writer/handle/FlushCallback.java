package com.alicloud.openservices.tablestore.writer.handle;

import com.alicloud.openservices.tablestore.AsyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.Error;
import com.alicloud.openservices.tablestore.writer.Group;
import com.alicloud.openservices.tablestore.writer.RowWriteResult;
import com.alicloud.openservices.tablestore.writer.config.BucketConfig;
import com.alicloud.openservices.tablestore.writer.enums.WriteMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class FlushCallback<Req, Res> implements TableStoreCallback<Req, Res> {
    private Logger logger = LoggerFactory.getLogger(FlushCallback.class);

    private final AsyncClientInterface ots;
    private final AtomicInteger count;
    private final Semaphore semaphore;
    private final BucketConfig bucketConfig;
    private final TableStoreCallback<RowChange, RowWriteResult> callback;
    private final Executor executor;
    private final WriterHandleStatistics writerStatistics;
    private final Semaphore bucketSemaphore;
    private final List<Group> groupList;
    public static AtomicLong counter = new AtomicLong(0);

    public FlushCallback(AsyncClientInterface ots, AtomicInteger count, Semaphore semaphore,
                         TableStoreCallback<RowChange, RowWriteResult> callback,
                         Executor executor, WriterHandleStatistics writerStatistics,
                         BucketConfig bucketConfig, Semaphore bucketSemaphore, List<Group> groupList) {
        this.ots = ots;
        this.count = count;
        this.semaphore = semaphore;
        this.bucketConfig = bucketConfig;
        this.callback = callback;
        this.executor = executor;
        this.writerStatistics = writerStatistics;
        this.bucketSemaphore = bucketSemaphore;
        this.groupList = groupList;
    }

    private void triggerSucceedCallback(final RowChange rowChange, final ConsumedCapacity consumedCapacity, final Row row, final Group group) {
        writerStatistics.totalSucceedRowsCount.incrementAndGet();
        group.succeedOneRow(rowChange);
        if (callback == null) {
            return;
        }

        executor.execute(new Runnable() {
            @Override
            public void run() {
                callback.onCompleted(rowChange, new RowWriteResult(consumedCapacity, row));
            }
        });
    }

    private void triggerFailedCallback(final RowChange rowChange, final Exception exp, final Group group) {
        writerStatistics.totalFailedRowsCount.incrementAndGet();
        group.failedOneRow(rowChange, exp);
        logger.error("RowChange Failed: ", exp);
        if (callback == null) {
            return;
        }

        executor.execute(new Runnable() {
            @Override
            public void run() {
                callback.onFailed(rowChange, exp);
            }
        });
    }

    private void triggerFailedCallback(final List<RowChange> rowChanges, final Exception exp, final List<Group> groupList) {
        writerStatistics.totalFailedRowsCount.addAndGet(rowChanges.size());
        for (int i = 0; i < rowChanges.size(); i++) {
            RowChange rowChange = rowChanges.get(i);
            Group group = groupList.get(i);
            group.failedOneRow(rowChange, exp);
            logger.error("RowChange Failed: ", exp);
        }
        if (callback == null) {
            return;
        }

        executor.execute(new Runnable() {
            @Override
            public void run() {
                for (RowChange rowChange : rowChanges) {
                    callback.onFailed(rowChange, exp);
                }
            }
        });
    }

    /**
     * After the request ends, the semaphore needs to be actively released.<br/>
     * However, note that not every completed request can release the semaphore.<br/>
     * For example, a BatchWriteRow request acquires one semaphore, but due to containing dirty data, it is necessary to resend each row of data individually, causing one concurrency 
     * to split into N concurrencies. These N concurrent requests will have their own independent callbacks. The semaphore that was applied for should be released by one of these N concurrencies, but which one?
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

    public void onCompleted(BatchWriteRowRequest originRequest, BatchWriteRowResponse result) {
        List<BatchWriteRowResponse.RowResult> succeed = new ArrayList<BatchWriteRowResponse.RowResult>();
        List<BatchWriteRowResponse.RowResult> failed = new ArrayList<BatchWriteRowResponse.RowResult>();

        result.getResult(succeed, failed);

        for (BatchWriteRowResponse.RowResult status : succeed) {
            Group group = groupList.get(status.getIndex());
            triggerSucceedCallback(originRequest.getRowChange(status.getTableName(), status.getIndex()), status.getConsumedCapacity(),
                    status.getRow(), group);
        }

        for (BatchWriteRowResponse.RowResult status : failed) {
            Error error = status.getError();
            Group group = groupList.get(status.getIndex());
            triggerFailedCallback(originRequest.getRowChange(status.getTableName(), status.getIndex()),
                    new TableStoreException(error.getMessage(), null, error.getCode(), result.getRequestId(), 0),
                    group);
        }
    }

    public void onCompleted(BulkImportRequest originRequest, BulkImportResponse result) {
        List<BulkImportResponse.RowResult> succeed = new ArrayList<BulkImportResponse.RowResult>();
        List<BulkImportResponse.RowResult> failed = new ArrayList<BulkImportResponse.RowResult>();

        result.getResult(succeed, failed);

        for (BulkImportResponse.RowResult status : succeed) {
            Group group = groupList.get(status.getIndex());
            triggerSucceedCallback(originRequest.getRowChange(status.getIndex()), status.getConsumedCapacity(),
                    null, group);
        }

        for (BulkImportResponse.RowResult status : failed) {
            Error error = status.getError();
            Group group = groupList.get(status.getIndex());
            triggerFailedCallback(originRequest.getRowChange(status.getIndex()),
                    new TableStoreException(error.getMessage(), null, error.getCode(), result.getRequestId(), 0),
                    group);
        }
    }

    @Override
    public void onCompleted(Req request, Res response) {
        logger.debug("OnComplete: {}", request.getClass().getName());
        if (request instanceof BatchWriteRowRequest) {
            onCompleted((BatchWriteRowRequest) request, (BatchWriteRowResponse) response);
        } else if (request instanceof BulkImportRequest) {
            onCompleted((BulkImportRequest) request, (BulkImportResponse) response);
        } else if (request instanceof PutRowRequest) {
            PutRowRequest pr = (PutRowRequest) request;
            triggerSucceedCallback(pr.getRowChange(), ((PutRowResponse) response).getConsumedCapacity(), ((PutRowResponse) response).getRow(), groupList.get(0));
        } else if (request instanceof UpdateRowRequest) {
            UpdateRowRequest ur = (UpdateRowRequest) request;
            triggerSucceedCallback(ur.getRowChange(), ((UpdateRowResponse) response).getConsumedCapacity(), ((UpdateRowResponse) response).getRow(), groupList.get(0));
        } else if (request instanceof DeleteRowRequest) {
            DeleteRowRequest dr = (DeleteRowRequest) request;
            triggerSucceedCallback(dr.getRowChange(), ((DeleteRowResponse) response).getConsumedCapacity(), ((DeleteRowResponse) response).getRow(), groupList.get(0));
        }

        requestComplete();

    }

    @Override
    public void onFailed(Req request, Exception ex) {

        if (ex instanceof TableStoreException) {
            failedOnException(request, (TableStoreException) ex);
        } else {
            failedOnUnknownException(request, ex);
        }

        requestComplete();

    }

    public void failedOnException(Req request, TableStoreException ex) {
        logger.debug("OnFailed on TableStoreException: {}, {}", request.getClass().getName(), ex);
        if (request instanceof BatchWriteRowRequest) {
            retryBatchWrite((BatchWriteRowRequest) request);
        } else if (request instanceof BulkImportRequest) {
            retryBulkImport((BulkImportRequest) request);
        } else if (request instanceof PutRowRequest) {
            PutRowRequest pr = (PutRowRequest) request;
            triggerFailedCallback(pr.getRowChange(), ex, groupList.get(0));
        } else if (request instanceof UpdateRowRequest) {
            UpdateRowRequest ur = (UpdateRowRequest) request;
            triggerFailedCallback(ur.getRowChange(), ex, groupList.get(0));
        } else if (request instanceof DeleteRowRequest) {
            DeleteRowRequest dr = (DeleteRowRequest) request;
            triggerFailedCallback(dr.getRowChange(), ex, groupList.get(0));
        }

    }

    public void failedOnUnknownException(Req request, Exception ex) {
        logger.debug("OnFailed on ClientException: {}, {}", request.getClass().getName(), ex);
        List<RowChange> failedRows = new ArrayList<RowChange>();
        if (request instanceof BatchWriteRowRequest) {
            BatchWriteRowRequest bwr = (BatchWriteRowRequest) request;
            for (Map.Entry<String, List<RowChange>> entry : bwr.getRowChange().entrySet()) {
                failedRows.addAll(entry.getValue());
            }
        } else if (request instanceof BulkImportRequest) {
            BulkImportRequest bir = (BulkImportRequest) request;
            failedRows.addAll(bir.getRowChange());
        } else if (request instanceof PutRowRequest) {
            PutRowRequest pr = (PutRowRequest) request;
            failedRows.add(pr.getRowChange());
        } else if (request instanceof UpdateRowRequest) {
            UpdateRowRequest ur = (UpdateRowRequest) request;
            failedRows.add(ur.getRowChange());
        } else if (request instanceof DeleteRowRequest) {
            DeleteRowRequest dr = (DeleteRowRequest) request;
            failedRows.add(dr.getRowChange());
        }

        triggerFailedCallback(failedRows, ex, groupList);
    }

    /**
     * In general, BatchWriteRow will not result in an overall exception. If it does occur, it indicates that there is dirty data involved, 
     * such as a row where the size of the attribute column exceeds the size limit.
     * In this scenario, we do not want a single row of dirty data to cause the import of other rows to fail. However, since we cannot determine which specific row is causing the issue, 
     * our approach is to write all rows included in this batch using single-row write operations like PutRow.
     * Since there is a layer of parameter checking outside the writer, this situation occurs extremely rarely.
     *
     * @param request
     */
    private void retryBatchWrite(BatchWriteRowRequest request) {
        for (Map.Entry<String, List<RowChange>> entry : request.getRowChange().entrySet()) {
            for (int i = 0; i < entry.getValue().size(); i++) {
                Group group = groupList.get(i);
                retrySingleRowChange(entry.getValue().get(i), group);
            }
        }
    }

    /**
     * Similar to BatchWriteRow, BulkImport will not result in overall exceptions. If it does occur, 
     * it indicates that there is dirty data included, such as one row having property columns exceeding the size limit, etc.
     * In such cases, we do not want a single row of dirty data to cause the failure of importing other rows. 
     * However, since we are unable to identify which specific row causes the issue, our approach is to write all rows included 
     * in this batch using single-row write operations like PutRow.
     * Since we have an additional layer of parameter checking outside the writer, such situations occur very rarely.
     *
     * @param request
     */
    private void retryBulkImport(BulkImportRequest request) {
        for (int i = 0; i < request.getRowChange().size(); i++) {
            Group group = groupList.get(i);
            retrySingleRowChange(request.getRowChange(i), group);
        }
    }

    private void retrySingleRowChange(RowChange rowChange, Group group) {
        writerStatistics.totalSingleRowRequestCount.incrementAndGet();
        writerStatistics.totalRequestCount.incrementAndGet();

        if (WriteMode.SEQUENTIAL.equals(bucketConfig.getWriteMode())) {
            retrySequentialWriteSingleRowChange(rowChange, group);
        } else {
            retryParallelWriteSingleRowChange(rowChange, group);
        }
    }

    private void retryParallelWriteSingleRowChange(RowChange rowChange, Group group) {
        final List<Group> subGroupList = new ArrayList<Group>(1);
        subGroupList.add(group);

        this.count.incrementAndGet();
        if (rowChange instanceof RowPutChange) {
            RowPutChange rowPutChange = (RowPutChange) rowChange;
            PutRowRequest pr = new PutRowRequest();
            pr.setRowChange(rowPutChange);
            ots.putRow(pr, new FlushCallback<PutRowRequest, PutRowResponse>(ots, count, semaphore, callback, executor, writerStatistics, bucketConfig, bucketSemaphore, subGroupList));
        } else if (rowChange instanceof RowUpdateChange) {
            UpdateRowRequest ur = new UpdateRowRequest();
            RowUpdateChange rowUpdateChange = (RowUpdateChange) rowChange;
            ur.setRowChange(rowUpdateChange);
            ots.updateRow(ur, new FlushCallback<UpdateRowRequest, UpdateRowResponse>(ots, count, semaphore, callback, executor, writerStatistics, bucketConfig, bucketSemaphore, subGroupList));
        } else if (rowChange instanceof RowDeleteChange) {
            DeleteRowRequest dr = new DeleteRowRequest();
            RowDeleteChange rowDeleteChange = (RowDeleteChange) rowChange;
            dr.setRowChange(rowDeleteChange);
            ots.deleteRow(dr, new FlushCallback<DeleteRowRequest, DeleteRowResponse>(ots, count, semaphore, callback, executor, writerStatistics, bucketConfig, bucketSemaphore, subGroupList));
        }
    }

    private void retrySequentialWriteSingleRowChange(RowChange rowChange, Group group) {
        if (rowChange instanceof RowPutChange) {
            RowPutChange rowPutChange = (RowPutChange) rowChange;
            PutRowRequest pr = new PutRowRequest();
            pr.setRowChange(rowPutChange);
            try {
                PutRowResponse response = ots.asSyncClient().putRow(pr);
                triggerSucceedCallback(rowPutChange, response.getConsumedCapacity(), response.getRow(), group);
            } catch (Exception e) {
                triggerFailedCallback(rowPutChange, e, group);
            }
        } else if (rowChange instanceof RowUpdateChange) {
            UpdateRowRequest ur = new UpdateRowRequest();
            RowUpdateChange rowUpdateChange = (RowUpdateChange) rowChange;
            ur.setRowChange(rowUpdateChange);
            try {
                UpdateRowResponse response = ots.asSyncClient().updateRow(ur);
                triggerSucceedCallback(rowUpdateChange, response.getConsumedCapacity(), response.getRow(), group);
            } catch (Exception e) {
                triggerFailedCallback(rowUpdateChange, e, group);
            }
        } else if (rowChange instanceof RowDeleteChange) {
            DeleteRowRequest dr = new DeleteRowRequest();
            RowDeleteChange rowDeleteChange = (RowDeleteChange) rowChange;
            dr.setRowChange(rowDeleteChange);
            try {
                DeleteRowResponse response = ots.asSyncClient().deleteRow(dr);
                triggerSucceedCallback(rowDeleteChange, response.getConsumedCapacity(), response.getRow(), group);
            } catch (Exception e) {
                triggerFailedCallback(rowDeleteChange, e, group);
            }
        }
    }
}
