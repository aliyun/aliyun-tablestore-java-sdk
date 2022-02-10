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
     * 在一般情况下，BatchWriteRow是不会发生整体异常的，若发生，则代表其中包含有脏数据，例如有一行的属性列的大小超过大小限制等。
     * 在这种情况下，我们不希望因为这一行脏数据，导致其他的行导入失败。但是由于我们无法找出这是哪一行，所以采取的做法是将这一次Batch内
     * 包含的所有行通过PutRow等单行写操作写过去。
     * 由于我们在writer外层是有一层参数检查了，所以这种情况是极少发生的。
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
     * 类似BatchWriteRow，BulkImport是不会发生整体异常的，若发生，则代表其中包含有脏数据，例如有一行的属性列的大小超过大小限制等。
     * 在这种情况下，我们不希望因为这一行脏数据，导致其他的行导入失败。但是由于我们无法找出这是哪一行，所以采取的做法是将这一次Batch内
     * 包含的所有行通过PutRow等单行写操作写过去。
     * 由于我们在writer外层是有一层参数检查了，所以这种情况是极少发生的。
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
