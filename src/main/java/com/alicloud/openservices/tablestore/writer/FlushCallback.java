package com.alicloud.openservices.tablestore.writer;

import com.alicloud.openservices.tablestore.AsyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.Error;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

class FlushCallback<Req, Res> implements TableStoreCallback<Req, Res> {
    private Logger logger = LoggerFactory.getLogger(FlushCallback.class);

    private AsyncClientInterface ots;
    private AtomicInteger count;
    private Semaphore semaphore;
    private long startTime;
    private TableStoreCallback<RowChange, RowWriteResult> callback;
    private Executor executor;
    private DefaultWriterStatistics writerStatistics;

    public FlushCallback(AsyncClientInterface ots, AtomicInteger count, Semaphore semaphore,
                         TableStoreCallback<RowChange, RowWriteResult> callback, Executor executor, DefaultWriterStatistics writerStatistics) {
        this.ots = ots;
        this.count = count;
        this.semaphore = semaphore;
        this.startTime = System.currentTimeMillis();
        this.callback = callback;
        this.executor = executor;
        this.writerStatistics = writerStatistics;
    }

    private void triggerSucceedCallback(final RowChange rowChange, final ConsumedCapacity consumedCapacity, final Row row) {
        if (callback == null) {
            return;
        }

        executor.execute(new Runnable() {
            @Override
            public void run() {
                writerStatistics.totalSucceedRowsCount.incrementAndGet();
                callback.onCompleted(rowChange, new RowWriteResult(consumedCapacity, row));
            }
        });
    }

    private void triggerFailedCallback(final RowChange rowChange, final Exception exp) {
        if (callback == null) {
            return;
        }

        executor.execute(new Runnable() {
            @Override
            public void run() {
                writerStatistics.totalFailedRowsCount.incrementAndGet();
                callback.onFailed(rowChange, exp);
            }
        });
    }

    private void triggerFailedCallback(final List<RowChange> rowChanges, final Exception exp) {
        if (callback == null) {
            return;
        }

        executor.execute(new Runnable() {
            @Override
            public void run() {
                for (RowChange rowChange : rowChanges) {
                    writerStatistics.totalFailedRowsCount.incrementAndGet();
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
            semaphore.release();
            logger.debug("Release semaphore.");
        }
        long endTime = System.currentTimeMillis();
        logger.debug("BatchWriteRow latency: {}", endTime - startTime);
    }

    public void onCompleted(BatchWriteRowRequest originRequest, BatchWriteRowResponse result) {
        List<BatchWriteRowResponse.RowResult> succeed = new ArrayList<BatchWriteRowResponse.RowResult>();
        List<BatchWriteRowResponse.RowResult> failed = new ArrayList<BatchWriteRowResponse.RowResult>();

        result.getResult(succeed, failed);
        for (BatchWriteRowResponse.RowResult status : succeed) {
            triggerSucceedCallback(originRequest.getRowChange(status.getTableName(), status.getIndex()), status.getConsumedCapacity(), status.getRow());
        }

        for (BatchWriteRowResponse.RowResult status : failed) {
            Error error = status.getError();
            triggerFailedCallback(originRequest.getRowChange(status.getTableName(), status.getIndex()), new TableStoreException(error.getMessage(), null, error.getCode(), result.getRequestId(), 0));
        }
    }

    @Override
    public void onCompleted(Req request, Res response) {
        logger.debug("OnComplete: {}", request.getClass().getName());
        if (request instanceof BatchWriteRowRequest) {
            onCompleted((BatchWriteRowRequest) request, (BatchWriteRowResponse) response);
        } else if (request instanceof PutRowRequest) {
            PutRowRequest pr = (PutRowRequest) request;
            triggerSucceedCallback(pr.getRowChange(), ((PutRowResponse) response).getConsumedCapacity(), ((PutRowResponse) response).getRow());
        } else if (request instanceof UpdateRowRequest) {
            UpdateRowRequest ur = (UpdateRowRequest) request;
            triggerSucceedCallback(ur.getRowChange(), ((UpdateRowResponse) response).getConsumedCapacity(), ((UpdateRowResponse) response).getRow());
        } else if (request instanceof DeleteRowRequest) {
            DeleteRowRequest dr = (DeleteRowRequest) request;
            triggerSucceedCallback(dr.getRowChange(), ((DeleteRowResponse) response).getConsumedCapacity(), ((DeleteRowResponse) response).getRow());
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
    }

    public void failedOnException(Req request, TableStoreException ex) {
        logger.debug("OnFailed on TableStoreException: {}, {}", request.getClass().getName(), ex);
        if (request instanceof BatchWriteRowRequest) {
            retryBatchWrite((BatchWriteRowRequest) request);
        } else if (request instanceof PutRowRequest) {
            PutRowRequest pr = (PutRowRequest) request;
            triggerFailedCallback(pr.getRowChange(), ex);
        } else if (request instanceof UpdateRowRequest) {
            UpdateRowRequest ur = (UpdateRowRequest) request;
            triggerFailedCallback(ur.getRowChange(), ex);
        } else if (request instanceof DeleteRowRequest) {
            DeleteRowRequest dr = (DeleteRowRequest) request;
            triggerFailedCallback(dr.getRowChange(), ex);
        }

        requestComplete();
    }

    public void failedOnUnknownException(Req request, Exception ex) {
        logger.debug("OnFailed on ClientException: {}, {}", request.getClass().getName(), ex);
        List<RowChange> failedRows = new ArrayList<RowChange>();
        if (request instanceof BatchWriteRowRequest) {
            BatchWriteRowRequest bwr = (BatchWriteRowRequest) request;
            for (Map.Entry<String, List<RowChange>> entry : bwr.getRowChange().entrySet()) {
                failedRows.addAll(entry.getValue());
            }
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

        triggerFailedCallback(failedRows, ex);

        requestComplete();
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
            count.addAndGet(entry.getValue().size());
            for (RowChange rowChange : entry.getValue()) {
                if (rowChange instanceof RowPutChange) {
                    RowPutChange rowPutChange = (RowPutChange) rowChange;
                    PutRowRequest pr = new PutRowRequest();
                    pr.setRowChange(rowPutChange);
                    writerStatistics.totalSingleRowRequestCount.incrementAndGet();
                    writerStatistics.totalRequestCount.incrementAndGet();
                    ots.putRow(pr, new FlushCallback<PutRowRequest, PutRowResponse>(ots, count, semaphore, callback, executor, writerStatistics));
                } else if (rowChange instanceof RowUpdateChange) {
                    UpdateRowRequest ur = new UpdateRowRequest();
                    RowUpdateChange rowUpdateChange = (RowUpdateChange) rowChange;
                    ur.setRowChange(rowUpdateChange);
                    writerStatistics.totalSingleRowRequestCount.incrementAndGet();
                    writerStatistics.totalRequestCount.incrementAndGet();
                    ots.updateRow(ur, new FlushCallback<UpdateRowRequest, UpdateRowResponse>(ots, count, semaphore, callback, executor, writerStatistics));
                } else if (rowChange instanceof RowDeleteChange) {
                    DeleteRowRequest dr = new DeleteRowRequest();
                    RowDeleteChange rowDeleteChange = (RowDeleteChange) rowChange;
                    dr.setRowChange(rowDeleteChange);
                    writerStatistics.totalSingleRowRequestCount.incrementAndGet();
                    writerStatistics.totalRequestCount.incrementAndGet();
                    ots.deleteRow(dr, new FlushCallback<DeleteRowRequest, DeleteRowResponse>(ots, count, semaphore, callback, executor, writerStatistics));
                }
            }
        }
    }
}
