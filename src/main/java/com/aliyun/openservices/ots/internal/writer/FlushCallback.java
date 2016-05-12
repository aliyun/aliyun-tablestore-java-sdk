package com.aliyun.openservices.ots.internal.writer;

import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.OTSAsync;
import com.aliyun.openservices.ots.OTSException;
import com.aliyun.openservices.ots.internal.OTSCallback;
import com.aliyun.openservices.ots.model.*;
import com.aliyun.openservices.ots.model.Error;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

class FlushCallback<Req, Res> implements OTSCallback<Req, Res> {
    private Logger logger = LoggerFactory.getLogger(FlushCallback.class);

    private OTSAsync ots;
    private AtomicInteger count;
    private Semaphore semaphore;
    private long startTime;
    private OTSCallback<RowChange, ConsumedCapacity> callback;
    private Executor executor;

    public FlushCallback(OTSAsync ots, AtomicInteger count, Semaphore semaphore, OTSCallback<RowChange, ConsumedCapacity> callback, Executor executor) {
        this.ots = ots;
        this.count = count;
        this.semaphore = semaphore;
        this.startTime = System.currentTimeMillis();
        this.callback = callback;
        this.executor = executor;
    }

    private void triggerSucceedCallback(final RowChange rowChange, final ConsumedCapacity consumedCapacity) {
        if (callback == null) {
            return;
        }

        executor.execute(new Runnable() {
            @Override
            public void run() {
                callback.onCompleted(new OTSContext<RowChange, ConsumedCapacity>(rowChange, consumedCapacity));
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
                if (exp instanceof OTSException) {
                    callback.onFailed(new OTSContext<RowChange, ConsumedCapacity>(rowChange, null), (OTSException) exp);
                } else {
                    callback.onFailed(new OTSContext<RowChange, ConsumedCapacity>(rowChange, null), (ClientException) exp);
                }
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
                    if (exp instanceof OTSException) {
                        callback.onFailed(new OTSContext<RowChange, ConsumedCapacity>(rowChange, null), (OTSException) exp);
                    } else {
                        callback.onFailed(new OTSContext<RowChange, ConsumedCapacity>(rowChange, null), (ClientException) exp);
                    }
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

    public void onCompleted(BatchWriteRowRequest originRequest, BatchWriteRowResult result) {
        List<BatchWriteRowResult.RowStatus> succeed = new ArrayList<BatchWriteRowResult.RowStatus>();
        List<BatchWriteRowResult.RowStatus> failed = new ArrayList<BatchWriteRowResult.RowStatus>();

        // handle row put
        result.getResultOfPut(succeed, failed);
        for (BatchWriteRowResult.RowStatus status : succeed) {
            triggerSucceedCallback(originRequest.getRowPutChange(status.getTableName(), status.getIndex()), status.getConsumedCapacity());
        }
        for (BatchWriteRowResult.RowStatus status : failed) {
            Error error = status.getError();
            triggerFailedCallback(originRequest.getRowPutChange(status.getTableName(), status.getIndex()), new OTSException(error.getMessage(), error.getCode(), result.getRequestID()));
        }

        // handle row update
        succeed.clear();
        failed.clear();
        result.getResultOfUpdate(succeed, failed);
        for (BatchWriteRowResult.RowStatus status : succeed) {
            triggerSucceedCallback(originRequest.getRowUpdateChange(status.getTableName(), status.getIndex()), status.getConsumedCapacity());
        }
        for (BatchWriteRowResult.RowStatus status : failed) {
            Error error = status.getError();
            triggerFailedCallback(originRequest.getRowUpdateChange(status.getTableName(), status.getIndex()), new OTSException(error.getMessage(), error.getCode(), result.getRequestID()));
        }

        // handle row delete
        succeed.clear();
        failed.clear();
        result.getResultOfDelete(succeed, failed);
        for (BatchWriteRowResult.RowStatus status : succeed) {
            triggerSucceedCallback(originRequest.getRowDeleteChange(status.getTableName(), status.getIndex()), status.getConsumedCapacity());
        }
        for (BatchWriteRowResult.RowStatus status : failed) {
            Error error = status.getError();
            triggerFailedCallback(originRequest.getRowDeleteChange(status.getTableName(), status.getIndex()), new OTSException(error.getMessage(), error.getCode(), result.getRequestID()));
        }
    }

    @Override
    public void onCompleted(OTSContext<Req, Res> otsContext) {
        Req request = otsContext.getOTSRequest();
        Res response = otsContext.getOTSResult();
        logger.debug("OnComplete: {}", request.getClass().getName());
        if (request instanceof BatchWriteRowRequest) {
            onCompleted((BatchWriteRowRequest) request, (BatchWriteRowResult) response);
        } else if (request instanceof PutRowRequest) {
            PutRowRequest pr = (PutRowRequest) request;
            triggerSucceedCallback(pr.getRowChange(), ((PutRowResult) response).getConsumedCapacity());
        } else if (request instanceof UpdateRowRequest) {
            UpdateRowRequest ur = (UpdateRowRequest) request;
            triggerSucceedCallback(ur.getRowChange(), ((UpdateRowResult) response).getConsumedCapacity());
        } else if (request instanceof DeleteRowRequest) {
            DeleteRowRequest dr = (DeleteRowRequest) request;
            triggerSucceedCallback(dr.getRowChange(), ((DeleteRowResult) response).getConsumedCapacity());
        }

        requestComplete();
    }

    @Override
    public void onFailed(OTSContext<Req, Res> otsContext, OTSException ex) {
        Req request = otsContext.getOTSRequest();
        logger.debug("OnFailed on OTSException: {}, {}", request.getClass().getName(), ex);
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

    /**
     * 在一般情况下，BatchWriteRow是不会发生整体异常的，若发生，则代表其中包含有脏数据，例如有一行的属性列的大小超过大小限制等。
     * 在这种情况下，我们不希望因为这一行脏数据，导致其他的行导入失败。但是由于我们无法找出这是哪一行，所以采取的做法是将这一次Batch内
     * 包含的所有行通过PutRow等单行写操作写过去。
     * 由于我们在writer外层是有一层参数检查了，所以这种情况是极少发生的。
     *
     * @param request
     */
    private void retryBatchWrite(BatchWriteRowRequest request) {
        for (Map.Entry<String, List<RowPutChange>> entry : request.getRowPutChange().entrySet()) {
            count.addAndGet(entry.getValue().size());
            for (RowPutChange rowChange : entry.getValue()) {
                PutRowRequest pr = new PutRowRequest();
                pr.setRowChange(rowChange);
                ots.putRow(pr, new FlushCallback<PutRowRequest, PutRowResult>(ots, count, semaphore, callback, executor));
            }
        }

        for (Map.Entry<String, List<RowUpdateChange>> entry : request.getRowUpdateChange().entrySet()) {
            count.addAndGet(entry.getValue().size());
            for (RowUpdateChange rowChange : entry.getValue()) {
                UpdateRowRequest ur = new UpdateRowRequest();
                ur.setRowChange(rowChange);
                ots.updateRow(ur, new FlushCallback<UpdateRowRequest, UpdateRowResult>(ots, count, semaphore, callback, executor));
            }
        }

        for (Map.Entry<String, List<RowDeleteChange>> entry : request.getRowDeleteChange().entrySet()) {
            count.addAndGet(entry.getValue().size());
            for (RowDeleteChange rowChange : entry.getValue()) {
                DeleteRowRequest dr = new DeleteRowRequest();
                dr.setRowChange(rowChange);
                ots.deleteRow(dr, new FlushCallback<DeleteRowRequest, DeleteRowResult>(ots, count, semaphore, callback, executor));
            }
        }
    }

    @Override
    public void onFailed(OTSContext<Req, Res> otsContext, ClientException ex) {
        Req request = otsContext.getOTSRequest();
        logger.debug("OnFailed on ClientException: {}, {}", request.getClass().getName(), ex);
        List<RowChange> failedRows = new ArrayList<RowChange>();
        if (request instanceof BatchWriteRowRequest) {
            BatchWriteRowRequest bwr = (BatchWriteRowRequest) request;
            for (Map.Entry<String, List<RowPutChange>> entry : bwr.getRowPutChange().entrySet()) {
                for (RowPutChange rowChange : entry.getValue()) {
                    failedRows.add(rowChange);
                }
            }

            for (Map.Entry<String, List<RowUpdateChange>> entry : bwr.getRowUpdateChange().entrySet()) {
                for (RowUpdateChange rowChange : entry.getValue()) {
                    failedRows.add(rowChange);
                }
            }

            for (Map.Entry<String, List<RowDeleteChange>> entry : bwr.getRowDeleteChange().entrySet()) {
                for (RowDeleteChange rowChange : entry.getValue()) {
                    failedRows.add(rowChange);
                }
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
}
