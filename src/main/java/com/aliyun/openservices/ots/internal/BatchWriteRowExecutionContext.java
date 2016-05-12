package com.aliyun.openservices.ots.internal;

import com.aliyun.openservices.ots.PartialResultFailedException;
import com.aliyun.openservices.ots.model.BatchWriteRowRequest;
import com.aliyun.openservices.ots.model.BatchWriteRowResult;
import com.aliyun.openservices.ots.model.OTSBasicFuture;

import java.util.concurrent.ScheduledExecutorService;

public class BatchWriteRowExecutionContext extends OTSExecutionContext<BatchWriteRowRequest, BatchWriteRowResult> {

    private BatchWriteRowRequest originalRequest;
    private BatchWriteRowResult lastResult;

    public BatchWriteRowExecutionContext(BatchWriteRowRequest request, OTSBasicFuture future, OTSTraceLogger traceLogger, OTSRetryStrategy retryStrategy, ScheduledExecutorService retryExecutor) {
        super(request, future, traceLogger, retryStrategy, retryExecutor);
        this.originalRequest = request;
    }

    public BatchWriteRowResult getLastResult() {
        return this.lastResult;
    }

    @Override
    public void retry(Exception ex) {
        retries++;
        if (ex instanceof PartialResultFailedException) {
            lastResult = (BatchWriteRowResult) ((PartialResultFailedException) ex).getResult();
            BatchWriteRowRequest request = originalRequest.createRequestForRetry(
                    ((BatchWriteRowResult) lastResult).getFailedRowsOfPut(),
                    ((BatchWriteRowResult) lastResult).getFailedRowsOfUpdate(),
                    ((BatchWriteRowResult) lastResult).getFailedRowsOfDelete());
            setRequest(request);
        }
    }
}
