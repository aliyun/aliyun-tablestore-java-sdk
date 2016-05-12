package com.aliyun.openservices.ots.internal;

import com.aliyun.openservices.ots.PartialResultFailedException;
import com.aliyun.openservices.ots.model.BatchGetRowRequest;
import com.aliyun.openservices.ots.model.BatchGetRowResult;
import com.aliyun.openservices.ots.model.OTSBasicFuture;

import java.util.concurrent.ScheduledExecutorService;

public class BatchGetRowExecutionContext extends OTSExecutionContext<BatchGetRowRequest, BatchGetRowResult> {

    private BatchGetRowRequest originalRequest;
    private BatchGetRowResult lastResult;

    public BatchGetRowExecutionContext(BatchGetRowRequest request, OTSBasicFuture<BatchGetRowResult> future, OTSTraceLogger traceLogger, OTSRetryStrategy retryStrategy, ScheduledExecutorService retryExecutor) {
        super(request, future, traceLogger, retryStrategy, retryExecutor);
        this.originalRequest = request;
    }

    public BatchGetRowResult getLastResult() {
        return lastResult;
    }

    @Override
    public void retry(Exception ex) {
        retries++;
        if (ex instanceof PartialResultFailedException) {
            lastResult = (BatchGetRowResult) ((PartialResultFailedException) ex).getResult();
            BatchGetRowRequest request = this.originalRequest.createRequestForRetry(lastResult.getFailedRows());
            setRequest(request);
        }
    }

}
