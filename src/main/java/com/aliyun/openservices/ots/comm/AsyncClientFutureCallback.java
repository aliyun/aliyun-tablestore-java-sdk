package com.aliyun.openservices.ots.comm;

import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.OTSException;
import com.aliyun.openservices.ots.PartialResultFailedException;
import com.aliyun.openservices.ots.internal.OTSExecutionContext;
import com.aliyun.openservices.ots.internal.OTSRetryStrategy;
import com.aliyun.openservices.ots.internal.OTSTraceLogger;
import com.aliyun.openservices.ots.log.LogUtil;
import com.aliyun.openservices.ots.model.OTSBasicFuture;
import com.aliyun.openservices.ots.model.OTSResult;
import org.apache.http.concurrent.FutureCallback;

import java.util.concurrent.TimeUnit;

public class AsyncClientFutureCallback<Res> implements FutureCallback<Res> {

    private OTSExecutionContext<?, Res> executionContext;
    private String action;

    public AsyncClientFutureCallback(String action, OTSExecutionContext<?, Res> executionContext) {
        this.action = action;
        this.executionContext = executionContext;
    }

    @Override
    public void completed(Res result) {
        OTSBasicFuture<Res> future = executionContext.getFuture();
        OTSTraceLogger traceLogger = executionContext.getTraceLogger();

        if (result instanceof OTSResult) {
            ((OTSResult) result).setTraceId(traceLogger.getTraceId());
            LogUtil.logOnCompleted(executionContext, ((OTSResult) result).getRequestID());
            traceLogger.printLog();
            future.completed(result);
        } else {
            future.failed(new ClientException(
                    "Type error: The type of result is not OTSResult."));
        }
    }

    @Override
    public void failed(Exception ex) {
        final OTSBasicFuture<Res> future = executionContext.getFuture();
        OTSTraceLogger traceLogger = executionContext.getTraceLogger();
        OTSRetryStrategy retryStrategy = executionContext.getRetryStrategy();

        final Exception e;
        String requestId = null;
        if (ex instanceof OTSException) {
            e = ex;
            requestId = ((OTSException) ex).getRequestId();
            ((OTSException) e).setTraceId(traceLogger.getTraceId());
        } else if (ex instanceof ClientException) {
            e = ex;
            ((ClientException) e).setTraceId(traceLogger.getTraceId());
        } else {
            e = new ClientException("Unexpected error: " + ex, ex, traceLogger.getTraceId());
        }
        LogUtil.logOnFailed(executionContext, e, requestId);
        if (retryStrategy.shouldRetry(action, e, executionContext.getRetries() + 1)) {
            executionContext.getRetryExecutor().schedule(new Runnable() {
                                                             @Override
                                                             public void run() {
                                                                 try {
                                                                     executionContext.retry(e);
                                                                     executionContext.getCallable().call();
                                                                 } catch (Exception ex) {
                                                                     future.failed(new ClientException("Failed to retry.", ex));
                                                                 }
                                                             }
                                                         }, retryStrategy.getPauseDelay(action, e, executionContext.getRetries() + 1),
                    TimeUnit.MILLISECONDS);
        } else {
            if (e instanceof PartialResultFailedException) {
                traceLogger.printLog();
                future.completed((Res) ((PartialResultFailedException) e).getResult());
            } else {
                traceLogger.printLog();
                future.failed(e);
            }
        }
    }

    /*
     * 这个接口不会被调用到，原因是这个FutureCallback对应的是HttpAsyncClient.execute()返回的Future
     * ， 我们没有使用和保存这个Future，即不会调用这个Future的cancel()方法，所以不会触发cancelled()。
     * 对用户而言，他看到的接口都不包含Cancel相关的逻辑。
     */
    @Override
    public void cancelled() {

    }
}
