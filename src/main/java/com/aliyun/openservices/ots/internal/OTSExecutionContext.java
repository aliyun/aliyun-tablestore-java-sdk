package com.aliyun.openservices.ots.internal;


import com.aliyun.openservices.ots.model.OTSBasicFuture;
import org.apache.http.concurrent.FutureCallback;

import java.util.concurrent.ScheduledExecutorService;

public class OTSExecutionContext<Req, Res> {
    private Req request;
    private OTSBasicFuture<Res> future;
    private OTSTraceLogger traceLogger;
    private FutureCallback<Res> asyncClientCallback;
    private OTSRetryStrategy retryStrategy;
    private ScheduledExecutorService retryExecutor;
    private OTSCallable callable;
    protected int retries = 0;

    public OTSExecutionContext(Req request, OTSBasicFuture<Res> future, OTSTraceLogger traceLogger,
                               OTSRetryStrategy retryStrategy, ScheduledExecutorService retryExecutor) {
        this.request = request;
        this.future = future;
        this.traceLogger = traceLogger;
        this.retryStrategy = retryStrategy;
        this.retryExecutor = retryExecutor;
    }

    /**
     * asyncClientCallback的创建依赖OTSExecutionContext对象，所以这里要单独set, 且应在setRetryHandler之后。
     * @return
     */
    public void setAsyncClientCallback(FutureCallback futureCallback) {
        this.asyncClientCallback = futureCallback;
    }

    public void setCallable(OTSCallable callable) {
        this.callable = callable;
    }

    public OTSCallable getCallable() {
        return this.callable;
    }

    public void retry(Exception ex) {
        retries++;
    }

    public void setRequest(Req request) {
        this.request = request;
    }

    public Req getRequest() {
        return this.request;
    }

    public OTSBasicFuture<Res> getFuture() {
        return this.future;
    }

    public OTSTraceLogger getTraceLogger() {
        return this.traceLogger;
    }

    public FutureCallback<Res> getAsyncClientCallback() {
        return asyncClientCallback;
    }

    public OTSRetryStrategy getRetryStrategy() {
        return retryStrategy;
    }

    public ScheduledExecutorService getRetryExecutor() {
        return retryExecutor;
    }

    public int getRetries() {
        return retries;
    }
}
