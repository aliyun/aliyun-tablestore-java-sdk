package com.alicloud.openservices.tablestore.core;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import com.aliyun.ots.thirdparty.org.apache.http.concurrent.FutureCallback;

import com.alicloud.openservices.tablestore.PartialResultFailedException;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.Request;
import com.alicloud.openservices.tablestore.model.Response;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;

public class AsyncCompletion<Req extends Request, Res extends Response>
  extends AbstractWatchableCallback<Req, Res>
  implements FutureCallback<Res> {

    protected Req request;
    protected OperationLauncher launcher;
    protected TraceLogger tracer;
    protected ExecutorService callbackExecutor;
    protected RetryStrategy retry;
    protected ScheduledExecutorService retryExecutor;

    public AsyncCompletion(
        OperationLauncher launcher,
        Req request, TraceLogger tracer,
        ExecutorService callbackExecutor,
        RetryStrategy retry, ScheduledExecutorService retryExecutor)
    {
        Preconditions.checkNotNull(launcher, "launcher must not be null.");
        Preconditions.checkNotNull(request, "request must not be null.");
        Preconditions.checkNotNull(tracer, "tracer must not be null.");
        Preconditions.checkNotNull(callbackExecutor, "callbackExecutor must not be null.");
        Preconditions.checkNotNull(retry, "retry must not be null.");
        Preconditions.checkNotNull(retryExecutor, "retryExecutor must not be null.");

        this.launcher = launcher;
        this.request = request;
        this.tracer = tracer;
        this.callbackExecutor = callbackExecutor;
        this.retry = retry.clone();
        this.retryExecutor = retryExecutor;
    }

    @Override
    public void completed(Res result) {
        try {
            result.setTraceId(tracer.getTraceId());
            LogUtil.logOnCompleted(tracer, retry, result.getRequestId());
            tracer.printLog();
            onCompleted(request, result);
        } catch (Exception ex) {
            LogUtil.LOG.error("unknown error in completed, future or user callback may not be notified", ex);
        }
    }

    @Override
    public void failed(Exception ex) {
        try {
            final Exception e;
            String requestId = null;
            if (ex instanceof TableStoreException) {
                e = ex;
                requestId = ((TableStoreException) ex).getRequestId();
                ((TableStoreException) e).setTraceId(tracer.getTraceId());
            } else if (ex instanceof ClientException) {
                e = ex;
                ((ClientException) e).setTraceId(tracer.getTraceId());
            } else {
                e = new ClientException("Unexpected error: " + ex, ex, tracer.getTraceId());
            }
            long nextPause = retry.nextPause(request.getOperationName(), e);
            LogUtil.logOnFailed(tracer, retry, e, requestId, nextPause > 0);
            if (nextPause <= 0) {
                tracer.printLog();
                if (e instanceof PartialResultFailedException) {
                    onCompleted(this.request, (Res) ((PartialResultFailedException) e).getResult());
                } else {
                    onFailed(request, e);
                }
            } else {
                final AsyncCompletion<Req, Res> self = this;
                retryExecutor.schedule(
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                Req requestForRetry = (Req) launcher.getRequestForRetry(e);
                                launcher.fire(requestForRetry, self);
                            } catch (Exception ex) {
                                failed(new ClientException("Fail to retry.", ex));
                            }
                        }
                    },
                    nextPause, TimeUnit.MILLISECONDS);
            }
        } catch (Exception exception) {
            LogUtil.LOG.error("unknown error in failed, future or user callback may not be notified", exception);
        }
    }

    @Override public void cancelled() {
        failed(new ClientException("request cancelled"));
    }

    @Override public void onCompleted(final Req req, final Res res) {
        for(final TableStoreCallback<Req, Res> cb : downstreams) {
            callbackExecutor.submit(
                new Runnable() {
                    @Override
                    public void run() {
                        cb.onCompleted(req, res);
                    }
                });
        }
    }

    @Override public void onFailed(final Req req, final Exception ex) {
        for(final TableStoreCallback<Req, Res> cb : downstreams) {
            callbackExecutor.submit(
                new Runnable() {
                    @Override
                    public void run() {
                        cb.onFailed(req, ex);
                    }
                });
        }
    }
}
