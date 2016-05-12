/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.model;

import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.OTSException;
import org.apache.http.util.Args;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class OTSBasicFuture<T> implements OTSFuture<T> {

    private final OTSFutureCallback<T> callback;

    private volatile boolean completed;
    private volatile T result;
    private volatile Exception ex;

    public OTSBasicFuture(final OTSFutureCallback<T> callback) {
        super();
        this.callback = callback;
    }

    public boolean isDone() {
        return this.completed;
    }

    private T getResult() throws OTSException, ClientException {
        if (this.ex instanceof OTSException) {
            // create a new exception as this.ex doesn't has current stack trace
            OTSException tmp = (OTSException)this.ex;
            OTSException newExp = new OTSException(tmp.getMessage(), tmp, tmp.getErrorCode(), tmp.getRequestId(), tmp.getHttpStatus());
            newExp.setTraceId(tmp.getTraceId());
            throw newExp;
        } else if (this.ex instanceof ClientException) {
            // create a new exception as this.ex doesn't has current stack trace
            throw new ClientException(this.ex.getMessage(), this.ex, ((ClientException) this.ex).getTraceId());
        }
        return this.result;
    }

    public synchronized T get() throws OTSException, ClientException {
        while (!this.completed) {
            try {
                wait();
            } catch (InterruptedException e) {
                throw new ClientException(String.format(
                        "The thread was interrupted: %s", e.getMessage()));
            }
        }
        return getResult();
    }

    public synchronized T get(final long timeout, final TimeUnit unit)
            throws OTSException, ClientException, TimeoutException {
        Args.notNull(unit, "Time unit");
        final long msecs = unit.toMillis(timeout);
        final long startTime = (msecs <= 0) ? 0 : System.currentTimeMillis();
        long waitTime = msecs;
        if (this.completed) {
            return getResult();
        } else if (waitTime <= 0) {
            throw new TimeoutException();
        } else {
            for (;;) {
                try {
                    wait(waitTime);
                } catch (InterruptedException e) {
                    throw new ClientException(String.format(
                            "The thread was interrupted: %s", e.getMessage()));
                }
                if (this.completed) {
                    return getResult();
                } else {
                    waitTime = msecs - (System.currentTimeMillis() - startTime);
                    if (waitTime <= 0) {
                        throw new TimeoutException();
                    }
                }
            }
        }
    }

    public boolean completed(final T result) {
        synchronized (this) {
            if (this.completed) {
                return false;
            }
            this.completed = true;
            this.result = result;
            notifyAll();
        }
        if (this.callback != null) {
            this.callback.completed(result);
        }
        return true;
    }

    public boolean failed(final Exception exception) {
        synchronized (this) {
            if (this.completed) {
                return false;
            }
            this.completed = true;
            this.ex = exception;
            notifyAll();
        }
        if (this.callback != null) {
            this.callback.failed(exception);
        }
        return true;
    }
}
