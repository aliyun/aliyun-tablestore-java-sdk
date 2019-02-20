package com.alicloud.openservices.tablestore.core;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class CallbackImpledFuture<Req, Res>
  extends AbstractWatchableCallback<Req, Res>
  implements Future<Res> {
    private boolean completed;
    private Res result;
    private Exception ex;

    public CallbackImpledFuture()
    {
        this.completed = false;
    }
    
    @Override
    public void onCompleted(final Req request, final Res result) {
        synchronized (this) {
            if (this.completed) {
                throw new IllegalStateException("completed() must not be invoked twice.");
            }
            this.completed = true;
            this.result = result;
            notifyAll();
        }
        for(TableStoreCallback<Req, Res> downstream : this.downstreams) {
            downstream.onCompleted(request, result);
        }
    }

    @Override
    public void onFailed(final Req request, final Exception ex) {
        synchronized (this) {
            if (this.completed) {
                throw new IllegalStateException("completed() must not be invoked twice.");
            }
            this.completed = true;
            this.ex = ex;
            notifyAll();
        }
        for(TableStoreCallback<Req, Res> downstream : this.downstreams) {
            downstream.onFailed(request, ex);
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        synchronized(this) {
            return this.completed;
        }
    }

    @Override
    public synchronized Res get() throws InterruptedException, ExecutionException  {
        while (!this.completed) {
            wait();
        }
        return getResultWithoutLock();
    }

    @Override
    public Res get(long timeout, TimeUnit unit)
        throws TimeoutException, InterruptedException, ExecutionException {
        Preconditions.checkNotNull(unit, "Time unit should not be null");
        final long endTime = System.currentTimeMillis() + unit.toMillis(timeout);
        synchronized(this) {
            while(true) {
                if (this.completed) {
                    return getResultWithoutLock();
                } else {
                    long waitTime = endTime - System.currentTimeMillis();
                    if (waitTime <= 0) {
                        throw new TimeoutException();
                    } else {
                        wait(waitTime);
                    }
                }
            }
        }
    }


    private Res getResultWithoutLock() throws TableStoreException, ClientException {
        if (this.ex instanceof TableStoreException) {
            // create a new exception as this.ex doesn't has current stack trace
            TableStoreException tmp = (TableStoreException)this.ex;
            TableStoreException newExp = new TableStoreException(tmp.getMessage(), tmp, tmp.getErrorCode(), tmp.getRequestId(), tmp.getHttpStatus());
            newExp.setTraceId(tmp.getTraceId());
            throw newExp;
        } else if (this.ex instanceof ClientException) {
            // create a new exception as this.ex doesn't has current stack trace
            throw new ClientException(this.ex.getMessage(), this.ex, ((ClientException) this.ex).getTraceId());
        } else {
            return this.result;
        }
    }

}

