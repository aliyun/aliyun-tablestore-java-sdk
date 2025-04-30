package com.alicloud.openservices.tablestore.writer.retry;


import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.PartialResultFailedException;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public abstract class BaseWriterRetryStrategy implements RetryStrategy {
    private final int MAX_BASE = 320; // in msec
    private Random rnd = new Random();
    private int retries = 0;
    protected long timeout = 0; // in msec
    private int base = 10; // in msec
    private long deadline = 0;

    public BaseWriterRetryStrategy(long timeout, TimeUnit unit) {
        this.timeout = unit.toMillis(timeout);
        this.deadline = System.currentTimeMillis() + this.timeout;
    }

    @Override
    public abstract RetryStrategy clone();

    @Override
    public int getRetries() {
        return retries;
    }

    /**
     * Error codes that do not require retry:
     * OTSParameterInvalid, OTSConditionCheckFail, OTSRequestBodyTooLarge,
     * OTSInvalidPK, OTSOutOfColumnCountLimit, OTSOutOfRowSizeLimit,
     */
    abstract boolean retryNotMatterActions(String errorCode);

    private boolean shouldRetryWithOTSException(String errorCode, int httpStatus) {
        boolean serverError = httpStatus >= 500 && httpStatus <= 599;

        return retryNotMatterActions(errorCode) || serverError;
    };

    /**
     * A retry strategy specially used for Writer. The rules are as follows:
     * 1. If the exception is TableStoreException and it is retryable, then a retry can be performed.
     * 2. If the exception is ClientException (a network-related exception), then a retry can be performed.
     * 3. If the HTTP status code is 500, 502, or 503, then a retry can be performed.
     * 4. If the operation is a Batch operation, the batch operation can only be retried if all failed rows are retryable.
     *
     * @param ex     Error information from the last failed access attempt; either ClientException or OTSException
     * @return
     */
    protected boolean shouldRetry( Exception ex) {
        if (ex instanceof TableStoreException) {
            if (ex instanceof PartialResultFailedException) {
                PartialResultFailedException prfe = (PartialResultFailedException)ex;
                for (TableStoreException otsException : prfe.getErrors()) {
                    if (!shouldRetryWithOTSException(otsException.getErrorCode(), prfe.getHttpStatus())) {
                        return false;
                    }
                }
                return true;
            } else {
                TableStoreException otsException = (TableStoreException)ex;
                return shouldRetryWithOTSException(otsException.getErrorCode(), otsException.getHttpStatus());
            }
        } else if (ex instanceof ClientException) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public long nextPause(String action, Exception ex) {
        if (!shouldRetry(ex)) {
            return 0;
        }

        if (base <= 0) {
            return 0;
        }

        long now = System.currentTimeMillis();
        int expire = (int)(deadline - now);
        if (expire <= 0) {
            return 0;
        }

        // randomly exponential backoff, in order to make requests sparse.
        long delay = 1 + rnd.nextInt(base < expire ? base : expire);
        ++retries;
        base *= 2;
        if (base > MAX_BASE) {
            base = MAX_BASE;
        }
        return delay;
    }
}
