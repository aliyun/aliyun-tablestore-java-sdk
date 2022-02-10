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
     * 不做重试的误码集合：
     * OTSParameterInvalid, OTSConditionCheckFail, OTSRequestBodyTooLarge,
     * OTSInvalidPK, OTSOutOfColumnCountLimit, OTSOutOfRowSizeLimit,
     **/
    abstract boolean retryNotMatterActions(String errorCode);

    private boolean shouldRetryWithOTSException(String errorCode, int httpStatus) {
        boolean serverError = httpStatus >= 500 && httpStatus <= 599;

        return retryNotMatterActions(errorCode) || serverError;
    };

    /**
     * Writer特殊使用的重试策略，规则为：
     * 1. 若异常为TableStoreException，且为可重试时，可以重试。
     * 2. 若异常为ClientException（网络类异常），则可以重试。
     * 3. 若http状态码为500， 502或503，则可以重试。
     * 4. 若操作为Batch操作，只有所有失败的行可重试时，批量操作才可以重试
     *
     * @param ex     上次访问失败的错误信息、为ClientException或OTSException
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
