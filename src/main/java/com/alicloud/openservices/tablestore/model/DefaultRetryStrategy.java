package com.alicloud.openservices.tablestore.model;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.PartialResultFailedException;
import com.alicloud.openservices.tablestore.core.ErrorCode;

import static com.alicloud.openservices.tablestore.model.OperationNames.*;

/**
 * TableStore SDK支持自定义重试逻辑{@link RetryStrategy}, 重试逻辑用于判断在发生异常时是否需要重试, 并给出本次重试的时间间隔.
 * {@link DefaultRetryStrategy}为TableStore SDK默认的重试逻辑.
 */
public class DefaultRetryStrategy implements RetryStrategy {
    private final int MAX_BASE = 320; // in msec
    private Random rnd = new Random();
    private int retries = 0;
    private long timeout = 0; // in msec
    private int base = 10; // in msec
    private long deadline = 0;

    public DefaultRetryStrategy() {
        this(10, TimeUnit.SECONDS);
    }

    public DefaultRetryStrategy(long timeout, TimeUnit unit) {
        this.timeout = unit.toMillis(timeout);
        this.deadline = System.currentTimeMillis() + this.timeout;
    }

    @Override
    public RetryStrategy clone() {
        return new DefaultRetryStrategy(this.timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public int getRetries() {
        return retries;
    }

    protected boolean retryNotMatterActions(String errorCode, String errorMessage) {
        if (errorCode.equals(ErrorCode.ROW_OPERATION_CONFLICT) || errorCode.equals(ErrorCode.NOT_ENOUGH_CAPACITY_UNIT)
            || errorCode.equals(ErrorCode.TABLE_NOT_READY) || errorCode.equals(ErrorCode.PARTITION_UNAVAILABLE)
            || errorCode.equals(ErrorCode.SERVER_BUSY)
            || (errorCode.equals(ErrorCode.QUOTA_EXHAUSTED) && errorMessage.equals("Too frequent table operations."))) {
            return true;
        } else {
            return false;
        }
    }

    protected boolean shouldRetryWithOTSException(String action, boolean isIdempotent, String errorCode,
                                                String errorMessage, int httpStatus) {
        if (retryNotMatterActions(errorCode, errorMessage)) {
            return true;
        }

        boolean serverError = httpStatus >= 500 && httpStatus <= 599;
        if (isIdempotent &&
            (errorCode.equals(ErrorCode.STORAGE_TIMEOUT)
                || errorCode.equals(ErrorCode.INTERNAL_SERVER_ERROR)
                || errorCode.equals(ErrorCode.SERVER_UNAVAILABLE)
                || errorCode.equals(ErrorCode.TUNNEL_SERVER_UNAVAILABLE)
                || serverError
            )) {
            return true;
        }
        return false;
    }

    /**
     * SDK提供的默认重试策略，规则为：
     * 1. 若异常为TableStoreException，且错误码为OTSRowOperationConflict, OTSNotEnoughCapacityUnit, OTSTableNotReady,
     * OTSPartitionUnavailable或OTSServerBusy，则可以重试。
     * 2. 若异常为OTSQuotaExhausted且异常消息为"Too frequent table operations."，则可以重试。
     * 3. 若异常为ClientException（网络类异常），且操作是幂等的，则可以重试。
     * 4. 若异常为OTSTimeout, OTSInternalServerError, OTSServerUnavailable, OTSTunnelServerUnavailable或http状态码为500， 502或503，且操作为幂等的，则可以重试。
     * 5. 若操作为Batch操作，只有所有失败的行可重试时，批量操作才可以重试
     *
     * 默认的重试策略会把所有读相关的操作认为是幂等的，而所有写相关的操作会被认为是非幂等的，若用户对写有重试的需求，需要定制重试策略。
     *
     * @param action 操作名，比如"ListTable"、"GetRow"、"PutRow"等
     * @param ex     上次访问失败的错误信息、为ClientException或OTSException
     * @return
     */
    public boolean shouldRetry(String action, Exception ex) {
        boolean isIdempotent = IdempotentActionTool.isIdempotentAction(action);
        if (ex instanceof TableStoreException) {
            if (ex instanceof PartialResultFailedException) {
                PartialResultFailedException prfe = (PartialResultFailedException)ex;
                for (TableStoreException otsException : prfe.getErrors()) {
                    if (!shouldRetryWithOTSException(action, isIdempotent, otsException.getErrorCode(),
                        otsException.getMessage(), prfe.getHttpStatus())) {
                        return false;
                    }
                }
                return true;
            } else {
                TableStoreException otsException = (TableStoreException)ex;
                return shouldRetryWithOTSException(action, isIdempotent, otsException.getErrorCode(),
                    otsException.getMessage(), otsException.getHttpStatus());
            }
        } else if (ex instanceof ClientException) {
            return isIdempotent;
        } else {
            return false;
        }
    }

    @Override
    public long nextPause(String action, Exception ex) {
        if (!shouldRetry(action, ex)) {
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
