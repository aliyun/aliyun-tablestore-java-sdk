package com.alicloud.openservices.tablestore.model;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.PartialResultFailedException;
import com.alicloud.openservices.tablestore.core.ErrorCode;

import static com.alicloud.openservices.tablestore.model.OperationNames.*;

/**
 * The TableStore SDK supports custom retry logic {@link RetryStrategy}, which is used to determine whether a retry is needed when an exception occurs, and provides the time interval for the current retry.
 * {@link DefaultRetryStrategy} is the default retry logic of the TableStore SDK.
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
     * The default retry strategy provided by the SDK, with the following rules:
     * 1. If the exception is TableStoreException and the error code is OTSRowOperationConflict, OTSNotEnoughCapacityUnit, OTSTableNotReady,
     * OTSPartitionUnavailable, or OTSServerBusy, then it can be retried.
     * 2. If the exception is OTSQuotaExhausted and the exception message is "Too frequent table operations.", then it can be retried.
     * 3. If the exception is ClientException (a network-related exception), and the operation is idempotent, then it can be retried.
     * 4. If the exception is OTSTimeout, OTSInternalServerError, OTSServerUnavailable, OTSTunnelServerUnavailable, or the HTTP status code is 500, 502, or 503, and the operation is idempotent, then it can be retried.
     * 5. For batch operations, the entire batch operation can only be retried if all failed rows are eligible for retry.
     *
     * The default retry strategy considers all read-related operations as idempotent, while all write-related operations are considered non-idempotent. If users have retry requirements for writes, they need to customize the retry strategy.
     *
     * @param action Operation name, such as "ListTable", "GetRow", "PutRow", etc.
     * @param ex     Error information from the last failed attempt, either a ClientException or OTSException
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
