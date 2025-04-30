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
public class CustomRetryStrategy implements RetryStrategy {
    private final int MAX_BASE = 2000; // in msec
    private final Random rnd = new Random();
    private volatile int base = 100; // in msec
    private volatile int retries = 0;
    private volatile long timeout = 0; // in msec
    private volatile long deadline = 0;
    private final boolean retryUnIdempotentWriteOperation;

    public CustomRetryStrategy() {
        this(10, TimeUnit.SECONDS);
    }

    public CustomRetryStrategy(long timeout, TimeUnit unit) {
        this(timeout, unit, false);
    }

    /**
     * @param timeout Retry timeout duration
     * @param unit Timeout duration unit
     * @param retryUnIdempotentWriteOperation Whether to retry write operations in cases where idempotence is not guaranteed
     */
    public CustomRetryStrategy(long timeout, TimeUnit unit, boolean retryUnIdempotentWriteOperation) {
        this.timeout = unit.toMillis(timeout);
        this.deadline = System.currentTimeMillis() + this.timeout;
        this.retryUnIdempotentWriteOperation = retryUnIdempotentWriteOperation;
    }

    @Override
    public RetryStrategy clone() {
        return new CustomRetryStrategy(this.timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public int getRetries() {
        return retries;
    }

    /**
     * For the following error types, it can be clearly determined that the operation was not actually executed (does not affect idempotency), so both read or write operations can be retried explicitly.
     * @param errorCode
     * @param errorMessage
     * @return
     */
    private boolean retryNotMatterActions(String errorCode, String errorMessage) {
        if (errorCode.equals(ErrorCode.ROW_OPERATION_CONFLICT) || errorCode.equals(ErrorCode.NOT_ENOUGH_CAPACITY_UNIT)
                || errorCode.equals(ErrorCode.TABLE_NOT_READY) || errorCode.equals(ErrorCode.PARTITION_UNAVAILABLE)
                || errorCode.equals(ErrorCode.SERVER_BUSY)
                || (errorCode.equals(ErrorCode.QUOTA_EXHAUSTED) && errorMessage.equals("Too frequent table operations."))) {
            return true;
        } else {
            return false;
        }
    }

    private boolean shouldRetryWithOTSException(String action, boolean isIdempotent, String errorCode,
                                                String errorMessage, int httpStatus) {
        /**
         * When these errors occur, retry regardless of whether it is a read or write operation.
         */
        if (retryNotMatterActions(errorCode, errorMessage)) {
            return true;
        }

        // Determine whether it is a service layer error.
        boolean serverError = (httpStatus >= 500 && httpStatus <= 599)
                            || errorCode.equals(ErrorCode.STORAGE_TIMEOUT)
                            || errorCode.equals(ErrorCode.INTERNAL_SERVER_ERROR)
                            || errorCode.equals(ErrorCode.SERVER_UNAVAILABLE)
                            || errorCode.equals(ErrorCode.TUNNEL_SERVER_UNAVAILABLE);

        // For service layer errors, and the operation is an idempotent operation, retry.
        if (serverError && isIdempotent) {
            return true;
        }

        // For service-level errors, and for write operations (non-idempotent), the retryUnIdempotentWriteOperation switch is used to determine whether to retry.
        if (serverError && retryUnIdempotentWriteOperation && (OP_UPDATE_ROW.equals(action)
                                                || OP_PUT_ROW.equals(action)
                                                || OP_BATCH_WRITE_ROW.equals(action))) {
            return true;
        }

        // For cases other than the above, no retries will be attempted.
        return false;
    }

    /**
     * Idempotent operations:
     * In this strategy, all read-related operations are considered idempotent, while all write-related operations are considered non-idempotent.
     *
     * Retry strategy, the rules are as follows:
     * 1. If the exception is TableStoreException, it indicates a clear exception returned from the server, and whether it can be retried is judged by shouldRetryWithOTSException.
     *    Specifically, for batch operations where some rows fail, retries will only occur if all failed rows are retryable.
     * 2. If the exception is ClientException, it generally represents network errors or similar issues where no server response was received; in this case, only idempotent operations are retried.
     *
     * @param action Operation name, such as "ListTable", "GetRow", "PutRow", etc.
     * @param ex     Error information from the last failed access attempt, either a ClientException or OTSException
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

    /**
     * If it returns 0, it means not retryable; otherwise, it returns the interval time for this retry.
     *
     * The base value of the retry interval will double with the number of retries, but never exceed MAX_BASE.
     * For the smoothness of requests, the actual retry interval will have some random fluctuations based on the base.
     *
     * @param action  Operation name, such as "ListTable", "GetRow", "PutRow", etc.
     * @param ex      Error information from the last failed attempt, either ClientException or TableStoreException
     * @return
     */
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
        long delay = Math.min(base / 2 + rnd.nextInt(base), expire);
        ++retries;
        base *= 2;
        if (base > MAX_BASE) {
            base = MAX_BASE;
        }
        return delay;
    }
}
