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
     * @param timeout 重试超时时间
     * @param unit 超时时间单位
     * @param retryUnIdempotentWriteOperation 是否在可能非幂等的情况下重试写操作
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

    private boolean isIdempotent(String action) {
        /**
         * all read operations are idempotent
         */
        if (action.equals(OP_BATCH_GET_ROW) || action.equals(OP_DESCRIBE_TABLE) ||
                action.equals(OP_GET_RANGE) || action.equals(OP_GET_ROW) ||
                action.equals(OP_LIST_TABLE) || action.equals(OP_LIST_TUNNEL) ||
                action.equals(OP_DESCRIBE_TUNNEL) || action.equals(OP_READRECORDS)) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 对于以下错误类型，可以明确操作并未实际执行(不影响幂等性)，所以不论是读操作或者写操作，都是明确可以重试的。
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
         * 这些错误发生时，不论是读或写操作，都进行重试。
         */
        if (retryNotMatterActions(errorCode, errorMessage)) {
            return true;
        }

        // 判断是否为服务层错误。
        boolean serverError = (httpStatus >= 500 && httpStatus <= 599)
                            || errorCode.equals(ErrorCode.STORAGE_TIMEOUT)
                            || errorCode.equals(ErrorCode.INTERNAL_SERVER_ERROR)
                            || errorCode.equals(ErrorCode.SERVER_UNAVAILABLE)
                            || errorCode.equals(ErrorCode.TUNNEL_SERVER_UNAVAILABLE);

        // 对于服务层错误，且操作为幂等操作，进行重试。
        if (serverError && isIdempotent) {
            return true;
        }

        // 对于服务层错误，且操作为写操作(非幂等)，由retryUnIdempotentWriteOperation这个开关来判断是否重试。
        if (serverError && retryUnIdempotentWriteOperation && (OP_UPDATE_ROW.equals(action)
                                                || OP_PUT_ROW.equals(action)
                                                || OP_BATCH_WRITE_ROW.equals(action))) {
            return true;
        }

        // 非以上情况，都不重试。
        return false;
    }

    /**
     * 幂等操作：
     * 本策略中，认为所有读相关的操作是幂等的，而所有写相关的操作会被认为是非幂等的。
     *
     * 重试策略，规则为：
     * 1. 若异常为TableStoreException，则明确收到了服务端返回的异常，通过shouldRetryWithOTSException判断是否可重试。
     *    特别的是，对于batch操作部分行失败的情况，仅当所有失败行都可重试时，才会进行重试。
     * 2. 若异常为ClientException，一般为网络错误等，没有收到服务端响应，此时只重试幂等操作。
     *
     * @param action 操作名，比如"ListTable"、"GetRow"、"PutRow"等
     * @param ex     上次访问失败的错误信息、为ClientException或OTSException
     * @return
     */
    public boolean shouldRetry(String action, Exception ex) {
        boolean isIdempotent = isIdempotent(action);
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
     * 若返回0，代表不可重试，否则返回本次重试的间隔时间。
     *
     * 重试间隔的基础值(base)，会随着重试次数倍增，最大不超过MAX_BASE。
     * 为了请求的平滑性考虑，实际的重试间隔会在base基础上做一些随机的浮动。
     *
     * @param action  操作名，比如"ListTable"、"GetRow"、"PutRow"等
     * @param ex      上次访问失败的错误信息、为ClientException或TableStoreException
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
