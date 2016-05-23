package com.aliyun.openservices.ots.internal;

import com.aliyun.openservices.ots.*;

public class OTSDefaultRetryStrategy implements OTSRetryStrategy {

    private static final int DEFAULT_RETRY_PAUSE_SCALE_IN_MILLIS = 50; // milliseconds.
    private static final int DEFAULT_MAX_RETRY_PAUSE_IN_MILLIS = 100 * 1000;
    private static final int DEFAULT_MAX_RETRY_TIMES = 3;

    private int retryPauseInMillis = DEFAULT_RETRY_PAUSE_SCALE_IN_MILLIS;
    private int maxPauseInMillis = DEFAULT_MAX_RETRY_PAUSE_IN_MILLIS;
    private int maxRetryTimes = DEFAULT_MAX_RETRY_TIMES;

    public int getRetryPauseInMillis() {
        return retryPauseInMillis;
    }

    public void setRetryPauseInMillis(int retryPauseInMillis) {
        this.retryPauseInMillis = retryPauseInMillis;
    }

    public int getMaxRetryTimes() {
        return maxRetryTimes;
    }

    public void setMaxRetryTimes(int maxRetryTimes) {
        this.maxRetryTimes = maxRetryTimes;
    }

    public int getMaxPauseInMillis() {
        return maxPauseInMillis;
    }

    public void setMaxPauseInMillis(int maxPauseInMillis) {
        this.maxPauseInMillis = maxPauseInMillis;
    }

    private boolean isIdempotent(String action) {
        if (action.equals(OTSActionNames.ACTION_BATCH_GET_ROW) || action.equals(OTSActionNames.ACTION_GET_RANGE) ||
                action.equals(OTSActionNames.ACTION_DESCRIBE_TABLE) || action.equals(OTSActionNames.ACTION_GET_ROW) ||
                action.equals(OTSActionNames.ACTION_LIST_TABLE)) {
            return true;
        } else {
            return false;
        }
    }

    private boolean retryNotMatterActions(String errorCode, String errorMessage) {
        if (errorCode.equals(OTSErrorCode.ROW_OPERATION_CONFLICT) || errorCode.equals(OTSErrorCode.NOT_ENOUGH_CAPACITY_UNIT)
                || errorCode.equals(OTSErrorCode.TABLE_NOT_READY) || errorCode.equals(OTSErrorCode.PARTITION_UNAVAILABLE)
                || errorCode.equals(OTSErrorCode.SERVER_BUSY)
                || (errorCode.equals(OTSErrorCode.QUOTA_EXHAUSTED) && errorMessage.equals("Too frequent table operations."))) {
            return true;
        } else {
            return false;
        }
    }

    public boolean shouldRetryWithOTSException(String action, boolean isIdempotent, String errorCode, String errorMessage, int httpStatus) {
        if (retryNotMatterActions(errorCode, errorMessage)) {
            return true;
        }

        boolean serverError = httpStatus >= 500 && httpStatus <= 599;
        if (isIdempotent &&
                (errorCode.equals(OTSErrorCode.STORAGE_TIMEOUT) || errorCode.equals(OTSErrorCode.INTERNAL_SERVER_ERROR) ||
                        errorCode.equals(OTSErrorCode.SERVER_UNAVAILABLE) || serverError)) {
            return true;
        }
        return false;
    }

    /**
     * SDK提供的默认重试策略，规则为：
     *   1. 若异常为OTSException，且错误码为OTSRowOperationConflict, OTSNotEnoughCapacityUnit, OTSTableNotReady,
     *      OTSPartitionUnavailable或OTSServerBusy，则可以重试。
     *   2. 若异常为OTSQuotaExhausted且异常消息为"Too frequent table operations."，则可以重试。
     *   3. 若异常为ClientException（网络类异常），且操作是幂等的，则可以重试。
     *   4. 若异常为OTSTimeout, OTSInternalServerError, OTSServerUnavailable或http状态码为500， 502或503，且操作为幂等的，则可以重试。
     *
     * 默认的重试策略会把所有读相关的操作认为是幂等的，而所有写相关的操作会被认为是非幂等的，若用户对写有重试的需求，需要定制重试策略。
     *
     * @param action  操作名，比如"ListTable"、"GetRow"、"PutRow"等
     * @param ex      上次访问失败的错误信息、为ClientException或OTSException
     * @param retries 表示本次判断的为第retries次重试，retries 大于 0
     * @return  是否需要重试
     */
    @Override
    public boolean shouldRetry(String action, Exception ex, int retries) {
        if (retries > maxRetryTimes) {
            return false;
        }

        boolean isIdempotent = isIdempotent(action);
        if (ex instanceof OTSException) {
            if (ex instanceof PartialResultFailedException) {
                PartialResultFailedException prfe = (PartialResultFailedException) ex;
                for (OTSException otsException : prfe.getErrors()) {
                    if (!shouldRetryWithOTSException(action, isIdempotent, otsException.getErrorCode(), otsException.getMessage(), prfe.getHttpStatus())) {
                        return false;
                    }
                }
                return true;
            } else {
                OTSException otsException = (OTSException) ex;
                return shouldRetryWithOTSException(action, isIdempotent, otsException.getErrorCode(), otsException.getMessage(), otsException.getHttpStatus());
            }
        } else if (ex instanceof ClientException) {
            return isIdempotent;
        } else {
            return false;
        }
    }

    /**
     * 返回本次重试需要等待的间隔时间。
     * 默认的重试策略中重试间隔时间的计算公式为: Math.pow(2, retries) * {@link #retryPauseInMillis}。
     *
     * @param action  操作名，比如"ListTable"、"GetRow"、"PutRow"等
     * @param ex      上次访问失败的错误信息、为ClientException或OTSException
     * @param retries 表示将要发起第retries次重试， retries 大于 0
     * @return
     */
    @Override
    public long getPauseDelay(String action, Exception ex, int retries) {
        // make the pause time increase exponentially
        // based on an assumption that the more times it retries,
        // the less probability it succeeds.
        int scale = retryPauseInMillis;

        // avoid overflow
        if (retries > 30) {
            return maxPauseInMillis;
        }
        long delay = (long) Math.pow(2, retries) * scale;
        return delay;
    }


}
