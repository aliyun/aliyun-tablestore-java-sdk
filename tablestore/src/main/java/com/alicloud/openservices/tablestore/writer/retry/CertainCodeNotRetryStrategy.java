package com.alicloud.openservices.tablestore.writer.retry;

import com.alicloud.openservices.tablestore.core.ErrorCode;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

import java.util.concurrent.TimeUnit;

/**
 * writer supported operation:
 * BatchWriteRow, BulkImportRow, PutRow, UpdateRow, DeleteRow
 *
 */
public class CertainCodeNotRetryStrategy extends BaseWriterRetryStrategy {

    public CertainCodeNotRetryStrategy() {
        super(10, TimeUnit.SECONDS);
    }

    public CertainCodeNotRetryStrategy(long timeout, TimeUnit unit) {
        super(timeout, unit);
    }

    @Override
    public RetryStrategy clone() {
        return new CertainCodeNotRetryStrategy(super.timeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Error codes that should not be retried: all others will be retried by default.
     * OTSParameterInvalid, OTSConditionCheckFail, OTSRequestBodyTooLarge,
     * OTSInvalidPK, OTSOutOfColumnCountLimit, OTSOutOfRowSizeLimit,
     **/
    protected boolean retryNotMatterActions(String errorCode) {
        if (ErrorCode.INVALID_PARAMETER.equals(errorCode) || ErrorCode.CONDITION_CHECK_FAIL.equals(errorCode)
                || ErrorCode.REQUEST_TOO_LARGE.equals(errorCode) || ErrorCode.INVALID_PK.equals(errorCode)
                || ErrorCode.OUT_OF_COLUMN_COUNT_LIMIT.equals(errorCode) || ErrorCode.OUT_OF_ROW_SIZE_LIMIT.equals(errorCode)
                ) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public boolean shouldRetry( Exception ex) {
        return super.shouldRetry(ex);
    }

    @Override
    public long nextPause(String action, Exception ex) {
        return super.nextPause(action, ex);
    }
}
