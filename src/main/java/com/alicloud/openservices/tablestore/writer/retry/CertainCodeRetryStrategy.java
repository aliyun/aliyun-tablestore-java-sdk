package com.alicloud.openservices.tablestore.writer.retry;

import com.alicloud.openservices.tablestore.core.ErrorCode;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

import java.util.concurrent.TimeUnit;

/**
 * writer supported operation:
 * BatchWriteRow, BulkImportRow, PutRow, UpdateRow, DeleteRow
 */
public class CertainCodeRetryStrategy extends BaseWriterRetryStrategy {

    public CertainCodeRetryStrategy() {
        super(10, TimeUnit.SECONDS);
    }

    public CertainCodeRetryStrategy(long timeout, TimeUnit unit) {
        super(timeout, unit);
    }

    @Override
    public RetryStrategy clone() {
        return new CertainCodeRetryStrategy(this.timeout, TimeUnit.MILLISECONDS);
    }

    /**
     * 可以重试的误码集合：其他默认不重试
     * OTSInternalServerError, OTSRequestTimeout, OTSPartitionUnavailable, OTSTableNotReady,
     * OTSRowOperationConflict, OTSTimeout, OTSServerUnavailable, OTSServerBusy,则重试
     **/
    protected boolean retryNotMatterActions(String errorCode) {
        if (ErrorCode.INTERNAL_SERVER_ERROR.equals(errorCode) || ErrorCode.REQUEST_TIMEOUT.equals(errorCode)
                || ErrorCode.PARTITION_UNAVAILABLE.equals(errorCode) || ErrorCode.TABLE_NOT_READY.equals(errorCode)
                || ErrorCode.ROW_OPERATION_CONFLICT.equals(errorCode) || ErrorCode.STORAGE_TIMEOUT.equals(errorCode)
                || ErrorCode.SERVER_UNAVAILABLE.equals(errorCode) || ErrorCode.SERVER_BUSY.equals(errorCode)) {
            return true;
        } else {
            return false;
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
