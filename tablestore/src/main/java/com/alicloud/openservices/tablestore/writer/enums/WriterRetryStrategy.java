package com.alicloud.openservices.tablestore.writer.enums;


public enum WriterRetryStrategy {

    /**
     * Given a set of error codes that need to be retried.
     * Error codes not in the given set will not be retried.
     *
     * Retry list:
     * OTSInternalServerError, OTSRequestTimeout, OTSPartitionUnavailable, OTSTableNotReady,
     * OTSRowOperationConflict, OTSTimeout, OTSServerUnavailable, OTSServerBusy,
     */
    CERTAIN_ERROR_CODE_RETRY,

    /**
     * Given a set of error codes that do not require retrying.
     * All non-given error codes will be retried.
     *
     * Do not retry list:
     * OTSParameterInvalid, OTSConditionCheckFail, OTSRequestBodyTooLarge,
     * OTSInvalidPK, OTSOutOfColumnCountLimit, OTSOutOfRowSizeLimit,
     */
    CERTAIN_ERROR_CODE_NOT_RETRY,
}
