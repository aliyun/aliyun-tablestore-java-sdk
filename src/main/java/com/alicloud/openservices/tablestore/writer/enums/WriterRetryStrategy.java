package com.alicloud.openservices.tablestore.writer.enums;


public enum WriterRetryStrategy {

    /**
     * 给定需要重试的错误码集合
     * 非给定的错误码，都不做重试
     *
     * 重试列表：
     * OTSInternalServerError, OTSRequestTimeout, OTSPartitionUnavailable, OTSTableNotReady,
     * OTSRowOperationConflict, OTSTimeout, OTSServerUnavailable, OTSServerBusy,
     */
    CERTAIN_ERROR_CODE_RETRY,

    /**
     * 给定不需要重试的错误码集合
     * 非给定的错误码，都做重试
     *
     * 不重试列表：
     * OTSParameterInvalid, OTSConditionCheckFail, OTSRequestBodyTooLarge,
     * OTSInvalidPK, OTSOutOfColumnCountLimit, OTSOutOfRowSizeLimit,
     */
    CERTAIN_ERROR_CODE_NOT_RETRY,
}
