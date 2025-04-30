/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 *
 * Copyright (C) Alibaba Cloud Computing
 */

package com.alicloud.openservices.tablestore.core;

/**
 * Represents the error codes from the Open Table Service (OTS).
 */
public class ErrorCode {

    /**
     * User authentication failed.
     */
    public static final String AUTHORIZATION_FAILURE = "OTSAuthFailed";

    /**
     * Internal server error.
     */
    public static final String INTERNAL_SERVER_ERROR = "OTSInternalServerError";

    /**
     * Parameter error.
     */
    public static final String INVALID_PARAMETER = "OTSParameterInvalid";

    /**
     * The entire request is too large.
     */
    public static final String REQUEST_TOO_LARGE = "OTSRequestBodyTooLarge";

    /**
     * Client request timeout.
     */
    public static final String REQUEST_TIMEOUT = "OTSRequestTimeout";

    /**
     * The user's quota has been fully utilized.
     */
    public static final String QUOTA_EXHAUSTED = "OTSQuotaExhausted";

    /**
     * Internal server failover occurs, causing some partitions of the table to be unavailable for service.
     */
    public static final String PARTITION_UNAVAILABLE = "OTSPartitionUnavailable";

    /**
     * The table cannot provide services immediately after it has been created.
     */
    public static final String TABLE_NOT_READY = "OTSTableNotReady";

    /**
     * The requested table does not exist.
     */
    public static final String OBJECT_NOT_EXIST = "OTSObjectNotExist";

    /**
     * The requested table to be created already exists.
     */
    public static final String OBJECT_ALREADY_EXIST = "OTSObjectAlreadyExist";

    /**
     * Multiple concurrent requests writing the same row of data, causing conflicts.
     */
    public static final String ROW_OPERATION_CONFLICT = "OTSRowOperationConflict";

    /**
     * Primary key mismatch.
     */
    public static final String INVALID_PK = "OTSInvalidPK";

    /**
     * Read and write capacity adjustments are too frequent.
     */
    public static final String TOO_FREQUENT_RESERVED_THROUGHPUT_ADJUSTMENT
        = "OTSTooFrequentReservedThroughputAdjustment";

    /**
     * The total number of columns in this row exceeds the limit.
     */
    public static final String OUT_OF_COLUMN_COUNT_LIMIT = "OTSOutOfColumnCountLimit";

    /**
     * The total size of all column data in this row exceeds the limit.
     */
    public static final String OUT_OF_ROW_SIZE_LIMIT = "OTSOutOfRowSizeLimit";

    /**
     * Insufficient remaining reserved read/write capacity.
     */
    public static final String NOT_ENOUGH_CAPACITY_UNIT = "OTSNotEnoughCapacityUnit";

    /**
     * Pre-check condition failed.
     */
    public static final String CONDITION_CHECK_FAIL = "OTSConditionCheckFail";

    /**
     * Internal operation timeout in OTS.
     */
    public static final String STORAGE_TIMEOUT = "OTSTimeout";

    /**
     * There are inaccessible servers inside the OTS.
     */
    public static final String SERVER_UNAVAILABLE = "OTSServerUnavailable";

    /**
     * The internal server of OTS is busy.
     */
    public static final String SERVER_BUSY = "OTSServerBusy";

    /**
     * The Tunnel resource is unavailable, for example, due to a Heartbeat timeout or a version status conflict in the Channel.
     */
    public static final String RESOURCE_GONE = "OTSResourceGone";

    /**
     * Tunnel service is unavailable.
     */
    public static final String TUNNEL_SERVER_UNAVAILABLE = "OTSTunnelServerUnavailable";

    /**
     * Tunnel Checkpoint sequence number conflict.
     */
    public static final String SEQUENCE_NUMBER_NOT_MATCH = "OTSSequenceNumberNotMatch";

    /**
     * XXX: User usage error.
     */
    public static final String CLIENT_ERROR = "OTSClientError";

    /**
     * This Tunnel has expired.
     */
    public static final String TUNNEL_EXPIRED = "OTSTunnelExpired";

    /**
     * The Tunnel already exists.
     */
    public static final String TUNNEL_EXIST = "OTSTunnelExist";
}
