package com.alicloud.openservices.tablestore.model;

/**
 * Indicates the type of server-side encryption key
 */
public enum SSEKeyType {
    /**
     * Use the service master key of KMS
     */
	SSE_KMS_SERVICE,
    /**
     * Use the customer master key of KMS, supporting user-defined key upload
     */
        SSE_BYOK;
}
