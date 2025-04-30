package com.alicloud.openservices.tablestore.model;

/**
 * Indicates the index update mode.
 */
public enum IndexUpdateMode {
    /**
     * Asynchronously update the index.
     */
    IUM_ASYNC_INDEX,
    /**
     * Synchronous index update
     */
    IUM_SYNC_INDEX
}
