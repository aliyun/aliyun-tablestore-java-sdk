package com.alicloud.openservices.tablestore.model;

public interface RetryStrategy {

    /**
     * Returns a RetryStrategy object of the same type that has not been retried.
     *
     * @return A RetryStrategy object of the same type that has not been retried.
     */
    public RetryStrategy clone();
    
    /**
     * Returns the number of retries for the current operation
     *
     * @return the number of retries for the current operation
     */
    public int getRetries();

    /**
     * Get the delay time before the retries-th retry is initiated. The SDK will initiate the retries-th retry after this period of time.
     * If the return value is less than or equal to 0, it means no retry will be attempted.
     *
     * @param action  Operation name, such as "ListTable", "GetRow", "PutRow", etc.
     * @param ex      Error information from the last failed attempt, either a ClientException or a TableStoreException
     * @return The delay time before the retries-th retry is initiated (in milliseconds). A value less than or equal to 0 indicates that the operation is not retryable.
     */
    public long nextPause(String action, Exception ex);
}
