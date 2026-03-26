package com.alicloud.openservices.tablestore.model;

/**
 * A retry strategy that never retries.
 * Used for non-idempotent write operations (e.g., AddDocuments, DeleteDocuments)
 * to prevent duplicate side effects caused by automatic retries.
 */
public class NoRetryStrategy implements RetryStrategy {

    @Override
    public RetryStrategy clone() {
        return new NoRetryStrategy();
    }

    @Override
    public int getRetries() {
        return 0;
    }

    @Override
    public long nextPause(String action, Exception ex) {
        return 0;
    }
}
