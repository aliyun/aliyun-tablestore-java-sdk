package com.alicloud.openservices.tablestore.tunnel.pipeline;

public interface IBackoff {
    /**
     * Reset the BackOfff to its initial state.
     */
    void reset();

    /**
     * Get the sleep time.
     * @return Sleep time
     */
    long nextBackOffMillis();
}
