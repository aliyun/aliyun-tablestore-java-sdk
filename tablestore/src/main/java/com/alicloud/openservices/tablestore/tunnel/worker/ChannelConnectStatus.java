package com.alicloud.openservices.tablestore.tunnel.worker;

public enum ChannelConnectStatus {
    /**
     * Waiting status, the initial state of ChannelConnect.
     */
    WAIT,
    /**
     * Running status.
     */
    RUNNING,
    /**
     * Closing.
     */
    CLOSING,
    /**
     * Closed.
     */
    CLOSED
}
