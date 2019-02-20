package com.alicloud.openservices.tablestore.tunnel.worker;

public enum ChannelConnectStatus {
    /**
     * 等待状态，ChannelConnect的初始状态。
     */
    WAIT,
    /**
     * 运行状态。
     */
    RUNNING,
    /**
     * 关闭中。
     */
    CLOSING,
    /**
     * 已关闭。
     */
    CLOSED
}
