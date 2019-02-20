package com.alicloud.openservices.tablestore.model.tunnel;

public enum ChannelStatus {
    /**
     * 等待。
     */
    WAIT,
    /**
     * 打开。
     */
    OPEN,
    /**
     * 关闭中。
     */
    CLOSING,
    /**
     * 关闭。
     */
    CLOSE,
    /**
     * 结束。
     */
    TERMINATED,
}
