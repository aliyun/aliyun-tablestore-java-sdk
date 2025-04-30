package com.alicloud.openservices.tablestore.model.tunnel;

public enum StartOffsetFlag {
    /**
     * Default, the create time of the tunnel.
     */
    LATEST,
    /**
     *  The earliest stream log time.
     */
    EARLIEST
}
