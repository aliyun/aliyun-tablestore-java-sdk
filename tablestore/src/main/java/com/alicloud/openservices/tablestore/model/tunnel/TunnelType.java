package com.alicloud.openservices.tablestore.model.tunnel;

/**
 * Indicates the type of Tunnel, currently supports three types: {@link #BaseData}, {@link #Stream}, and {@link #BaseAndStream}.
 */
public enum TunnelType {
    /**
     * Full amount type.
     */
    BaseData,

    /**
     * Incremental type.
     */
    Stream,

    /**
     * Full amount plus incremental type.
     */
    BaseAndStream
}
