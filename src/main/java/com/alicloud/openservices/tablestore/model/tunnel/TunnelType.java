package com.alicloud.openservices.tablestore.model.tunnel;

/**
 * 表示Tunnel的类型，目前支持{@link #BaseData}、{@link #Stream}、{@link #BaseAndStream} 这三种类型。
 */
public enum TunnelType {
    /**
     * 全量类型。
     */
    BaseData,

    /**
     * 增量类型。
     */
    Stream,

    /**
     * 全量加增量类型。
     */
    BaseAndStream
}
