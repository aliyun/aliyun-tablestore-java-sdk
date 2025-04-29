package com.alicloud.openservices.tablestore.model.tunnel;

public enum TunnelStage {
    /**
     * Initialization.
     */
    InitBaseDataAndStreamShard,
    /**
     * Full amount processing.
     */
    ProcessBaseData,
    /**
     *  Incremental processing.
     */
    ProcessStream
}
