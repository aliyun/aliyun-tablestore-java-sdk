package com.alicloud.openservices.tablestore.model.tunnel;

public enum TunnelStage {
    /**
     *  初始化。
     */
    InitBaseDataAndStreamShard,
    /**
     *  全量处理。
     */
    ProcessBaseData,
    /**
     *  增量处理。
     */
    ProcessStream
}
