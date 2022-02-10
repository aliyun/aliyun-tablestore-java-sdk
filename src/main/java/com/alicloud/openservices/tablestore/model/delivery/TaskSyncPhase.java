package com.alicloud.openservices.tablestore.model.delivery;

public enum TaskSyncPhase {
    /**
     * 初始化阶段
     */
    INIT,
    /**
     * 全量消费阶段
     */
    FULL,
    /**
     * 增量消费阶段
     */
    INCR
}
