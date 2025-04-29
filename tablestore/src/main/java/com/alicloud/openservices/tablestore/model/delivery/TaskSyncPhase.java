package com.alicloud.openservices.tablestore.model.delivery;

public enum TaskSyncPhase {
    /**
     * Initialization phase
     */
    INIT,
    /**
     * Full consumption phase
     */
    FULL,
    /**
     * Incremental consumption phase
     */
    INCR
}
