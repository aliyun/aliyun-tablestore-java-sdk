package com.alicloud.openservices.tablestore.model.delivery;

public enum DeliveryTaskType {

    /**
     * 全量
     */
    BASE,

    /**
     * 增量
     */
    INC,

    /**
     * 全量加增量
     */
    BASE_INC
}
