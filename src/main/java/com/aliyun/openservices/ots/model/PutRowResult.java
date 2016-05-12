/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.model;

public class PutRowResult extends OTSResult {
    /**
     * 此次操作消耗的CapacityUnit。
     */
    private ConsumedCapacity consumedCapacity;

    public PutRowResult(OTSResult meta) {
        super(meta);
    }

    /**
     * 获取此次操作消耗的CapacityUnit。
     * @return 此次操作消耗的CapacityUnit。
     */
    public ConsumedCapacity getConsumedCapacity() {
        return consumedCapacity;
    }

    void setConsumedCapacity(ConsumedCapacity consumedCapacity) {
        this.consumedCapacity = consumedCapacity;
    }
}
