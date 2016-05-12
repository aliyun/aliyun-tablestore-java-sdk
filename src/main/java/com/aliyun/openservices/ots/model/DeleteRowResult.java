/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.model;

public class DeleteRowResult extends OTSResult {
    private ConsumedCapacity consumedCapacity;

    public DeleteRowResult(OTSResult meta) {
        super(meta);
    }

    public ConsumedCapacity getConsumedCapacity() {
        return consumedCapacity;
    }

    public void setConsumedCapacity(ConsumedCapacity consumedCapacity) {
        this.consumedCapacity = consumedCapacity;
    }
}
