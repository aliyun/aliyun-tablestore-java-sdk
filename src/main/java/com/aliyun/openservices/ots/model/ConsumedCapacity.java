/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.model;

public class ConsumedCapacity {
    
    private CapacityUnit capacityUnit;
    
    public ConsumedCapacity() {
        
    }

    /**
     * 获取CapacityUnit。
     * @return CapacityUnit。
     */
    public CapacityUnit getCapacityUnit() {
        return capacityUnit;
    }

    /**
     * 设置CapacityUnit。
     * @param capacityUnit capacityUnit。
     */
    public void setCapacityUnit(CapacityUnit capacityUnit) {
        this.capacityUnit = capacityUnit;
    }

}
