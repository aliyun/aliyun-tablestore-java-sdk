/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.model;

public class ReservedThroughput {
    
    /**
     * 表的预留吞吐量。
     */
    private CapacityUnit capacityUnit;
    
    public ReservedThroughput() {
        
    }
    
    public ReservedThroughput(CapacityUnit capacityUnit) {
        this.capacityUnit = capacityUnit;
    }

    /**
     * 获取表的预留吞吐量的值。
     * @return CapacityUnit。
     */
    public CapacityUnit getCapacityUnit() {
        return capacityUnit;
    }

    /**
     * 设置表的预留吞吐量的值。
     * @param capacityUnit capacityUnit。
     */
    public void setCapacityUnit(CapacityUnit capacityUnit) {
        this.capacityUnit = capacityUnit;
    }

}
