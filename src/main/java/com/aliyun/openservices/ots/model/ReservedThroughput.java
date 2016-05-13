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
