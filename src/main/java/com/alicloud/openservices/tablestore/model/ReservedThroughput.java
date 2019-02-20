package com.alicloud.openservices.tablestore.model;

public class ReservedThroughput {
    
    /**
     * 表的预留吞吐量配置。
     */
    private CapacityUnit capacityUnit;

    /**
     * 初始化{@link ReservedThroughput}，使用默认的预留读写吞吐量配置（0单位的读能力单元和0单位的写能力单元）。
     */
    public ReservedThroughput() {
        capacityUnit = new CapacityUnit(0, 0);
    }

    /**
     * 初始化{@link ReservedThroughput}.
     * @param capacityUnit 表的预留读写吞吐量设置。
     */
    public ReservedThroughput(CapacityUnit capacityUnit) {
        setCapacityUnit(capacityUnit);
    }

    /**
     * 初始化{@link ReservedThroughput}.
     * @param read 表的预留读吞吐量的配置。
     * @param write 表的预留写吞吐量的配置。
     */
    public ReservedThroughput(int read, int write) {
        setCapacityUnit(new CapacityUnit(read, write));
    }

    /**
     * 获取表的预留吞吐量的值。
     * @return CapacityUnit。
     */
    public CapacityUnit getCapacityUnit() {
        return capacityUnit;
    }

    /**
     * 设置表的预留吞吐量的值，必须同时设置读和写能力单元。
     * @param capacityUnit capacityUnit。
     */
    public void setCapacityUnit(CapacityUnit capacityUnit) {
        this.capacityUnit = capacityUnit;
    }

    @Override
    public int hashCode() {
        return this.capacityUnit.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof ReservedThroughput)){
            return false;
        }

        ReservedThroughput r1 = (ReservedThroughput)o;
        return this.capacityUnit.equals(r1.capacityUnit);
    }
}
