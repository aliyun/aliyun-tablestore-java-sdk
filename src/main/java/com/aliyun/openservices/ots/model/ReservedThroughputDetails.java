package com.aliyun.openservices.ots.model;

public class ReservedThroughputDetails {
    
    /**
     * 当前表的预留吞吐量。
     */
    private CapacityUnit capacityUnit;
    
    /**
     * 最近一次上调读或写CapacityUnit的时间。
     */
    private long lastIncreaseTime;
    
    /**
     * 最近一次下调读或写CapacityUnit的时间。
     */
    private long lastDecreaseTime;
    
    /**
     * 当天总过下调读或写CapacityUnit的次数。
     */
    private int numberOfDecreasesToday;
    
    ReservedThroughputDetails() {
        this(null, 0, 0, 0);
    }
    
    ReservedThroughputDetails(CapacityUnit capacityUnit, long lastIncreaseTime, long lastDecreaseTime, int numberOfDecreasesToday) {
        this.capacityUnit = capacityUnit;
        this.lastIncreaseTime = lastIncreaseTime;
        this.lastDecreaseTime = lastDecreaseTime;
        this.numberOfDecreasesToday = numberOfDecreasesToday;
    }
    
    /**
     * 获取当前表的CapacityUnit设置。
     * @return 当前表的CapacityUnit设置。
     */
    public CapacityUnit getCapacityUnit() {
        return capacityUnit;
    }
    
    void setCapacityUnit(CapacityUnit capacityUnit) {
        this.capacityUnit = capacityUnit;
    }

    /**
     * 获取最近一次上调读或写CapacityUnit的时间。
     * @return 最近一次上调读或写CapacityUnit的时间。
     */
    public long getLastIncreaseTime() {
        return lastIncreaseTime;
    }

    void setLastIncreaseTime(long lastIncreaseTime) {
        this.lastIncreaseTime = lastIncreaseTime;
    }

    /**
     * 获取最近一次下调读或写CapacityUnit的时间。若用户未下调过CapacityUnit，则返回时间为0。
     * @return 最近一次下调读或写CapacityUnit的时间。
     */
    public long getLastDecreaseTime() {
        return lastDecreaseTime;
    }

    void setLastDecreaseTime(long lastDecreaseTime) {
        this.lastDecreaseTime = lastDecreaseTime;
    }

    /**
     * 获取当天总过下调读或写CapacityUnit的次数。
     * @return 当天总过下调读或写CapacityUnit的次数。
     */
    public int getNumberOfDecreasesToday() {
        return numberOfDecreasesToday;
    }

    void setNumberOfDecreasesToday(int numberOfDecreasesToday) {
        this.numberOfDecreasesToday = numberOfDecreasesToday;
    }
}
