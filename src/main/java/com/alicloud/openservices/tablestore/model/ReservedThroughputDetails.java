package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

public class ReservedThroughputDetails implements Jsonizable {

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

    public ReservedThroughputDetails(CapacityUnit capacityUnit, long lastIncreaseTime, long lastDecreaseTime) {
        this.capacityUnit = capacityUnit;
        this.lastIncreaseTime = lastIncreaseTime;
        this.lastDecreaseTime = lastDecreaseTime;
    }

    /**
     * 获取当前表的CapacityUnit设置。
     *
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
     *
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
     *
     * @return 最近一次下调读或写CapacityUnit的时间。
     */
    public long getLastDecreaseTime() {
        return lastDecreaseTime;
    }

    void setLastDecreaseTime(long lastDecreaseTime) {
        this.lastDecreaseTime = lastDecreaseTime;
    }

    @Override
    public String toString() {
        return "" + capacityUnit + ", LastIncreaseTime: " + lastIncreaseTime +
                ", LastDecreaseTime: " + lastDecreaseTime;
    }

    @Override
    public String jsonize() {
        StringBuilder sb = new StringBuilder();
        jsonize(sb, "\n  ");
        return sb.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        sb.append('{');
        sb.append("\"CapacityUnit\": ");
        capacityUnit.jsonize(sb, newline + "  ");
        sb.append(", \"LastIncreaseTime\": ");
        sb.append(lastIncreaseTime);
        sb.append(", \"LastDecreaseTime\": ");
        sb.append(lastDecreaseTime);
        sb.append("}");
    }
}
