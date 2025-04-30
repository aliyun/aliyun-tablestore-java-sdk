package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

public class ReservedThroughputDetails implements Jsonizable {

    /**
     * The reserved throughput of the current table.
     */
    private CapacityUnit capacityUnit;

    /**
     * The time of the most recent increase in read or write CapacityUnit.
     */
    private long lastIncreaseTime;

    /**
     * The time of the most recent downgrade of read or write CapacityUnit.
     */
    private long lastDecreaseTime;

    public ReservedThroughputDetails(CapacityUnit capacityUnit, long lastIncreaseTime, long lastDecreaseTime) {
        this.capacityUnit = capacityUnit;
        this.lastIncreaseTime = lastIncreaseTime;
        this.lastDecreaseTime = lastDecreaseTime;
    }

    /**
     * Get the CapacityUnit settings for the current table.
     *
     * @return The CapacityUnit settings for the current table.
     */
    public CapacityUnit getCapacityUnit() {
        return capacityUnit;
    }

    void setCapacityUnit(CapacityUnit capacityUnit) {
        this.capacityUnit = capacityUnit;
    }

    /**
     * Get the time of the most recent increase in read or write CapacityUnit.
     *
     * @return The time of the most recent increase in read or write CapacityUnit.
     */
    public long getLastIncreaseTime() {
        return lastIncreaseTime;
    }

    void setLastIncreaseTime(long lastIncreaseTime) {
        this.lastIncreaseTime = lastIncreaseTime;
    }

    /**
     * Get the time of the most recent reduction in read or write CapacityUnit. If the user has never reduced the CapacityUnit, the returned time will be 0.
     *
     * @return The time of the most recent reduction in read or write CapacityUnit.
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
