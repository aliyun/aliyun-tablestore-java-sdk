package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.model.ReservedThroughput;

public class MeteringInfo {

    /**
     * 索引表的预留吞吐量。
     */
    private ReservedThroughput reservedThroughput;

    /**
     * 索引表的存储大小，该大小为上一次计量时(通过timestamp获取计量时间)统计到的值，并非当前时刻的值。
     */
    private long storageSize;

    /**
     * 索引表的总行数，该行数为上一次计量时(通过timestamp获取计量时间)统计到的值，并非当前时刻的值。
     */
    private long rowCount;

    /**
     * 索引表上一次计量的时间。
     */
    private long timestamp;

    public ReservedThroughput getReservedThroughput() {
        return reservedThroughput;
    }

    public void setReservedThroughput(ReservedThroughput reservedThroughput) {
        this.reservedThroughput = reservedThroughput;
    }

    public long getStorageSize() {
        return storageSize;
    }

    public void setStorageSize(long storageSize) {
        this.storageSize = storageSize;
    }

    public long getRowCount() {
        return rowCount;
    }

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
