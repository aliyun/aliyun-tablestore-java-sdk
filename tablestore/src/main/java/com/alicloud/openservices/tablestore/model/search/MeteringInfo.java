package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.model.ReservedThroughput;

public class MeteringInfo {

    /**
     * The reserved throughput of the index table.
     */
    private ReservedThroughput reservedThroughput;

    /**
     * The storage size of the index table, this size is the value statistics at the last metering time (obtained through timestamp), not the value at the current moment.
     */
    private long storageSize;

    /**
     * The total number of rows in the index table. This value is the result of the last measurement (the measurement time can be obtained through timestamp) and does not represent the current value.
     */
    private long rowCount;

    /**
     * The time of the last metering on the index table.
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
