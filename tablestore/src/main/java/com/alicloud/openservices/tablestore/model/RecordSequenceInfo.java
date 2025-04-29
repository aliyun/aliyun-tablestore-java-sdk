package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

public class RecordSequenceInfo implements Jsonizable {

    /**
     *  Identifier to resolve timing issues caused by machine time differences
     */
    private int epoch;

    /**
     * The time when the record was written into the system
     */
    private long timestamp;

    /**
     * Row position identifier
     */
    private int rowIndex;

    public RecordSequenceInfo() {

    }

    public RecordSequenceInfo(int epoch, long timestamp, int rowIndex) {
        this.epoch = epoch;
        this.timestamp = timestamp;
        this.rowIndex = rowIndex;
    }

    /**
     * Get the epoch of the row's timestamp information
     * @return Return the epoch
     */
    public int getEpoch() {
        return epoch;
    }

    public void setEpoch(int epoch) {
        this.epoch = epoch;
    }

    /**
     * Get the time when the row was inserted into the system
     * @return Returns the time when the row was inserted into the system
     */
    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Get the row location identifier for this row
     * @return Return the row location identifier
     */
    public int getRowIndex() {
        return rowIndex;
    }
    public void setRowIndex(int rowIndex) {
        this.rowIndex = rowIndex;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("(" + "Epoch:" + epoch + ")");
        sb.append("(" + "Timestamp:" + timestamp + ")");
        sb.append("(" + "RowIndex:" + rowIndex + ")");
        return sb.toString();
    }

    @Override
    public String jsonize() {
        StringBuilder sb = new StringBuilder();
        jsonize(sb, "\n  ");
        return sb.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        sb.append("{\"Epoch\": ");
        sb.append(epoch);
        sb.append(", \"Timestamp\": ");
        sb.append(timestamp);
        sb.append(", \"RowIndex\": ");
        sb.append(rowIndex);
        sb.append("}");
    }
}
