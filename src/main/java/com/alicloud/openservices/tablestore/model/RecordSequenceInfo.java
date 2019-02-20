package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

public class RecordSequenceInfo implements Jsonizable {

    /**
     *  解决机器时差带来的时序问题的标识
     */
    private int epoch;

    /**
     * Record写入系统的时间
     */
    private long timestamp;

    /**
     * 行位置标识
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
     * 获取行时序信息的epoch
     * @return 返回epoch
     */
    public int getEpoch() {
        return epoch;
    }

    public void setEpoch(int epoch) {
        this.epoch = epoch;
    }

    /**
     * 获取改行插入系统的时间
     * @return 返回行插入系统的时间
     */
    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * 获取改行的行位置标识
     * @return 返回行位置标识
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
