package com.alicloud.openservices.tablestore.core.protocol;

public class PlainBufferSequenceInfo {

    private int epoch = 0;
    private long timestamp = 0;
    private int rowIndex = 0;
    private boolean hasSeq = false;

    public int getEpoch() {
        return epoch;
    }

    void setEpoch(int epoch) {
        this.epoch = epoch;
        this.hasSeq = true;
    }
    public long getTimestamp() {
        return timestamp;
    }

    void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        this.hasSeq = true;
    }
    public int getRowIndex() {
        return rowIndex;
    }

    void setRowIndex(int rowIndex) {
        this.rowIndex = rowIndex;
        this.hasSeq = true;
    }

    public boolean getHasSeq() {
        return hasSeq;
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("Epoch: " + epoch);
        sb.append(", Timestamp: " + timestamp);
        sb.append(", RowIndex: " + rowIndex);
        return sb.toString();
    }
}
