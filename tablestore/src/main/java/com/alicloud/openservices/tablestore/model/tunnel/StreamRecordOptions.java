package com.alicloud.openservices.tablestore.model.tunnel;

import com.alicloud.openservices.tablestore.model.StreamColumn;

public class StreamRecordOptions {
    private boolean getVersionGeneratorValue;
    private boolean getSysColumns;
    private boolean getNewRowInfo;
    private StreamColumn oldColumnsToGet;
    private StreamColumn newColumnsToGet;

    public StreamRecordOptions() {
    }

    public StreamRecordOptions(StreamColumn oldColumnsToGet, StreamColumn newColumnsToGet) {
        this.oldColumnsToGet = oldColumnsToGet;
        this.newColumnsToGet = newColumnsToGet;
    }

    public boolean isGetVersionGeneratorValue() {
        return getVersionGeneratorValue;
    }

    public void setGetVersionGeneratorValue(boolean getVersionGeneratorValue) {
        this.getVersionGeneratorValue = getVersionGeneratorValue;
    }

    public boolean isGetSysColumns() {
        return getSysColumns;
    }

    public void setGetSysColumns(boolean getSysColumns) {
        this.getSysColumns = getSysColumns;
    }

    public boolean isGetNewRowInfo() {
        return getNewRowInfo;
    }

    public void setGetNewRowInfo(boolean getNewRowInfo) {
        this.getNewRowInfo = getNewRowInfo;
    }

    public StreamColumn getOldColumnsToGet() {
        return oldColumnsToGet;
    }

    public void setOldColumnsToGet(StreamColumn oldColumnsToGet) {
        this.oldColumnsToGet = oldColumnsToGet;
    }

    public StreamColumn getNewColumnsToGet() {
        return newColumnsToGet;
    }

    public void setNewColumnsToGet(StreamColumn newColumnsToGet) {
        this.newColumnsToGet = newColumnsToGet;
    }
}
