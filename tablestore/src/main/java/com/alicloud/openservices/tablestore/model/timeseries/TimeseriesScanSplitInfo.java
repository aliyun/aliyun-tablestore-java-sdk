package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.core.utils.Base64;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class TimeseriesScanSplitInfo {

    private final byte[] serializedData;

    public TimeseriesScanSplitInfo(byte[] serializedData) {
        Preconditions.checkArgument(serializedData != null && serializedData.length > 0, "task info cannot be empty");
        this.serializedData = serializedData;
    }

    public byte[] getSerializedData() {
        return serializedData;
    }

    @Override
    public String toString() {
        return "TimeseriesScanTaskInfo{" +
                "serializedData=" + Base64.toBase64String(serializedData) +
                '}';
    }
}
