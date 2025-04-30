package com.alicloud.openservices.tablestore.model.timeseries;

public class TimeseriesLastpointIndex {

    private final String lastpointIndexName;

    public TimeseriesLastpointIndex(String lastpointIndexName) {
        this.lastpointIndexName = lastpointIndexName;
    }

    public String getLastpointIndexName() {
        return lastpointIndexName;
    }

    @Override
    public String toString() {
        return "TimeseriesLastpointIndex [lastpointIndexName=" + lastpointIndexName + "]";
    }
}
