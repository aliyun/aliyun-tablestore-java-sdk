package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

public class SplitTimeseriesScanTaskRequest implements Request {

    private String timeseriesTableName;
    private String measurementName;
    private int splitCountHint;

    public SplitTimeseriesScanTaskRequest(String timeseriesTableName) {
        this.timeseriesTableName = timeseriesTableName;
    }

    public SplitTimeseriesScanTaskRequest(String timeseriesTableName, String measurementName) {
        this.timeseriesTableName = timeseriesTableName;
        this.measurementName = measurementName;
    }

    public SplitTimeseriesScanTaskRequest(String timeseriesTableName, int splitCountHint) {
        this.timeseriesTableName = timeseriesTableName;
        this.splitCountHint = splitCountHint;
    }

    public SplitTimeseriesScanTaskRequest(String timeseriesTableName, String measurementName, int splitCountHint) {
        this.timeseriesTableName = timeseriesTableName;
        this.measurementName = measurementName;
        this.splitCountHint = splitCountHint;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_SPLIT_TIMESERIES_SCAN_TASK;
    }

    public String getTimeseriesTableName() {
        return timeseriesTableName;
    }

    public void setTimeseriesTableName(String timeseriesTableName) {
        this.timeseriesTableName = timeseriesTableName;
    }

    public int getSplitCountHint() {
        return splitCountHint;
    }

    public void setSplitCountHint(int splitCountHint) {
        this.splitCountHint = splitCountHint;
    }

    public String getMeasurementName() {
        return measurementName;
    }

    public void setMeasurementName(String measurementName) {
        this.measurementName = measurementName;
    }
}
