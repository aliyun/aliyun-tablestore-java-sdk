package com.alicloud.openservices.tablestore.model.timeseries;

public class TimeseriesTableRow {
    private TimeseriesRow timeseriesRow;
    private String tableName;

    public TimeseriesTableRow(TimeseriesRow timeseriesRow, String tableName) {
        this.timeseriesRow = timeseriesRow;
        this.tableName = tableName;
    }

    public TimeseriesRow getTimeseriesRow() {
        return timeseriesRow;
    }

    public void setTimeseriesRow(TimeseriesRow timeseriesRow) {
        this.timeseriesRow = timeseriesRow;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
