package com.alicloud.openservices.tablestore.timeserieswriter.group;

import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesTableRow;

public class TimeseriesRowWithGroup {
    public final TimeseriesTableRow timeseriesTableRow;
    public final TimeseriesGroup timeseriesGroup;

    public TimeseriesRowWithGroup(TimeseriesTableRow timeseriesTableRow, TimeseriesGroup timeseriesGroup) {
        this.timeseriesTableRow = timeseriesTableRow;
        this.timeseriesGroup = timeseriesGroup;
    }
}
