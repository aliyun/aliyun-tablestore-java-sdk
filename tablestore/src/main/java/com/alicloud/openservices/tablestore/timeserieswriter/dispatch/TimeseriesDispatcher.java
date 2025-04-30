package com.alicloud.openservices.tablestore.timeserieswriter.dispatch;

import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesRow;



public interface TimeseriesDispatcher {
    /**
     * Get the write partition bucket number
     */
    int getDispatchIndex(TimeseriesRow timeseriesRow);
}
