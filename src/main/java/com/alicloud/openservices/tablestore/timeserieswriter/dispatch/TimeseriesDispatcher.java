package com.alicloud.openservices.tablestore.timeserieswriter.dispatch;

import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesRow;



public interface TimeseriesDispatcher {
    /**
     * 获取写入分桶编号
     */
    int getDispatchIndex(TimeseriesRow timeseriesRow);
}
