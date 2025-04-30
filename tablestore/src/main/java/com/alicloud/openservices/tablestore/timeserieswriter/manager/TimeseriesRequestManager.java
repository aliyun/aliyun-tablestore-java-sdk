package com.alicloud.openservices.tablestore.timeserieswriter.manager;

import com.alicloud.openservices.tablestore.timeserieswriter.group.TimeseriesRequestWithGroups;
import com.alicloud.openservices.tablestore.timeserieswriter.group.TimeseriesRowWithGroup;

public interface TimeseriesRequestManager {
    boolean appendTimeseriesRow(TimeseriesRowWithGroup timeseriesRowWithGroup);

    TimeseriesRequestWithGroups makeRequest(String tablename);

    void sendRequest(TimeseriesRequestWithGroups timeseriesRequestWithGroups);

    int getTotalRowsCount();

    void clear();
}
