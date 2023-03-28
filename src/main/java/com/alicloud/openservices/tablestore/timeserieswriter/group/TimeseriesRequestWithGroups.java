package com.alicloud.openservices.tablestore.timeserieswriter.group;

import com.alicloud.openservices.tablestore.model.Request;

import java.util.List;

public class TimeseriesRequestWithGroups {
    private final Request request;
    private final List<TimeseriesGroup> timeseriesGroupList;

    public TimeseriesRequestWithGroups(Request request, List<TimeseriesGroup> timeseriesGroupList) {
        this.request = request;
        this.timeseriesGroupList = timeseriesGroupList;
    }

    public Request getRequest() {
        return request;
    }

    public List<TimeseriesGroup> getGroupList() {
        return timeseriesGroupList;
    }
}
