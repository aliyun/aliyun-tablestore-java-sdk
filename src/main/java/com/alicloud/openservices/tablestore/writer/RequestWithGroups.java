package com.alicloud.openservices.tablestore.writer;

import com.alicloud.openservices.tablestore.model.Request;

import java.util.List;

public class RequestWithGroups {
    private final Request request;
    private final List<Group> groupList;

    public RequestWithGroups(Request request, List<Group> groupList) {
        this.request = request;
        this.groupList = groupList;
    }

    public Request getRequest() {
        return request;
    }

    public List<Group> getGroupList() {
        return groupList;
    }
}
