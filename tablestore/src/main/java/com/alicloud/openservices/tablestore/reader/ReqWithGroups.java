package com.alicloud.openservices.tablestore.reader;

import java.util.List;
import java.util.Map;

import com.alicloud.openservices.tablestore.model.BatchGetRowRequest;

public class ReqWithGroups {
    private final BatchGetRowRequest request;
    private final Map<String, List<ReaderGroup>> groupMap;

    public ReqWithGroups(BatchGetRowRequest request, Map<String, List<ReaderGroup>> groupList) {
        this.request = request;
        this.groupMap = groupList;
    }

    public BatchGetRowRequest getRequest() {
        return request;
    }

    public Map<String, List<ReaderGroup>> getGroupMap() {
        return groupMap;
    }
}
