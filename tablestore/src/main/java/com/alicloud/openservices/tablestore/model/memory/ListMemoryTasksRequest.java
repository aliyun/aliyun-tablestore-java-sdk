package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.OperationNames;

public class ListMemoryTasksRequest extends AbstractMemoryRequest {
    private String memoryStoreName;
    private Scope scope;
    private String status;
    private Integer limit;
    private String nextToken;
    private Object minTimestamp;
    private Object maxTimestamp;

    public ListMemoryTasksRequest() {
    }

    public ListMemoryTasksRequest(String memoryStoreName, Scope scope) {
        this.memoryStoreName = memoryStoreName;
        this.scope = scope;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_LIST_MEMORY_TASKS;
    }

    public String getMemoryStoreName() {
        return memoryStoreName;
    }

    public void setMemoryStoreName(String memoryStoreName) {
        this.memoryStoreName = memoryStoreName;
    }

    public Scope getScope() {
        return scope;
    }

    public void setScope(Scope scope) {
        this.scope = scope;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    public String getNextToken() {
        return nextToken;
    }

    public void setNextToken(String nextToken) {
        this.nextToken = nextToken;
    }

    public Object getMinTimestamp() {
        return minTimestamp;
    }

    public void setMinTimestamp(long minTimestamp) {
        this.minTimestamp = minTimestamp;
    }

    public void setMinTimestamp(String minTimestamp) {
        this.minTimestamp = minTimestamp;
    }

    public Object getMaxTimestamp() {
        return maxTimestamp;
    }

    public void setMaxTimestamp(long maxTimestamp) {
        this.maxTimestamp = maxTimestamp;
    }

    public void setMaxTimestamp(String maxTimestamp) {
        this.maxTimestamp = maxTimestamp;
    }
}
