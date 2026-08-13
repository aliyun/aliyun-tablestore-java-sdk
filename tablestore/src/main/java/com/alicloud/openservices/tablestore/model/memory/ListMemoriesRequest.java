package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.OperationNames;

public class ListMemoriesRequest extends AbstractMemoryRequest {
    private String memoryStoreName;
    private Scope scope;
    private Integer limit;
    private String nextToken;

    public ListMemoriesRequest() {
    }

    public ListMemoriesRequest(String memoryStoreName) {
        this.memoryStoreName = memoryStoreName;
    }

    public ListMemoriesRequest(String memoryStoreName, Scope scope) {
        this.memoryStoreName = memoryStoreName;
        this.scope = scope;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_LIST_MEMORIES;
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
}
