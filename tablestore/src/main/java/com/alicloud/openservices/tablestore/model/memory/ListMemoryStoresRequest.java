package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.OperationNames;

public class ListMemoryStoresRequest extends AbstractMemoryRequest {
    private Integer limit;
    private String nextToken;

    public ListMemoryStoresRequest() {
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_LIST_MEMORY_STORES;
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
