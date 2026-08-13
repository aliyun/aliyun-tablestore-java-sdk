package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.OperationNames;

public class ListItemsRequest extends ItemRequest {
    private String pathPrefix;
    private String nextToken;
    private Integer limit;

    public ListItemsRequest() {
    }

    public ListItemsRequest(String memoryStoreName, Scope scope) {
        super(memoryStoreName, scope);
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_LIST_ITEMS;
    }

    public String getPathPrefix() {
        return pathPrefix;
    }

    public void setPathPrefix(String pathPrefix) {
        this.pathPrefix = pathPrefix;
    }

    public String getNextToken() {
        return nextToken;
    }

    public void setNextToken(String nextToken) {
        this.nextToken = nextToken;
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }
}
