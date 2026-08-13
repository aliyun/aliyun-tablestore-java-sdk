package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.OperationNames;

public class ListItemVersionsRequest extends ItemRequest {
    private String itemId;
    private String path;
    private String nextToken;
    private Integer limit;
    private String operation;

    public ListItemVersionsRequest() {
    }

    public ListItemVersionsRequest(String memoryStoreName, Scope scope, String itemId) {
        super(memoryStoreName, scope);
        this.itemId = itemId;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_LIST_ITEM_VERSIONS;
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
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

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }
}
