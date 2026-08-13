package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.OperationNames;

public class GetItemRequest extends ItemRequest {
    private String path;
    private Boolean includeContent;

    public GetItemRequest() {
    }

    public GetItemRequest(String memoryStoreName, Scope scope, String path) {
        super(memoryStoreName, scope);
        this.path = path;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_GET_ITEM;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Boolean getIncludeContent() {
        return includeContent;
    }

    public void setIncludeContent(Boolean includeContent) {
        this.includeContent = includeContent;
    }
}
