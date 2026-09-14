package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.OperationNames;

public class DeleteItemRequest extends ItemRequest {
    private String itemId;
    private String path;
    private String pathPrefix;
    private ItemPrecondition precondition;
    private String sessionId;

    public DeleteItemRequest() {
    }

    public DeleteItemRequest(String memoryStoreName, Scope scope) {
        super(memoryStoreName, scope);
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_DELETE_ITEM;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getPathPrefix() {
        return pathPrefix;
    }

    public void setPathPrefix(String pathPrefix) {
        this.pathPrefix = pathPrefix;
    }

    public ItemPrecondition getPrecondition() {
        return precondition;
    }

    public void setPrecondition(ItemPrecondition precondition) {
        this.precondition = precondition;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }
}
