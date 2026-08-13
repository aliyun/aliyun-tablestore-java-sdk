package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.OperationNames;

public class AddItemRequest extends ItemRequest {
    private String path;
    private String content;
    private String sessionId;

    public AddItemRequest() {
    }

    public AddItemRequest(String memoryStoreName, Scope scope, String path, String content) {
        super(memoryStoreName, scope);
        this.path = path;
        this.content = content;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_ADD_ITEM;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }
}
