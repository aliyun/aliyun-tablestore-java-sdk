package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.OperationNames;

public class DeleteItemRequest extends ItemRequest {
    private String path;
    private String expectedSha256;
    private String sessionId;

    public DeleteItemRequest() {
    }

    public DeleteItemRequest(String memoryStoreName, Scope scope, String path) {
        super(memoryStoreName, scope);
        this.path = path;
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

    public String getExpectedSha256() {
        return expectedSha256;
    }

    public void setExpectedSha256(String expectedSha256) {
        this.expectedSha256 = expectedSha256;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }
}
