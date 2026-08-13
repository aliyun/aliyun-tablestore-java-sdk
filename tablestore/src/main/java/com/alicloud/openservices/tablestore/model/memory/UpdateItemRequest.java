package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.OperationNames;

public class UpdateItemRequest extends ItemRequest {
    private String path;
    private String content;
    private String newPath;
    private Boolean overwrite;
    private String expectedSha256;
    private String sessionId;

    public UpdateItemRequest() {
    }

    public UpdateItemRequest(String memoryStoreName, Scope scope, String path) {
        super(memoryStoreName, scope);
        this.path = path;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_UPDATE_ITEM;
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

    public String getNewPath() {
        return newPath;
    }

    public void setNewPath(String newPath) {
        this.newPath = newPath;
    }

    public Boolean getOverwrite() {
        return overwrite;
    }

    public void setOverwrite(Boolean overwrite) {
        this.overwrite = overwrite;
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
