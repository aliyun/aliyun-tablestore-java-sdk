package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.OperationNames;

public class DeleteMemoryRequest extends AbstractMemoryRequest {
    private String memoryStoreName;
    private String memoryId;
    private Scope scope;

    public DeleteMemoryRequest() {
    }

    public DeleteMemoryRequest(String memoryStoreName, String memoryId, Scope scope) {
        this.memoryStoreName = memoryStoreName;
        this.memoryId = memoryId;
        this.scope = scope;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_DELETE_MEMORY;
    }

    public String getMemoryStoreName() {
        return memoryStoreName;
    }

    public void setMemoryStoreName(String memoryStoreName) {
        this.memoryStoreName = memoryStoreName;
    }

    public String getMemoryId() {
        return memoryId;
    }

    public void setMemoryId(String memoryId) {
        this.memoryId = memoryId;
    }

    public Scope getScope() {
        return scope;
    }

    public void setScope(Scope scope) {
        this.scope = scope;
    }
}
