package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.OperationNames;

public class GetMemoryTaskRequest extends AbstractMemoryRequest {
    private String memoryStoreName;
    private String requestId;
    private Scope scope;

    public GetMemoryTaskRequest() {
    }

    public GetMemoryTaskRequest(String memoryStoreName, String requestId) {
        this.memoryStoreName = memoryStoreName;
        this.requestId = requestId;
    }

    public GetMemoryTaskRequest(String memoryStoreName, String requestId, Scope scope) {
        this.memoryStoreName = memoryStoreName;
        this.requestId = requestId;
        this.scope = scope;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_GET_MEMORY_TASK;
    }

    public String getMemoryStoreName() {
        return memoryStoreName;
    }

    public void setMemoryStoreName(String memoryStoreName) {
        this.memoryStoreName = memoryStoreName;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public Scope getScope() {
        return scope;
    }

    public void setScope(Scope scope) {
        this.scope = scope;
    }
}
