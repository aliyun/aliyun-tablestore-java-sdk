package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.OperationNames;

public class DeleteMemoryStoreRequest extends AbstractMemoryRequest {
    private String memoryStoreName;

    public DeleteMemoryStoreRequest() {
    }

    public DeleteMemoryStoreRequest(String memoryStoreName) {
        this.memoryStoreName = memoryStoreName;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_DELETE_MEMORY_STORE;
    }

    public String getMemoryStoreName() {
        return memoryStoreName;
    }

    public void setMemoryStoreName(String memoryStoreName) {
        this.memoryStoreName = memoryStoreName;
    }
}
