package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.OperationNames;

public class CreateMemoryStoreRequest extends AbstractMemoryRequest {
    private String memoryStoreName;
    private String description;
    private String extractInstructions;
    private String storageMode;

    public CreateMemoryStoreRequest() {
    }

    public CreateMemoryStoreRequest(String memoryStoreName) {
        this.memoryStoreName = memoryStoreName;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_CREATE_MEMORY_STORE;
    }

    public String getMemoryStoreName() {
        return memoryStoreName;
    }

    public void setMemoryStoreName(String memoryStoreName) {
        this.memoryStoreName = memoryStoreName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getExtractInstructions() {
        return extractInstructions;
    }

    public void setExtractInstructions(String extractInstructions) {
        this.extractInstructions = extractInstructions;
    }

    public String getStorageMode() {
        return storageMode;
    }

    public void setStorageMode(String storageMode) {
        this.storageMode = storageMode;
    }
}
