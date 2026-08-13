package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.OperationNames;

public class UpdateMemoryStoreRequest extends AbstractMemoryRequest {
    private String memoryStoreName;
    private String description;
    private String extractInstructions;

    public UpdateMemoryStoreRequest() {
    }

    public UpdateMemoryStoreRequest(String memoryStoreName) {
        this.memoryStoreName = memoryStoreName;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_UPDATE_MEMORY_STORE;
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
}
