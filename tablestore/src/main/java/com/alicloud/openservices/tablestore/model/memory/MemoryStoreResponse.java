package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.Response;

public class MemoryStoreResponse extends Response {
    private String otsInstance;
    private String memoryStoreName;
    private String description;
    private String extractInstructions;
    private String storageMode;
    private String createdAt;
    private String updatedAt;
    private String lastSeenAt;

    public MemoryStoreResponse() {
    }

    public String getOtsInstance() {
        return otsInstance;
    }

    public void setOtsInstance(String otsInstance) {
        this.otsInstance = otsInstance;
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

    public String getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    public String getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(String updatedAt) {
        this.updatedAt = updatedAt;
    }

    public String getLastSeenAt() {
        return lastSeenAt;
    }

    public void setLastSeenAt(String lastSeenAt) {
        this.lastSeenAt = lastSeenAt;
    }
}
