package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.Response;

public class CreateMemoryDreamTaskResponse extends Response {
    private String memoryStoreName;
    private String dreamId;
    private String status;
    private String createdAt;

    public CreateMemoryDreamTaskResponse() {
    }

    public String getMemoryStoreName() {
        return memoryStoreName;
    }

    public void setMemoryStoreName(String memoryStoreName) {
        this.memoryStoreName = memoryStoreName;
    }

    public String getDreamId() {
        return dreamId;
    }

    public void setDreamId(String dreamId) {
        this.dreamId = dreamId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }
}
