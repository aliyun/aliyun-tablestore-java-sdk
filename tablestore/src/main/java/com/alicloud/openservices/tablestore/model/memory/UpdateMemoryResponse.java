package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.Response;

public class UpdateMemoryResponse extends Response {
    private MemoryUnit memory;
    private Boolean updated;
    private String memoryStoreName;

    public UpdateMemoryResponse() {
    }

    public MemoryUnit getMemory() {
        return memory;
    }

    public void setMemory(MemoryUnit memory) {
        this.memory = memory;
    }

    public Boolean getUpdated() {
        return updated;
    }

    public void setUpdated(Boolean updated) {
        this.updated = updated;
    }

    public String getMemoryStoreName() {
        return memoryStoreName;
    }

    public void setMemoryStoreName(String memoryStoreName) {
        this.memoryStoreName = memoryStoreName;
    }
}
