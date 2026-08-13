package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.Response;

public class GetMemoryResponse extends Response {
    private MemoryUnit memory;
    private String memoryStoreName;

    public GetMemoryResponse() {
    }

    public MemoryUnit getMemory() {
        return memory;
    }

    public void setMemory(MemoryUnit memory) {
        this.memory = memory;
    }

    public String getMemoryStoreName() {
        return memoryStoreName;
    }

    public void setMemoryStoreName(String memoryStoreName) {
        this.memoryStoreName = memoryStoreName;
    }
}
