package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.Response;

public class GetMemoryTaskResponse extends Response {
    private MemoryTask task;
    private String memoryStoreName;

    public GetMemoryTaskResponse() {
    }

    public MemoryTask getTask() {
        return task;
    }

    public void setTask(MemoryTask task) {
        this.task = task;
    }

    public String getMemoryStoreName() {
        return memoryStoreName;
    }

    public void setMemoryStoreName(String memoryStoreName) {
        this.memoryStoreName = memoryStoreName;
    }
}
