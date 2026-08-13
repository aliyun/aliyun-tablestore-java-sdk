package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.OperationNames;

public class GetMemoryDreamTaskRequest extends AbstractMemoryRequest {
    private String memoryStoreName;
    private String dreamId;

    public GetMemoryDreamTaskRequest() {
    }

    public GetMemoryDreamTaskRequest(String memoryStoreName, String dreamId) {
        this.memoryStoreName = memoryStoreName;
        this.dreamId = dreamId;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_GET_MEMORY_DREAM_TASK;
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
}
