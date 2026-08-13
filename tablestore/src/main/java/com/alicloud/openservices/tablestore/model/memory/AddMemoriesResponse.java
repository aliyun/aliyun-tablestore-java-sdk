package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.Response;

public class AddMemoriesResponse extends Response {
    private transient String memoryRequestId;
    private String status;
    private Integer acceptedMessages;
    private Scope scope;
    private String memoryStoreName;
    private Integer memcellsCreated;
    private Integer unitsCreated;

    public AddMemoriesResponse() {
    }

    public String getMemoryRequestId() {
        return memoryRequestId;
    }

    public void setMemoryRequestId(String memoryRequestId) {
        this.memoryRequestId = memoryRequestId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Integer getAcceptedMessages() {
        return acceptedMessages;
    }

    public void setAcceptedMessages(Integer acceptedMessages) {
        this.acceptedMessages = acceptedMessages;
    }

    public Scope getScope() {
        return scope;
    }

    public void setScope(Scope scope) {
        this.scope = scope;
    }

    public String getMemoryStoreName() {
        return memoryStoreName;
    }

    public void setMemoryStoreName(String memoryStoreName) {
        this.memoryStoreName = memoryStoreName;
    }

    public Integer getMemcellsCreated() {
        return memcellsCreated;
    }

    public void setMemcellsCreated(Integer memcellsCreated) {
        this.memcellsCreated = memcellsCreated;
    }

    public Integer getUnitsCreated() {
        return unitsCreated;
    }

    public void setUnitsCreated(Integer unitsCreated) {
        this.unitsCreated = unitsCreated;
    }
}
