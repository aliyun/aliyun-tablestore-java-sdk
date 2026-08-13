package com.alicloud.openservices.tablestore.model.memory;

import java.util.List;

public class MemoryTask {
    private String requestId;
    private String eventType;
    private String memoryStoreName;
    private String conversationKey;
    private Scope scope;
    private String status;
    private Integer acceptedMessages;
    private String derivedMemcellId;
    private List<String> derivedUnitIds;
    private String lastError;
    private String createdAt;
    private String updatedAt;
    private String finishedAt;

    public MemoryTask() {
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getMemoryStoreName() {
        return memoryStoreName;
    }

    public void setMemoryStoreName(String memoryStoreName) {
        this.memoryStoreName = memoryStoreName;
    }

    public String getConversationKey() {
        return conversationKey;
    }

    public void setConversationKey(String conversationKey) {
        this.conversationKey = conversationKey;
    }

    public Scope getScope() {
        return scope;
    }

    public void setScope(Scope scope) {
        this.scope = scope;
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

    public String getDerivedMemcellId() {
        return derivedMemcellId;
    }

    public void setDerivedMemcellId(String derivedMemcellId) {
        this.derivedMemcellId = derivedMemcellId;
    }

    public List<String> getDerivedUnitIds() {
        return derivedUnitIds;
    }

    public void setDerivedUnitIds(List<String> derivedUnitIds) {
        this.derivedUnitIds = derivedUnitIds;
    }

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError;
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

    public String getFinishedAt() {
        return finishedAt;
    }

    public void setFinishedAt(String finishedAt) {
        this.finishedAt = finishedAt;
    }
}
