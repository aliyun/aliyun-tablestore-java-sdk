package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.Response;

public class GetMemoryDreamTaskResponse extends Response {
    private String memoryStoreName;
    private String dreamId;
    private String status;
    private String taskType;
    private String applyMode;
    private String scopeOutputMode;
    private DreamTaskInputSummary input;
    private Long processedThroughMs;
    private DreamTaskActionStats actions;
    private String instructions;
    private String lastError;
    private String createdAt;
    private String updatedAt;
    private String finishedAt;

    public GetMemoryDreamTaskResponse() {
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

    public String getTaskType() {
        return taskType;
    }

    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }

    public String getApplyMode() {
        return applyMode;
    }

    public void setApplyMode(String applyMode) {
        this.applyMode = applyMode;
    }

    public String getScopeOutputMode() {
        return scopeOutputMode;
    }

    public void setScopeOutputMode(String scopeOutputMode) {
        this.scopeOutputMode = scopeOutputMode;
    }

    public DreamTaskInputSummary getInput() {
        return input;
    }

    public void setInput(DreamTaskInputSummary input) {
        this.input = input;
    }

    public Long getProcessedThroughMs() {
        return processedThroughMs;
    }

    public void setProcessedThroughMs(Long processedThroughMs) {
        this.processedThroughMs = processedThroughMs;
    }

    public DreamTaskActionStats getActions() {
        return actions;
    }

    public void setActions(DreamTaskActionStats actions) {
        this.actions = actions;
    }

    public String getInstructions() {
        return instructions;
    }

    public void setInstructions(String instructions) {
        this.instructions = instructions;
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
