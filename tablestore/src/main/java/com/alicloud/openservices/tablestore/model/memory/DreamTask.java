package com.alicloud.openservices.tablestore.model.memory;

import java.util.List;
import java.util.Map;

public class DreamTask {
    private String dreamId;
    private String memoryStoreName;
    private String status;
    private String taskType;
    private String applyMode;
    private String scopeOutputMode;
    private String clientToken;
    private String triggerType;
    private Integer expandedScopeLimit;
    private List<Scope> inputScopes;
    private List<Scope> expandedScopes;
    private List<Scope> targetScopes;
    private Long minTimestamp;
    private Long maxTimestamp;
    private Boolean incremental;
    private Long processedThroughMs;
    private Integer maxSessions;
    private Integer maxMessages;
    private Integer maxMemories;
    private Map<String, Double> confidenceThresholds;
    private String instructions;
    private Integer sessionCount;
    private Integer messageCount;
    private Integer memoryCount;
    private Integer actionCount;
    private Integer proposedCount;
    private Integer appliedCount;
    private Integer skippedCount;
    private Map<String, Integer> skippedByReason;
    private Integer failedCount;
    private String lastError;
    private Integer version;
    private String createdAt;
    private String updatedAt;
    private String finishedAt;

    public DreamTask() {
    }

    public String getDreamId() {
        return dreamId;
    }

    public void setDreamId(String dreamId) {
        this.dreamId = dreamId;
    }

    public String getMemoryStoreName() {
        return memoryStoreName;
    }

    public void setMemoryStoreName(String memoryStoreName) {
        this.memoryStoreName = memoryStoreName;
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

    public String getClientToken() {
        return clientToken;
    }

    public void setClientToken(String clientToken) {
        this.clientToken = clientToken;
    }

    public String getTriggerType() {
        return triggerType;
    }

    public void setTriggerType(String triggerType) {
        this.triggerType = triggerType;
    }

    public Integer getExpandedScopeLimit() {
        return expandedScopeLimit;
    }

    public void setExpandedScopeLimit(Integer expandedScopeLimit) {
        this.expandedScopeLimit = expandedScopeLimit;
    }

    public List<Scope> getInputScopes() {
        return inputScopes;
    }

    public void setInputScopes(List<Scope> inputScopes) {
        this.inputScopes = inputScopes;
    }

    public List<Scope> getExpandedScopes() {
        return expandedScopes;
    }

    public void setExpandedScopes(List<Scope> expandedScopes) {
        this.expandedScopes = expandedScopes;
    }

    public List<Scope> getTargetScopes() {
        return targetScopes;
    }

    public void setTargetScopes(List<Scope> targetScopes) {
        this.targetScopes = targetScopes;
    }

    public Long getMinTimestamp() {
        return minTimestamp;
    }

    public void setMinTimestamp(Long minTimestamp) {
        this.minTimestamp = minTimestamp;
    }

    public Long getMaxTimestamp() {
        return maxTimestamp;
    }

    public void setMaxTimestamp(Long maxTimestamp) {
        this.maxTimestamp = maxTimestamp;
    }

    public Boolean getIncremental() {
        return incremental;
    }

    public void setIncremental(Boolean incremental) {
        this.incremental = incremental;
    }

    public Long getProcessedThroughMs() {
        return processedThroughMs;
    }

    public void setProcessedThroughMs(Long processedThroughMs) {
        this.processedThroughMs = processedThroughMs;
    }

    public Integer getMaxSessions() {
        return maxSessions;
    }

    public void setMaxSessions(Integer maxSessions) {
        this.maxSessions = maxSessions;
    }

    public Integer getMaxMessages() {
        return maxMessages;
    }

    public void setMaxMessages(Integer maxMessages) {
        this.maxMessages = maxMessages;
    }

    public Integer getMaxMemories() {
        return maxMemories;
    }

    public void setMaxMemories(Integer maxMemories) {
        this.maxMemories = maxMemories;
    }

    public Map<String, Double> getConfidenceThresholds() {
        return confidenceThresholds;
    }

    public void setConfidenceThresholds(Map<String, Double> confidenceThresholds) {
        this.confidenceThresholds = confidenceThresholds;
    }

    public String getInstructions() {
        return instructions;
    }

    public void setInstructions(String instructions) {
        this.instructions = instructions;
    }

    public Integer getSessionCount() {
        return sessionCount;
    }

    public void setSessionCount(Integer sessionCount) {
        this.sessionCount = sessionCount;
    }

    public Integer getMessageCount() {
        return messageCount;
    }

    public void setMessageCount(Integer messageCount) {
        this.messageCount = messageCount;
    }

    public Integer getMemoryCount() {
        return memoryCount;
    }

    public void setMemoryCount(Integer memoryCount) {
        this.memoryCount = memoryCount;
    }

    public Integer getActionCount() {
        return actionCount;
    }

    public void setActionCount(Integer actionCount) {
        this.actionCount = actionCount;
    }

    public Integer getProposedCount() {
        return proposedCount;
    }

    public void setProposedCount(Integer proposedCount) {
        this.proposedCount = proposedCount;
    }

    public Integer getAppliedCount() {
        return appliedCount;
    }

    public void setAppliedCount(Integer appliedCount) {
        this.appliedCount = appliedCount;
    }

    public Integer getSkippedCount() {
        return skippedCount;
    }

    public void setSkippedCount(Integer skippedCount) {
        this.skippedCount = skippedCount;
    }

    public Map<String, Integer> getSkippedByReason() {
        return skippedByReason;
    }

    public void setSkippedByReason(Map<String, Integer> skippedByReason) {
        this.skippedByReason = skippedByReason;
    }

    public Integer getFailedCount() {
        return failedCount;
    }

    public void setFailedCount(Integer failedCount) {
        this.failedCount = failedCount;
    }

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError;
    }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
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
