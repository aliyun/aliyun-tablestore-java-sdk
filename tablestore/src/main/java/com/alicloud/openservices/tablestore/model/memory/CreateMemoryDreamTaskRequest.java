package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.OperationNames;
import java.util.List;
import java.util.Map;

public class CreateMemoryDreamTaskRequest extends AbstractMemoryRequest {
    private String memoryStoreName;
    private List<Scope> scopes;
    private Object minTimestamp;
    private Object maxTimestamp;
    private Integer maxSessions;
    private Integer maxMessages;
    private Integer maxMemories;
    private Integer expandedScopeLimit;
    private String scopeOutputMode;
    private String applyMode;
    private String taskType;
    private Map<String, Double> confidenceThresholds;
    private String instructions;
    private String clientToken;
    private Boolean incremental;

    public CreateMemoryDreamTaskRequest() {
    }

    public CreateMemoryDreamTaskRequest(String memoryStoreName, List<Scope> scopes) {
        this.memoryStoreName = memoryStoreName;
        this.scopes = scopes;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_CREATE_MEMORY_DREAM_TASK;
    }

    public String getMemoryStoreName() {
        return memoryStoreName;
    }

    public void setMemoryStoreName(String memoryStoreName) {
        this.memoryStoreName = memoryStoreName;
    }

    public List<Scope> getScopes() {
        return scopes;
    }

    public void setScopes(List<Scope> scopes) {
        this.scopes = scopes;
    }

    public Object getMinTimestamp() {
        return minTimestamp;
    }

    public void setMinTimestamp(long minTimestamp) {
        this.minTimestamp = minTimestamp;
    }

    public void setMinTimestamp(String minTimestamp) {
        this.minTimestamp = minTimestamp;
    }

    public Object getMaxTimestamp() {
        return maxTimestamp;
    }

    public void setMaxTimestamp(long maxTimestamp) {
        this.maxTimestamp = maxTimestamp;
    }

    public void setMaxTimestamp(String maxTimestamp) {
        this.maxTimestamp = maxTimestamp;
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

    public Integer getExpandedScopeLimit() {
        return expandedScopeLimit;
    }

    public void setExpandedScopeLimit(Integer expandedScopeLimit) {
        this.expandedScopeLimit = expandedScopeLimit;
    }

    public String getScopeOutputMode() {
        return scopeOutputMode;
    }

    public void setScopeOutputMode(String scopeOutputMode) {
        this.scopeOutputMode = scopeOutputMode;
    }

    public String getApplyMode() {
        return applyMode;
    }

    public void setApplyMode(String applyMode) {
        this.applyMode = applyMode;
    }

    public String getTaskType() {
        return taskType;
    }

    public void setTaskType(String taskType) {
        this.taskType = taskType;
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

    public String getClientToken() {
        return clientToken;
    }

    public void setClientToken(String clientToken) {
        this.clientToken = clientToken;
    }

    public Boolean getIncremental() {
        return incremental;
    }

    public void setIncremental(Boolean incremental) {
        this.incremental = incremental;
    }
}
