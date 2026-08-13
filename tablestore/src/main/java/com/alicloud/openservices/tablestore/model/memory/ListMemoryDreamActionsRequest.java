package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.OperationNames;

public class ListMemoryDreamActionsRequest extends AbstractMemoryRequest {
    private String memoryStoreName;
    private String dreamId;
    private Scope scope;
    private String actionType;
    private String status;
    private String action;
    private Scope targetScope;
    private String sourceMemoryId;
    private Double minConfidence;
    private Double maxConfidence;
    private String orderBy;
    private Integer limit;
    private String nextToken;

    public ListMemoryDreamActionsRequest() {
    }

    public ListMemoryDreamActionsRequest(String memoryStoreName) {
        this.memoryStoreName = memoryStoreName;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_LIST_MEMORY_DREAM_ACTIONS;
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

    public Scope getScope() {
        return scope;
    }

    public void setScope(Scope scope) {
        this.scope = scope;
    }

    public String getActionType() {
        return actionType;
    }

    public void setActionType(String actionType) {
        this.actionType = actionType;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public Scope getTargetScope() {
        return targetScope;
    }

    public void setTargetScope(Scope targetScope) {
        this.targetScope = targetScope;
    }

    public String getSourceMemoryId() {
        return sourceMemoryId;
    }

    public void setSourceMemoryId(String sourceMemoryId) {
        this.sourceMemoryId = sourceMemoryId;
    }

    public Double getMinConfidence() {
        return minConfidence;
    }

    public void setMinConfidence(Double minConfidence) {
        this.minConfidence = minConfidence;
    }

    public Double getMaxConfidence() {
        return maxConfidence;
    }

    public void setMaxConfidence(Double maxConfidence) {
        this.maxConfidence = maxConfidence;
    }

    public String getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(String orderBy) {
        this.orderBy = orderBy;
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    public String getNextToken() {
        return nextToken;
    }

    public void setNextToken(String nextToken) {
        this.nextToken = nextToken;
    }
}
