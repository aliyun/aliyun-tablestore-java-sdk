package com.alicloud.openservices.tablestore.model.memory;

import java.util.List;

public class DreamActionRecord {
    private String dreamId;
    private String actionId;
    private String memoryStoreName;
    private String action;
    private String status;
    private Scope targetScope;
    private List<Scope> sourceScopes;
    private String targetMemoryId;
    private Integer targetMemoryVersion;
    private Long targetMemoryUpdatedAtMs;
    private DreamMemorySpec newMemory;
    private DreamExtractSpec newExtract;
    private List<String> sourceMessageIds;
    private List<String> sourceMemoryIds;
    private String reason;
    private Double confidence;
    private String resultMemoryId;
    private String applyStep;
    private String applyDetailJson;
    private String skippedReason;
    private String skippedReasonDetail;
    private String appliedAt;
    private String applier;
    private Integer attemptCount;
    private String error;
    private String createdAt;
    private String updatedAt;

    public DreamActionRecord() {
    }

    public String getDreamId() {
        return dreamId;
    }

    public void setDreamId(String dreamId) {
        this.dreamId = dreamId;
    }

    public String getActionId() {
        return actionId;
    }

    public void setActionId(String actionId) {
        this.actionId = actionId;
    }

    public String getMemoryStoreName() {
        return memoryStoreName;
    }

    public void setMemoryStoreName(String memoryStoreName) {
        this.memoryStoreName = memoryStoreName;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Scope getTargetScope() {
        return targetScope;
    }

    public void setTargetScope(Scope targetScope) {
        this.targetScope = targetScope;
    }

    public List<Scope> getSourceScopes() {
        return sourceScopes;
    }

    public void setSourceScopes(List<Scope> sourceScopes) {
        this.sourceScopes = sourceScopes;
    }

    public String getTargetMemoryId() {
        return targetMemoryId;
    }

    public void setTargetMemoryId(String targetMemoryId) {
        this.targetMemoryId = targetMemoryId;
    }

    public Integer getTargetMemoryVersion() {
        return targetMemoryVersion;
    }

    public void setTargetMemoryVersion(Integer targetMemoryVersion) {
        this.targetMemoryVersion = targetMemoryVersion;
    }

    public Long getTargetMemoryUpdatedAtMs() {
        return targetMemoryUpdatedAtMs;
    }

    public void setTargetMemoryUpdatedAtMs(Long targetMemoryUpdatedAtMs) {
        this.targetMemoryUpdatedAtMs = targetMemoryUpdatedAtMs;
    }

    public DreamMemorySpec getNewMemory() {
        return newMemory;
    }

    public void setNewMemory(DreamMemorySpec newMemory) {
        this.newMemory = newMemory;
    }

    public DreamExtractSpec getNewExtract() {
        return newExtract;
    }

    public void setNewExtract(DreamExtractSpec newExtract) {
        this.newExtract = newExtract;
    }

    public List<String> getSourceMessageIds() {
        return sourceMessageIds;
    }

    public void setSourceMessageIds(List<String> sourceMessageIds) {
        this.sourceMessageIds = sourceMessageIds;
    }

    public List<String> getSourceMemoryIds() {
        return sourceMemoryIds;
    }

    public void setSourceMemoryIds(List<String> sourceMemoryIds) {
        this.sourceMemoryIds = sourceMemoryIds;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public Double getConfidence() {
        return confidence;
    }

    public void setConfidence(Double confidence) {
        this.confidence = confidence;
    }

    public String getResultMemoryId() {
        return resultMemoryId;
    }

    public void setResultMemoryId(String resultMemoryId) {
        this.resultMemoryId = resultMemoryId;
    }

    public String getApplyStep() {
        return applyStep;
    }

    public void setApplyStep(String applyStep) {
        this.applyStep = applyStep;
    }

    public String getApplyDetailJson() {
        return applyDetailJson;
    }

    public void setApplyDetailJson(String applyDetailJson) {
        this.applyDetailJson = applyDetailJson;
    }

    public String getSkippedReason() {
        return skippedReason;
    }

    public void setSkippedReason(String skippedReason) {
        this.skippedReason = skippedReason;
    }

    public String getSkippedReasonDetail() {
        return skippedReasonDetail;
    }

    public void setSkippedReasonDetail(String skippedReasonDetail) {
        this.skippedReasonDetail = skippedReasonDetail;
    }

    public String getAppliedAt() {
        return appliedAt;
    }

    public void setAppliedAt(String appliedAt) {
        this.appliedAt = appliedAt;
    }

    public String getApplier() {
        return applier;
    }

    public void setApplier(String applier) {
        this.applier = applier;
    }

    public Integer getAttemptCount() {
        return attemptCount;
    }

    public void setAttemptCount(Integer attemptCount) {
        this.attemptCount = attemptCount;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
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
}
