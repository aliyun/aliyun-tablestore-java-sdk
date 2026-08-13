package com.alicloud.openservices.tablestore.model.memory;

public class Scope {
    private String appId;
    private String tenantId;
    private String agentId;
    private String runId;

    public Scope() {
    }

    public Scope(String appId, String tenantId, String agentId, String runId) {
        this.appId = appId;
        this.tenantId = tenantId;
        this.agentId = agentId;
        this.runId = runId;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getAgentId() {
        return agentId;
    }

    public void setAgentId(String agentId) {
        this.agentId = agentId;
    }

    public String getRunId() {
        return runId;
    }

    public void setRunId(String runId) {
        this.runId = runId;
    }
}
