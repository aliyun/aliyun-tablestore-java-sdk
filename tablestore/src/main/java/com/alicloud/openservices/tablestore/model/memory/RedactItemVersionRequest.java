package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.OperationNames;

public class RedactItemVersionRequest extends ItemRequest {
    private String versionId;
    private String sessionId;

    public RedactItemVersionRequest() {
    }

    public RedactItemVersionRequest(String memoryStoreName, Scope scope, String versionId) {
        super(memoryStoreName, scope);
        this.versionId = versionId;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_REDACT_ITEM_VERSION;
    }

    public String getVersionId() {
        return versionId;
    }

    public void setVersionId(String versionId) {
        this.versionId = versionId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }
}
