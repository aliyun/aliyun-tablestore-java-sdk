package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.OperationNames;

public class RedactItemVersionRequest extends ItemRequest {
    private String itemId;
    private String versionId;
    private Long versionSeq;
    private String sessionId;

    public RedactItemVersionRequest() {
    }

    public RedactItemVersionRequest(String memoryStoreName, Scope scope, String itemId, String versionId, Long versionSeq) {
        super(memoryStoreName, scope);
        this.itemId = itemId;
        this.versionId = versionId;
        this.versionSeq = versionSeq;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_REDACT_ITEM_VERSION;
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public String getVersionId() {
        return versionId;
    }

    public void setVersionId(String versionId) {
        this.versionId = versionId;
    }

    public Long getVersionSeq() {
        return versionSeq;
    }

    public void setVersionSeq(Long versionSeq) {
        this.versionSeq = versionSeq;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }
}
