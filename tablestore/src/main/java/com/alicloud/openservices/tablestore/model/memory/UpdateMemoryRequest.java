package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.OperationNames;
import java.util.Map;

public class UpdateMemoryRequest extends AbstractMemoryRequest {
    private String memoryStoreName;
    private String memoryId;
    private Scope scope;
    private String text;
    private Map<String, String> metadata;

    public UpdateMemoryRequest() {
    }

    public UpdateMemoryRequest(String memoryStoreName, String memoryId, Scope scope) {
        this.memoryStoreName = memoryStoreName;
        this.memoryId = memoryId;
        this.scope = scope;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_UPDATE_MEMORY;
    }

    public String getMemoryStoreName() {
        return memoryStoreName;
    }

    public void setMemoryStoreName(String memoryStoreName) {
        this.memoryStoreName = memoryStoreName;
    }

    public String getMemoryId() {
        return memoryId;
    }

    public void setMemoryId(String memoryId) {
        this.memoryId = memoryId;
    }

    public Scope getScope() {
        return scope;
    }

    public void setScope(Scope scope) {
        this.scope = scope;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }
}
