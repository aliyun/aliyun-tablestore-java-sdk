package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.OperationNames;
import java.util.List;
import java.util.Map;

public class AddMemoriesRequest extends AbstractMemoryRequest {
    private String memoryStoreName;
    private Scope scope;
    private List<MessagePayload> messages;
    private String text;
    private Map<String, String> metadata;
    private Boolean sync;

    public AddMemoriesRequest() {
    }

    public AddMemoriesRequest(String memoryStoreName) {
        this.memoryStoreName = memoryStoreName;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_ADD_MEMORIES;
    }

    public String getMemoryStoreName() {
        return memoryStoreName;
    }

    public void setMemoryStoreName(String memoryStoreName) {
        this.memoryStoreName = memoryStoreName;
    }

    public Scope getScope() {
        return scope;
    }

    public void setScope(Scope scope) {
        this.scope = scope;
    }

    public List<MessagePayload> getMessages() {
        return messages;
    }

    public void setMessages(List<MessagePayload> messages) {
        this.messages = messages;
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

    public Boolean getSync() {
        return sync;
    }

    public void setSync(Boolean sync) {
        this.sync = sync;
    }
}
