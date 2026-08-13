package com.alicloud.openservices.tablestore.model.memory;

import java.util.List;

public class MemoryStoreSession {
    private Scope scope;
    private List<SessionMessage> messages;
    private String nextToken;

    public MemoryStoreSession() {
    }

    public Scope getScope() {
        return scope;
    }

    public void setScope(Scope scope) {
        this.scope = scope;
    }

    public List<SessionMessage> getMessages() {
        return messages;
    }

    public void setMessages(List<SessionMessage> messages) {
        this.messages = messages;
    }

    public String getNextToken() {
        return nextToken;
    }

    public void setNextToken(String nextToken) {
        this.nextToken = nextToken;
    }
}
