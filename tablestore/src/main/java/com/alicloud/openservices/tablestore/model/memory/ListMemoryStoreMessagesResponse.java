package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.Response;

public class ListMemoryStoreMessagesResponse extends Response {
    private MemoryStoreSession session;
    private String nextToken;

    public ListMemoryStoreMessagesResponse() {
    }

    public MemoryStoreSession getSession() {
        return session;
    }

    public void setSession(MemoryStoreSession session) {
        this.session = session;
    }

    public String getNextToken() {
        return nextToken;
    }

    public void setNextToken(String nextToken) {
        this.nextToken = nextToken;
    }
}
