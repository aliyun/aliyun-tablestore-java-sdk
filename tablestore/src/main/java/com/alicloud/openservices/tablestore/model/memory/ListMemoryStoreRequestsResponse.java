package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.Response;
import java.util.List;

public class ListMemoryStoreRequestsResponse extends Response {
    private List<MemoryStoreRequestRecord> requests;
    private Scope scope;
    private String memoryStoreName;
    private String nextToken;

    public ListMemoryStoreRequestsResponse() {
    }

    public List<MemoryStoreRequestRecord> getRequests() {
        return requests;
    }

    public void setRequests(List<MemoryStoreRequestRecord> requests) {
        this.requests = requests;
    }

    public Scope getScope() {
        return scope;
    }

    public void setScope(Scope scope) {
        this.scope = scope;
    }

    public String getMemoryStoreName() {
        return memoryStoreName;
    }

    public void setMemoryStoreName(String memoryStoreName) {
        this.memoryStoreName = memoryStoreName;
    }

    public String getNextToken() {
        return nextToken;
    }

    public void setNextToken(String nextToken) {
        this.nextToken = nextToken;
    }
}
