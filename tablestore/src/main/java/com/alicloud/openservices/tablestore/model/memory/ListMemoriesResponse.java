package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.Response;
import java.util.List;

public class ListMemoriesResponse extends Response {
    private List<MemoryUnit> memories;
    private Scope scope;
    private String memoryStoreName;
    private String nextToken;

    public ListMemoriesResponse() {
    }

    public List<MemoryUnit> getMemories() {
        return memories;
    }

    public void setMemories(List<MemoryUnit> memories) {
        this.memories = memories;
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
