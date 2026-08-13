package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.Response;
import java.util.List;

public class ListMemoryDreamActionsResponse extends Response {
    private String memoryStoreName;
    private String dreamId;
    private List<DreamActionRecord> actions;
    private String nextToken;

    public ListMemoryDreamActionsResponse() {
    }

    public String getMemoryStoreName() {
        return memoryStoreName;
    }

    public void setMemoryStoreName(String memoryStoreName) {
        this.memoryStoreName = memoryStoreName;
    }

    public String getDreamId() {
        return dreamId;
    }

    public void setDreamId(String dreamId) {
        this.dreamId = dreamId;
    }

    public List<DreamActionRecord> getActions() {
        return actions;
    }

    public void setActions(List<DreamActionRecord> actions) {
        this.actions = actions;
    }

    public String getNextToken() {
        return nextToken;
    }

    public void setNextToken(String nextToken) {
        this.nextToken = nextToken;
    }
}
