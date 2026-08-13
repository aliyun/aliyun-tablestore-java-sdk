package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.Response;
import java.util.List;

public class ListMemoryDreamTasksResponse extends Response {
    private String memoryStoreName;
    private Scope scope;
    private List<DreamTask> tasks;
    private String nextToken;

    public ListMemoryDreamTasksResponse() {
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

    public List<DreamTask> getTasks() {
        return tasks;
    }

    public void setTasks(List<DreamTask> tasks) {
        this.tasks = tasks;
    }

    public String getNextToken() {
        return nextToken;
    }

    public void setNextToken(String nextToken) {
        this.nextToken = nextToken;
    }
}
