package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.Response;
import java.util.List;

public class ListMemoryTasksResponse extends Response {
    private List<MemoryTask> tasks;
    private Scope scope;
    private String memoryStoreName;
    private String nextToken;

    public ListMemoryTasksResponse() {
    }

    public List<MemoryTask> getTasks() {
        return tasks;
    }

    public void setTasks(List<MemoryTask> tasks) {
        this.tasks = tasks;
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
