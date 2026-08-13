package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.Response;
import java.util.List;

public class SearchMemoriesResponse extends Response {
    private List<SearchHit> results;
    private Scope scope;
    private String memoryStoreName;

    public SearchMemoriesResponse() {
    }

    public List<SearchHit> getResults() {
        return results;
    }

    public void setResults(List<SearchHit> results) {
        this.results = results;
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
}
