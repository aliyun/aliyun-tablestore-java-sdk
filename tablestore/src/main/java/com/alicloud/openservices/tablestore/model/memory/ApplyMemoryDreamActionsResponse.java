package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.Response;
import java.util.List;

public class ApplyMemoryDreamActionsResponse extends Response {
    private String memoryStoreName;
    private String dreamId;
    private Integer applied;
    private Integer failed;
    private Integer notFound;
    private List<DreamApplyActionResult> results;

    public ApplyMemoryDreamActionsResponse() {
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

    public Integer getApplied() {
        return applied;
    }

    public void setApplied(Integer applied) {
        this.applied = applied;
    }

    public Integer getFailed() {
        return failed;
    }

    public void setFailed(Integer failed) {
        this.failed = failed;
    }

    public Integer getNotFound() {
        return notFound;
    }

    public void setNotFound(Integer notFound) {
        this.notFound = notFound;
    }

    public List<DreamApplyActionResult> getResults() {
        return results;
    }

    public void setResults(List<DreamApplyActionResult> results) {
        this.results = results;
    }
}
