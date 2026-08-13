package com.alicloud.openservices.tablestore.model.memory;

import java.util.Map;

public class DreamTaskActionStats {
    private Integer total;
    private Integer proposed;
    private Integer applied;
    private Integer skipped;
    private Integer failed;
    private Map<String, Integer> skippedByReason;

    public DreamTaskActionStats() {
    }

    public Integer getTotal() {
        return total;
    }

    public void setTotal(Integer total) {
        this.total = total;
    }

    public Integer getProposed() {
        return proposed;
    }

    public void setProposed(Integer proposed) {
        this.proposed = proposed;
    }

    public Integer getApplied() {
        return applied;
    }

    public void setApplied(Integer applied) {
        this.applied = applied;
    }

    public Integer getSkipped() {
        return skipped;
    }

    public void setSkipped(Integer skipped) {
        this.skipped = skipped;
    }

    public Integer getFailed() {
        return failed;
    }

    public void setFailed(Integer failed) {
        this.failed = failed;
    }

    public Map<String, Integer> getSkippedByReason() {
        return skippedByReason;
    }

    public void setSkippedByReason(Map<String, Integer> skippedByReason) {
        this.skippedByReason = skippedByReason;
    }
}
