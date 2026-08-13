package com.alicloud.openservices.tablestore.model.memory;

public class RankingSignals {
    private Boolean sameSession;
    private Double sessionAffinityWeight;

    public RankingSignals() {
    }

    public Boolean getSameSession() {
        return sameSession;
    }

    public void setSameSession(Boolean sameSession) {
        this.sameSession = sameSession;
    }

    public Double getSessionAffinityWeight() {
        return sessionAffinityWeight;
    }

    public void setSessionAffinityWeight(Double sessionAffinityWeight) {
        this.sessionAffinityWeight = sessionAffinityWeight;
    }
}
