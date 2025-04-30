package com.alicloud.openservices.tablestore.model.timeseries;

public class AnalyticalStoreSyncStat {

    private AnalyticalStoreSyncType syncPhase;
    private long currentSyncTimestamp;

    public void setSyncPhase(AnalyticalStoreSyncType syncPhase) {
        this.syncPhase = syncPhase;
    }

    public void setCurrentSyncTimestamp(long currentSyncTimestamp) {
        this.currentSyncTimestamp = currentSyncTimestamp;
    }

    public AnalyticalStoreSyncType getSyncPhase() {
        return syncPhase;
    }

    public long getCurrentSyncTimestamp() {
        return currentSyncTimestamp;
    }
}
