package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

public class SyncStat implements Jsonizable {

    public static enum SyncPhase {
        FULL,
        INCR
    }

    private SyncPhase syncPhase;
    private Long currentSyncTimestamp;

    public SyncPhase getSyncPhase() {
        return syncPhase;
    }

    public void setSyncPhase(SyncPhase syncPhase) {
        this.syncPhase = syncPhase;
    }

    public Long getCurrentSyncTimestamp() {
        return currentSyncTimestamp;
    }

    public void setCurrentSyncTimestamp(Long currentSyncTimestamp) {
        this.currentSyncTimestamp = currentSyncTimestamp;
    }

    @Override
    public String jsonize() {
        StringBuilder sb = new StringBuilder();
        jsonize(sb, "\n  ");
        return sb.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        sb.append('{');
        sb.append(newline);
        if (syncPhase != null) {
            sb.append("\"SyncPhase\": \"");
            sb.append(syncPhase.name());
            sb.append("\"");
            sb.append(",");
            sb.append(newline);
        }
        if (currentSyncTimestamp != null) {
            sb.append("\"CurrentSyncTimestamp\": ");
            sb.append(currentSyncTimestamp.toString());
        }
        sb.append("}");
    }
}
