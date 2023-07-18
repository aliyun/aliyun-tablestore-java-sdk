package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class TimeseriesAnalyticalStore {

    private final String analyticalStoreName;
    private OptionalValue<Integer> timeToLive = new OptionalValue<Integer>("TimeToLive");
    private OptionalValue<AnalyticalStoreSyncType> syncOption = new OptionalValue<AnalyticalStoreSyncType>("SyncOption");

    public TimeseriesAnalyticalStore(String analyticalStoreName) {
        Preconditions.checkArgument(analyticalStoreName != null && !analyticalStoreName.isEmpty(),
                "analyticalStoreName should not be null or empty.");
        this.analyticalStoreName = analyticalStoreName;
    }

    public String getAnalyticalStoreName() {
        return analyticalStoreName;
    }

    public int getTimeToLive() {
        if (!timeToLive.isValueSet()) {
            throw new IllegalStateException("The value of TimeToLive is not set.");
        }
        return timeToLive.getValue();
    }

    public AnalyticalStoreSyncType getSyncOption() {
        if (!syncOption.isValueSet()) {
            throw new IllegalStateException("The value of SyncOption is not set.");
        }
        return syncOption.getValue();
    }

    public boolean hasTimeToLive() {
        return timeToLive.isValueSet();
    }

    public boolean hasSyncOption() {
        return syncOption.isValueSet();
    }

    public void setTimeToLive(Integer timeToLive) {
        this.timeToLive.setValue(timeToLive);
    }

    public void setSyncOption(AnalyticalStoreSyncType syncOption) {
        this.syncOption.setValue(syncOption);
    }
}
