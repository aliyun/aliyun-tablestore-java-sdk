package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.model.Response;

public class DescribeTimeseriesAnalyticalStoreResponse extends Response {

    private TimeseriesAnalyticalStore analyticalStore;
    private AnalyticalStoreStorageSize storageSize;
    private AnalyticalStoreSyncStat syncStat;

    public DescribeTimeseriesAnalyticalStoreResponse(Response meta) {
        super(meta);
    }

    public TimeseriesAnalyticalStore getAnalyticalStore() {
        return analyticalStore;
    }

    public AnalyticalStoreStorageSize getStorageSize() {
        return storageSize;
    }

    public AnalyticalStoreSyncStat getSyncStat() {
        return syncStat;
    }

    public void setAnalyticalStore(TimeseriesAnalyticalStore analyticalStore) {
        this.analyticalStore = analyticalStore;
    }

    public void setStorageSize(AnalyticalStoreStorageSize storageSize) {
        this.storageSize = storageSize;
    }

    public void setSyncStat(AnalyticalStoreSyncStat syncStat) {
        this.syncStat = syncStat;
    }
}
