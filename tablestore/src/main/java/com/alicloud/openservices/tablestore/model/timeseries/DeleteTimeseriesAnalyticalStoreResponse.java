package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.model.Response;

public class DeleteTimeseriesAnalyticalStoreResponse extends Response {

    private TimeseriesAnalyticalStore analyticalStore;
    private AnalyticalStoreSyncStat syncStat;
    private AnalyticalStoreStorageSize storageSize;

    public DeleteTimeseriesAnalyticalStoreResponse(Response meta) {
        super(meta);
    }

    public TimeseriesAnalyticalStore getAnalyticalStore() {
        return analyticalStore;
    }

    public AnalyticalStoreSyncStat getSyncStat() {
        return syncStat;
    }

    public AnalyticalStoreStorageSize getStorageSize() {
        return storageSize;
    }

    public void setAnalyticalStore(TimeseriesAnalyticalStore analyticalStore) {
        this.analyticalStore = analyticalStore;
    }
}
