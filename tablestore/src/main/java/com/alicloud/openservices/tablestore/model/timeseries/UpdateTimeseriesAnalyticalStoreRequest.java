package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;
import com.alicloud.openservices.tablestore.timeline.utils.Preconditions;

public class UpdateTimeseriesAnalyticalStoreRequest implements Request {

    private final String timeseriesTableName;
    private TimeseriesAnalyticalStore analyticStore;

    public UpdateTimeseriesAnalyticalStoreRequest(String timeseriesTableName) {
        this.timeseriesTableName = timeseriesTableName;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_UPDATE_TIMESERIES_ANALYTICAL_STORE;
    }

    public void setAnalyticStore(TimeseriesAnalyticalStore analyticStore) {
        Preconditions.checkNotNull(analyticStore, "analyticStore should not be null.");
        this.analyticStore = analyticStore;
    }

    public String getTimeseriesTableName() {
        return timeseriesTableName;
    }

    public TimeseriesAnalyticalStore getAnalyticStore() {
        return analyticStore;
    }
}
