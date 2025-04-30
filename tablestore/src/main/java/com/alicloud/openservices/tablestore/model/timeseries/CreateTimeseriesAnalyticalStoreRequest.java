package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;
import com.alicloud.openservices.tablestore.timeline.utils.Preconditions;

public class CreateTimeseriesAnalyticalStoreRequest implements Request {

    private final String timeseriesTableName;
    private final TimeseriesAnalyticalStore analyticalStore;

    public CreateTimeseriesAnalyticalStoreRequest(String timeseriesTableName, TimeseriesAnalyticalStore analyticalStore) {
        Preconditions.checkArgument(
                timeseriesTableName != null && !timeseriesTableName.isEmpty(),
                "The name of table should not be null or empty.");
        Preconditions.checkArgument(analyticalStore != null, "The analytical store should not be null.");
        Preconditions.checkArgument(
                analyticalStore.getAnalyticalStoreName() != null && !analyticalStore.getAnalyticalStoreName().isEmpty(),
                "The name of analytical store should not be null or empty.");
        this.timeseriesTableName = timeseriesTableName;
        this.analyticalStore = analyticalStore;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_CREATE_TIMESERIES_ANALYTICAL_STORE;
    }

    public String getTimeseriesTableName() {
        return timeseriesTableName;
    }

    public TimeseriesAnalyticalStore getAnalyticalStore() {
        return analyticalStore;
    }
}
