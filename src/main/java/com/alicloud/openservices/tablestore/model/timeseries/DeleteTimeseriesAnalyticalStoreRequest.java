package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;
import com.alicloud.openservices.tablestore.timeline.utils.Preconditions;

public class DeleteTimeseriesAnalyticalStoreRequest implements Request {

    private final String timeseriesTableName;
    private final String analyticalStoreName;

    public DeleteTimeseriesAnalyticalStoreRequest(String timeseriesTableName, String analyticalStoreName) {
        Preconditions.checkArgument(
                timeseriesTableName != null && !timeseriesTableName.isEmpty(),
                "The name of table should not be null or empty.");
        Preconditions.checkArgument(
                analyticalStoreName != null && !analyticalStoreName.isEmpty(),
                "The name of analytical store should not be null or empty.");
        this.timeseriesTableName = timeseriesTableName;
        this.analyticalStoreName = analyticalStoreName;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_DELETE_TIMESERIES_ANALYTICAL_STORE;
    }

    public String getTimeseriesTableName() {
        return timeseriesTableName;
    }

    public String getAnalyticalStoreName() {
        return analyticalStoreName;
    }
}
