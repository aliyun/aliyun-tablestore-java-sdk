package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;
import com.alicloud.openservices.tablestore.timeline.utils.Preconditions;

public class DescribeTimeseriesAnalyticalStoreRequest implements Request {

    private final String timeseriesTableName;
    private final String analyticalStoreName;

    public DescribeTimeseriesAnalyticalStoreRequest(String timeseriesTableName, String analyticalStoreName) {
        Preconditions.checkArgument(timeseriesTableName != null && !timeseriesTableName.isEmpty(),
                "The timeseries table name should not be null or empty.");
        Preconditions.checkArgument(analyticalStoreName != null && !analyticalStoreName.isEmpty(),
                "The analytical store name should not be null or empty.");
        this.timeseriesTableName = timeseriesTableName;
        this.analyticalStoreName = analyticalStoreName;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_DESCRIBE_TIMESERIES_ANALYTICAL_STORE;
    }

    public String getTimeseriesTableName() {
        return timeseriesTableName;
    }

    public String getAnalyticalStoreName() {
        return analyticalStoreName;
    }
}
