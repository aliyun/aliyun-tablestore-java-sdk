package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

public class DeleteTimeseriesLastpointIndexRequest implements Request {

    private final String timeseriesTableName;
    private final String lastpointIndexName;

    public DeleteTimeseriesLastpointIndexRequest(String timeseriesTableName, String lastpointIndexName) {
        this.timeseriesTableName = timeseriesTableName;
        this.lastpointIndexName = lastpointIndexName;
    }

    public String getTimeseriesTableName() {
        return timeseriesTableName;
    }

    public String getLastpointIndexName() {
        return lastpointIndexName;
    }

    @Override
    public String toString() {
        return "DeleteTimeseriesLastpointIndexRequest [timeseriesTableName="
                + timeseriesTableName
                + ", lastpointIndexName="
                + lastpointIndexName
                + "]";
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_DELETE_TIMESERIES_LASTPOINT_INDEX;
    }
}
