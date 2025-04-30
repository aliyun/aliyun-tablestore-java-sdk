package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

public class DeleteTimeseriesTableRequest implements Request {

    /**
     * Table name information.
     */
    private String timeseriesTableName;

    /**
     * Initialize a DeleteTimeseriesTableRequest instance.
     * @param timeseriesTableName The name of the table.
     */
    public DeleteTimeseriesTableRequest(String timeseriesTableName) {
        setTimeseriesTableName(timeseriesTableName);
    }

    /**
     * Get the name of the table.
     * @return The name of the table.
     */
    public String getTimeseriesTableName() {
        return timeseriesTableName;
    }

    /**
     * Set the name of the table.
     * @param timeseriesTableName The name of the table.
     */
    public void setTimeseriesTableName(String timeseriesTableName) {
        Preconditions.checkArgument(
                timeseriesTableName != null && !timeseriesTableName.isEmpty(),
                "The name of table should not be null or empty.");

        this.timeseriesTableName = timeseriesTableName;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_DELETE_TIMESERIES_TABLE;
    }
}
