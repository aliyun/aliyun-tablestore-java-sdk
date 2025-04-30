package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;
import com.alicloud.openservices.tablestore.model.TimeseriesMetaOptions;
import com.alicloud.openservices.tablestore.model.TimeseriesTableOptions;


public class UpdateTimeseriesTableRequest implements Request {

    /**
     * Table name information and table option information
     */
    private String timeseriesTableName;
    private TimeseriesTableOptions timeseriesTableOptions;

    private TimeseriesMetaOptions timeseriesMetaOptions;

    /**
     * Initialize the UpdateTimeseriesTableRequest instance.
     * @param timeseriesTableName The table name.
     * @param timeseriesTableOptions The table options.
     */
    public UpdateTimeseriesTableRequest(String timeseriesTableName, TimeseriesTableOptions timeseriesTableOptions) {
        setTimeseriesTableName(timeseriesTableName);
        setTimeseriesTableOptions(timeseriesTableOptions);
    }

    public UpdateTimeseriesTableRequest(String timeseriesTableName) {
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
     * Get the options of the table.
     * @return The options of the table.
     */
    public TimeseriesTableOptions getTimeseriesTableOptions() {
        return timeseriesTableOptions;
    }

    /**
     * Sets the name of the table.
     * @param timeseriesTableName The name of the table.
     */
    public void setTimeseriesTableName(String timeseriesTableName) {
        Preconditions.checkArgument(
                timeseriesTableName != null && !timeseriesTableName.isEmpty(),
                "The name of table should not be null or empty.");
        this.timeseriesTableName = timeseriesTableName;
    }

    /**
     * Get the configuration related to timeline metadata
     * @return
     */
    public TimeseriesMetaOptions getTimeseriesMetaOptions() {
        return timeseriesMetaOptions;
    }

    /**
     * Set the configuration related to timeline metadata
     * @param timeseriesMetaOptions
     */
    public void setTimeseriesMetaOptions(TimeseriesMetaOptions timeseriesMetaOptions) {
        this.timeseriesMetaOptions = timeseriesMetaOptions;
    }

    /**
     * Set the options for the table.
     * @param timeseriesTableOptions The options for the table.
     */
    public void setTimeseriesTableOptions(TimeseriesTableOptions timeseriesTableOptions) {
        Preconditions.checkNotNull(timeseriesTableOptions, "TimeseriesTableOptions should not be null.");
        this.timeseriesTableOptions = timeseriesTableOptions;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_UPDATE_TIMESERIES_TABLE;
    }

}
