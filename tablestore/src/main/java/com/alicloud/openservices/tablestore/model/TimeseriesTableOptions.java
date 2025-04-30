package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;
import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

/**
 * Configuration options for the time-series table, currently only used for configuring TTL.
 * <p>TTL: Abbreviation of TimeToLive. TableStore supports automatic data expiration, and TimeToLive refers to the data's survival time.</p>
 * The server determines whether a specific version of each column has expired based on the current time, the version number of each column, and the TTL setting of the table. Data that has expired will be automatically cleaned up.
 */
public class TimeseriesTableOptions implements Jsonizable {
    /**
     * The TTL (Time-To-Live) for table data, in seconds.
     * After the table is created, this configuration item can be dynamically changed by calling 
     * {@link com.alicloud.openservices.tablestore.SyncClient#updateTable(UpdateTableRequest)}.
     */
    private OptionalValue<Integer> timeToLive = new OptionalValue<Integer>("TimeToLive");

    /**
     * Constructs a TableOptions object.
     */
    public TimeseriesTableOptions() {
    }

    /**
     * Constructs a TableOptions object.
     *
     * @param timeToLive TTL time
     */
    public TimeseriesTableOptions(int timeToLive) {
        setTimeToLive(timeToLive);
    }

    /**
     * Get the TTL time, in seconds.
     *
     * @return TTL time
     * @throws IllegalStateException if this parameter is not configured
     */
    public int getTimeToLive() {
        if (!timeToLive.isValueSet()) {
            throw new IllegalStateException("The value of TimeToLive is not set.");
        }
        return timeToLive.getValue();
    }

    /**
     * Set the TTL for table data, in seconds.
     *
     * @param timeToLive TTL time, in seconds
     */
    public void setTimeToLive(int timeToLive) {
        Preconditions.checkArgument(timeToLive > 0 || timeToLive == -1,
                "The value of timeToLive can be -1 or any positive value.");
        this.timeToLive.setValue(timeToLive);
    }

    /**
     * Query whether {@link #setTimeToLive(int)} has been called to set TTL.
     *
     * @return Whether TTL has been set.
     */
    public boolean hasSetTimeToLive() {
        return this.timeToLive.isValueSet();
    }

    /**
     * Get the maximum number of versions.
     *
     * @return the maximum number of versions
     * @throws IllegalStateException if this parameter is not configured
     */

    @Override
    public String toString() {
        return timeToLive.toString();
    }

    @Override
    public String jsonize() {
        StringBuilder sb = new StringBuilder();
        jsonize(sb, "\n  ");
        return sb.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        sb.append("{");
        this.jsonizeFields(sb, true);
        sb.append("}");
    }

    protected boolean jsonizeFields(StringBuilder sb, boolean firstItem) {
        if (this.timeToLive.isValueSet()) {
            if (firstItem) {
                firstItem = false;
            } else {
                sb.append(", ");
            }
            sb.append("\"TimeToLive\": ");
            sb.append(this.timeToLive.getValue());
        }
        return firstItem;
    }

}
