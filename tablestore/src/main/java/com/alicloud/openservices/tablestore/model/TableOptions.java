package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.NumberUtils;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

import java.util.concurrent.TimeUnit;

/**
 * Table configuration options, used to configure TTL and MaxVersions.
 * <p>TTL: Short for TimeToLive. TableStore supports automatic expiration of data; TimeToLive is the duration that data remains valid.</p>
 * The server determines whether each version of every column has expired based on the current time, the version number of each version of every column, and the table's TTL settings. Expired data will be automatically cleaned up.
 * <p>MaxVersions: The maximum number of versions that TableStore retains for each column in a row. When the number of written versions exceeds MaxVersions, TableStore only keeps the MaxVersions most recent versions with the highest version numbers.</p>
 * <p>MaxTimeDeviation: The maximum allowable deviation between the version specified when writing data to TableStore and the system time. Data with a deviation greater than MaxTimeDeviation from the system time is not allowed to be written.</p>
 */
public class TableOptions implements Jsonizable {
    /**
     * TTL (Time-to-Live) for table data, in seconds.
     * After the table is created, this configuration item can be dynamically changed by calling 
     * {@link com.alicloud.openservices.tablestore.SyncClient#updateTable(UpdateTableRequest)}.
     */
    private OptionalValue<Integer> timeToLive = new OptionalValue<Integer>("TimeToLive");

    /**
     * The maximum number of versions to retain for property columns.
     * After the table is created, this configuration item can be dynamically changed by calling 
     * {@link com.alicloud.openservices.tablestore.SyncClient#updateTable(UpdateTableRequest)}.
     */
    private OptionalValue<Integer> maxVersions = new OptionalValue<Integer>("MaxVersions");

    /**
     * The maximum allowable deviation in seconds between the version specified when writing data and the system's current time.
     * Writing data with a deviation outside this range is not allowed.
     * After the table is created, this configuration item can be dynamically changed by calling 
     * {@link com.alicloud.openservices.tablestore.SyncClient#updateTable(UpdateTableRequest)}.
     */
    private OptionalValue<Long> maxTimeDeviation = new OptionalValue<Long>("MaxTimeDeviation");

    /**
     * Whether Update operations are allowed on the table.
     * If set to false, only Put and Delete operations can be performed on the corresponding table, and Update operations are not allowed.
     * After the table is created, this configuration item can be dynamically modified by calling 
     * {@link com.alicloud.openservices.tablestore.SyncClient#updateTable(UpdateTableRequest)}.
     */
    private OptionalValue<Boolean> allowUpdate = new OptionalValue<Boolean>("AllowUpdate");

    /**
     * Whether the data in the table must be updated as a whole row during Update.
     * Whole-row update: When updating partial columns, the version number of the original columns will automatically be filled into the row.
     * For example, if a row contains the columns pk, a, b, c, and the Update column parameters only include pk and a, then the version numbers for columns b and c will be automatically filled (consistent with the version numbers of pk and a).
     * This configuration item is set to false by default.
     * If set to true,
     * Advantage (why update the entire row): The corresponding table can support secondary index and multi-index TTL functionality when allow_update is true;
     *      If the configuration is false and allow_update is true, then index TTL is not supported.
     * Disadvantage: 1. The Update operation will automatically fill in the version number of the original columns, increasing the write load of the Update request.
     *      2. Additionally, since automatic filling of original columns is required, the read load of the Update operation (reading original columns before writing) increases, along with the read CU.
     * After the table is created, this configuration cannot be dynamically modified.
     */
    private OptionalValue<Boolean> updateFullRow = new OptionalValue<Boolean>("UpdateFullRow");

    /**
     * Constructs a TableOptions object.
     */
    public TableOptions() {
    }

    /**
     * Constructs a TableOptions object.
     *
     * @param timeToLive TTL time
     */
    public TableOptions(int timeToLive) {
        setTimeToLive(timeToLive);
    }

    /**
     * Construct a TableOptions object.
     *
     * @param timeToLive  TTL time
     * @param maxVersions Maximum number of versions to retain
     */
    public TableOptions(int timeToLive, int maxVersions) {
        setTimeToLive(timeToLive);
        setMaxVersions(maxVersions);
    }

    /**
     * Constructs a TableOptions object.
     *
     * @param timeToLive  Time-to-live (TTL) duration
     * @param maxVersions Maximum number of versions to retain
     * @param maxTimeDeviation Maximum allowable deviation between the specified version being written and the system time
     */
    public TableOptions(int timeToLive, int maxVersions, long maxTimeDeviation) {
        setTimeToLive(timeToLive);
        setMaxVersions(maxVersions);
        setMaxTimeDeviation(maxTimeDeviation);
    }

    /**
     * Construct a TableOptions object
     */
     public TableOptions(boolean allowUpdate) {
         setAllowUpdate(allowUpdate);
     }

    /**
     * Construct a TableOptions object.
     *
     * @param allowUpdate Whether to allow Update operations
     * @param updateFullRow Whether the entire row must be updated together during an Update operation
     */
    public TableOptions(boolean allowUpdate, boolean updateFullRow) {
        setAllowUpdate(allowUpdate);
        setUpdateFullRow(updateFullRow);
    }

    /**
     * Get the TTL time, in seconds.
     *
     * @return TTL time
     * @throws java.lang.IllegalStateException if this parameter is not configured
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
     * Set the TTL time for table data
     *
     * @param days TTL time, in days
     */
    public void setTimeToLiveInDays(int days) {
        Preconditions.checkArgument(days > 0 || days == -1,
                "The value of timeToLive can be -1 or any positive value.");
        if (days == -1) {
            this.timeToLive.setValue(-1);
        } else {
            long seconds = TimeUnit.DAYS.toSeconds(days);
            this.timeToLive.setValue(NumberUtils.longToInt(seconds));
        }
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
     * @throws java.lang.IllegalStateException if this parameter is not configured
     */
    public int getMaxVersions() {
        if (!maxVersions.isValueSet()) {
            throw new IllegalStateException("The value of MaxVersions is not set.");
        }
        return maxVersions.getValue();
    }

    /**
     * Set the maximum number of versions.
     *
     * @param maxVersions the maximum number of versions
     */
    public void setMaxVersions(int maxVersions) {
        Preconditions.checkArgument(maxVersions > 0, "MaxVersions must be greater than 0.");
        this.maxVersions.setValue(maxVersions);
    }

    /**
     * Query whether {@link #setMaxVersions(int)} has been called to set MaxVersions.
     *
     * @return Whether MaxVersions has been set.
     */
    public boolean hasSetMaxVersions() {
        return maxVersions.isValueSet();
    }

    /**
     * Get the maximum deviation allowed between the version specified when writing data and the system's current time.
     *
     * @return Maximum deviation
     * @throws java.lang.IllegalStateException If this parameter is not configured
     */
    public long getMaxTimeDeviation() {
        if (!maxTimeDeviation.isValueSet()) {
            throw new IllegalStateException("The value of MaxTimeDeviation is not set.");
        }
        return maxTimeDeviation.getValue();
    }

    /**
     * Set the maximum deviation allowed between the version specified when writing data and the system's current time.
     *
     * @param maxTimeDeviation Maximum deviation in seconds
     */
    public void setMaxTimeDeviation(long maxTimeDeviation) {
        Preconditions.checkArgument(maxTimeDeviation > 0, "MaxTimeDeviation must be greater than 0.");
        this.maxTimeDeviation.setValue(maxTimeDeviation);
    }

    /**
     * Query whether {@link #setMaxTimeDeviation(long)} has been called to set MaxTimeDeviation.
     *
     * @return Whether MaxTimeDeviation has been set.
     */
    public boolean hasSetMaxTimeDeviation() {
        return maxTimeDeviation.isValueSet();
    }

    /**
     * Set whether the data in the table allows or prohibits Update operations
     * @param allowUpdate
     */
    public void setAllowUpdate(boolean allowUpdate) {
        this.allowUpdate.setValue(allowUpdate);
    }

    /**
     * Query whether {@link #setAllowUpdate(boolean)} is called to prohibit/allow Update operations on the data in the table
     * @return Whether AllowUpdate is set
     */
    public boolean hasSetAllowUpdate() { return allowUpdate.isValueSet(); }

    /**
     * Get whether the data in the table allows Update operations
     * @return Whether Update is allowed
     * @throws java.lang.IllegalStateException If this parameter is not configured
     */
    public boolean getAllowUpdate() {
        if (!allowUpdate.isValueSet()) {
            throw new IllegalStateException("The value of AllowUpdate is not set.");
        }
        return allowUpdate.getValue();
    }

    /**
     * Query whether {@link #setUpdateFullRow(boolean)} is called to set if the entire row must be updated together when updating data.
     * @return Whether UpdateFullRow is set.
     */
    public boolean hasSetUpdateFullRow() { return this.updateFullRow.isValueSet(); }

    /**
     * Check if the entire row must be updated together when performing an Update operation.
     * @return Whether the entire row must be updated together during an Update operation.
     * @throws java.lang.IllegalStateException If this parameter is not configured.
     */
    public boolean getUpdateFullRow() {
        if (!this.updateFullRow.isValueSet()) {
            throw new IllegalStateException("The value of UpdateFullRow is not set.");
        }
        return this.updateFullRow.getValue();
    }

    /**
     * Set whether the table must be updated with the entire row during data Update
     * @param updateFullRow
     */
    public void setUpdateFullRow(boolean updateFullRow) {
        this.updateFullRow.setValue(updateFullRow);
    }

    @Override
    public String toString() {
        return timeToLive + ", " + maxVersions + ", " + maxTimeDeviation + ", " + allowUpdate + ", " + updateFullRow;
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
        if (this.maxVersions.isValueSet()) {
            if (firstItem) {
                firstItem = false;
            } else {
                sb.append(", ");
            }
            sb.append("\"MaxVersions\": ");
            sb.append(this.maxVersions.getValue());
        }
        if (this.maxTimeDeviation.isValueSet()) {
            if (firstItem) {
                firstItem = false;
            } else {
                sb.append(", ");
            }
            sb.append("\"MaxTimeDeviation\": ");
            sb.append(this.maxTimeDeviation.getValue());
        }
        if (this.allowUpdate.isValueSet()) {
            if (firstItem) {
                firstItem = false;
            } else {
                sb.append(", ");
            }
            sb.append("\"AllowUpdate\": ");
            sb.append(this.allowUpdate.getValue());
        }
        if (this.updateFullRow.isValueSet()) {
            if (firstItem) {
                firstItem = false;
            } else {
                sb.append(", ");
            }
            sb.append("\"UpdateFullRow\": ");
            sb.append(this.updateFullRow.getValue());
        }
        return firstItem;
    }

}
