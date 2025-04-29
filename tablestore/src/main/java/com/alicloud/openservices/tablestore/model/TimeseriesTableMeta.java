package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * The structure information of the table, including the table name and configuration information.
 */
public class TimeseriesTableMeta implements Jsonizable {

    /**
     * The name of the table.
     */
    private String timeseriesTableName;

    /**
     * Table configuration options, including data TTL, etc.
     */
    private TimeseriesTableOptions timeseriesTableOptions;

    /**
     * Timeline metadata related configuration items, including metadata TTL, etc.
     */
    private TimeseriesMetaOptions timeseriesMetaOptions;

    /**
     * The state of the table.
     */
    private String status;

    /**
     * Customizes the primary key column.
     */
    private final List<String> timeseriesKeys = new ArrayList<String>();

    /**
     * Extend the primary key column.
     */
    private final List<PrimaryKeySchema> fieldPrimaryKeys = new ArrayList<PrimaryKeySchema>();

    /**
     * Create a new <code>TableMeta</code> instance with the given table name.
     *
     * @param timeseriesTableName The name of the table.
     */
    public TimeseriesTableMeta(String timeseriesTableName) {
        this(timeseriesTableName, new TimeseriesTableOptions());
    }

    public TimeseriesTableMeta(String timeseriesTableName, TimeseriesTableOptions timeseriesTableOptions) {
        Preconditions.checkArgument(timeseriesTableName != null && !timeseriesTableName.isEmpty(), "The name of table should not be null or empty.");
        setTimeseriesTableName(timeseriesTableName);
        setTimeseriesTableOptions(timeseriesTableOptions);
    }

    /* For internal use only */
    public TimeseriesTableMeta(String timeseriesTableName, TimeseriesTableOptions timeseriesTableOptions, String status) {
        Preconditions.checkArgument(timeseriesTableName != null && !timeseriesTableName.isEmpty(), "The name of table should not be null or empty.");
        setTimeseriesTableName(timeseriesTableName);
        setTimeseriesTableOptions(timeseriesTableOptions);
        setStatus(status);
    }

    /**
     * Sets the name of the table.
     *
     * @param timeseriesTableName The name of the table.
     */
    public void setTimeseriesTableName(String timeseriesTableName) {
        Preconditions.checkArgument(timeseriesTableName != null && !timeseriesTableName.isEmpty(), "The name of table should not be null or empty.");

        this.timeseriesTableName = timeseriesTableName;
    }

    /**
     * Returns the name of the table.
     *
     * @return the name of the table.
     */
    public String getTimeseriesTableName() {
        return timeseriesTableName;
    }

    /**
     * Set the table configuration parameters.
     *
     * @param timeseriesTableOptions The table configuration.
     */
    public void setTimeseriesTableOptions(TimeseriesTableOptions timeseriesTableOptions) {
        Preconditions.checkNotNull(timeseriesTableOptions, "TimeseriesTableOptions should not be null.");
        this.timeseriesTableOptions = timeseriesTableOptions;
    }

    /**
     * Returns the table configuration.
     *
     * @return The table configuration.
     */
    public TimeseriesTableOptions getTimeseriesTableOptions() {
        return timeseriesTableOptions;
    }

    public TimeseriesMetaOptions getTimeseriesMetaOptions() {
        return timeseriesMetaOptions;
    }

    public void setTimeseriesMetaOptions(TimeseriesMetaOptions timeseriesMetaOptions) {
        Preconditions.checkNotNull(timeseriesMetaOptions, "TimeseriesMetaOptions should not be null.");
        this.timeseriesMetaOptions = timeseriesMetaOptions;
    }

    /**
     * Set the table configuration parameters.
     *
     * @param status The table configuration.
     */
    public void setStatus(String status) {
        this.status = status;
    }

    /**
     * Returns the table configuration.
     *
     * @return The table configuration.
     */
    public String getStatus() {
        return status;
    }

    public void addTimeseriesKey(String primaryKey) {
        timeseriesKeys.add(primaryKey);
    }

    public List<String> getTimeseriesKeys() {
        return timeseriesKeys;
    }

    public void addFieldPrimaryKey(String name, PrimaryKeyType type) {
        Preconditions.checkArgument(name != null && !name.isEmpty(), "The name of primary key field should not be null or empty.");
        Preconditions.checkNotNull(type, "The type of primary key field should not be null.");
        fieldPrimaryKeys.add(new PrimaryKeySchema(name, type));
    }

    /**
     * Returns the extended primary key column.
     *
     * @return The extended primary key column.
     */
    public List<PrimaryKeySchema> getFieldPrimaryKeys() {
        return fieldPrimaryKeys;
    }

    @Override
    public String toString() {
        String s = "TimeseriesTableName: " + timeseriesTableName;
        return s;
    }

    @Override
    public String jsonize() {
        StringBuilder sb = new StringBuilder();
        jsonize(sb, "\n  ");
        return sb.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        sb.append('{');
        sb.append(newline);
        sb.append("\"TimeseriesTableName\": \"");
        sb.append(timeseriesTableName);
        sb.append('\"');

        sb.append("}");
    }
}
