package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.model.filter.Filter;

import java.util.*;

/**
 * Basic parameters for reading data from TableStore, mainly including:
 * <ul>
 * <li>ColumnsToGet: The list of attribute column names to read. If empty, it means reading all columns of the row.</li>
 * <li>TimeRange: The range of timestamps to read. If not set, it means reading all versions.</li>
 * <li>MaxVersions: The number of versions of the columns to return. If not set, it returns all versions currently retained by OTS.</li>
 * <li>Filter: Represents the Filter used for this query. The Filter can perform an initial filter on the data within the query range on the server side to reduce extra data transmission.</li>
 * </ul>
 */
public class RowQueryCriteria {
    /**
     * The name of the table being queried.
     */
    private String tableName;

    /**
     * The list of property column names to read. If it is empty, it means reading all columns of the row.
     */
    private Set<String> columnsToGet = new HashSet<String>();

    /**
     * The range of timestamps to read. If not set, it means reading all versions.
     */
    private OptionalValue<TimeRange> timeRange = new OptionalValue<TimeRange>("TimeRange");

    /**
     * The number of versions of the columns to be returned. If not set, it will return all versions currently retained by OTS.
     */
    private OptionalValue<Integer> maxVersions = new OptionalValue<Integer>("MaxVersions");

    /**
     * The Filter used for this query.
     */
    private OptionalValue<Filter> filter = new OptionalValue<Filter>("Filter");

    /**
     * The starting position of the query column range.
     */
    private OptionalValue<String> startColumn = new OptionalValue<String>("StartColumn");

    /**
     * The end position of the query column range.
     */
    private OptionalValue<String> endColumn = new OptionalValue<String>("EndColumn");


    /**
     * Internal parameters.
     */
    private OptionalValue<Boolean> cacheBlocks = new OptionalValue<Boolean>("CacheBlocks");

    /**
     * Constructor.
     *
     * @param tableName The name of the table to query.
     */
    public RowQueryCriteria(String tableName) {
        Preconditions.checkArgument(
        		tableName != null && !tableName.isEmpty(), 
        		"The name of table should not be null or empty.");
        this.tableName = tableName;
    }

    /**
     * Sets the name of the table to query.
     *
     * @param tableName The name of the table.
     */
    public void setTableName(String tableName) {
    	Preconditions.checkArgument(
    			tableName != null && !tableName.isEmpty(), 
    			"The name of table should not be null or empty.");
    	this.tableName = tableName;
    }

    /**
     * Returns the name of the queried table.
     *
     * @return the name of the table
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Returns the list of column names to be read (read-only).
     *
     * @return The list of column names (read-only).
     */
    public Set<String> getColumnsToGet() {
        return Collections.unmodifiableSet(columnsToGet);
    }

    /**
     * Add the column to be read.
     *
     * @param columnName The name of the column to be returned.
     */
    public void addColumnsToGet(String columnName) {
        Preconditions.checkArgument(columnName != null && !columnName.isEmpty(), "Column's name should not be null or empty.");
        this.columnsToGet.add(columnName);
    }

    /**
     * Add the columns to be read.
     *
     * @param columnNames The names of the columns to be returned.
     */
    public void addColumnsToGet(String[] columnNames) {
        Preconditions.checkNotNull(columnNames, "columnNames should not be null.");
        for (int i = 0; i < columnNames.length; ++i) {
            addColumnsToGet(columnNames[i]);
        }
    }

    /**
     * Add the columns to read.
     *
     * @param columnsToGet
     */
    public void addColumnsToGet(Collection<String> columnsToGet) {
        this.columnsToGet.addAll(columnsToGet);
    }

    /**
     * Clear the list of column names that have been set for reading.
     */
    public void clearColumnsToGet() {
        this.columnsToGet.clear();
    }

    /**
     * Returns the number of columns to be read.
     *
     * @return The number of columns to be read.
     */
    public int numColumnsToGet() {
        return this.columnsToGet.size();
    }

    /**
     * Set MaxVersions.
     *
     * @param maxVersions
     */
    public void setMaxVersions(int maxVersions) {
        Preconditions.checkArgument(maxVersions > 0, "The value of maxVersions must be greater than 0.");
        this.maxVersions.setValue(maxVersions);
    }

    /**
     * Get the set MaxVersions.
     *
     * @return MaxVersions
     * @throws java.lang.IllegalStateException if this parameter is not configured
     */
    public int getMaxVersions() {
        if (!this.maxVersions.isValueSet()) {
            throw new IllegalStateException("The value of maxVersions is not set.");
        }
        return this.maxVersions.getValue();
    }

    /**
     * Query whether MaxVersions is set.
     *
     * @return Returns true if MaxVersions is set, otherwise returns false.
     */
    public boolean hasSetMaxVersions() {
        return this.maxVersions.isValueSet();
    }

    /**
     * Set the timestamp range to read.
     *
     * @param timeRange the timestamp range
     */
    public void setTimeRange(TimeRange timeRange) {
        Preconditions.checkNotNull(timeRange, "The time range should not be null.");
        this.timeRange.setValue(timeRange);
    }

    /**
     * Set the specific timestamp to read.
     *
     * @param timestamp The timestamp
     */
    public void setTimestamp(long timestamp) {
        Preconditions.checkArgument(timestamp >= 0, "The timestamp must be positive.");
        this.timeRange.setValue(new TimeRange(timestamp, timestamp + 1));
    }

    /**
     * Get the configured time range.
     *
     * @return TimeRange
     * @throws java.lang.IllegalStateException if this parameter is not configured
     */
    public TimeRange getTimeRange() {
        if (!this.timeRange.isValueSet()) {
            throw new IllegalStateException("The value of timeRange is not set.");
        }
        return this.timeRange.getValue();
    }

    /**
     * Query whether TimeRange has been set.
     *
     * @return If TimeRange has been set, return true; otherwise, return false.
     */
    public boolean hasSetTimeRange() {
        return this.timeRange.isValueSet();
    }

    /**
     * Set the Filter to be used for this query.
     *
     * @param filter
     */
    public void setFilter(Filter filter) {
        Preconditions.checkNotNull(filter, "The filter should not be null");
        this.filter.setValue(filter);
    }

    /**
     * Get the Filter used in this query.
     *
     * @return Filter
     * @throws java.lang.IllegalStateException if the Filter is not set
     */
    public Filter getFilter() {
        if (!this.filter.isValueSet()) {
            throw new IllegalStateException("The value of filter is not set.");
        }
        return this.filter.getValue();
    }

    /**
     * Query whether the Filter is set.
     *
     * @return Returns true if the Filter is set, otherwise returns false.
     */
    public boolean hasSetFilter() {
        return this.filter.isValueSet();
    }


    /**
     * Set whether the data returned by this read operation should enter the BlockCache.
     *
     * @param cacheBlocks If true, the data read will be cached in the BlockCache
     */
    public void setCacheBlocks(boolean cacheBlocks) {
        this.cacheBlocks.setValue(cacheBlocks);
    }

    /**
     * Get the value of the CacheBlocks setting.
     *
     * @return CacheBlocks
     * @throws java.lang.IllegalStateException if this parameter is not configured
     */
    public boolean getCacheBlocks() {
        if (!this.cacheBlocks.isValueSet()) {
            throw new IllegalStateException("The value of cacheBlocks is not set.");
        }
        return this.cacheBlocks.getValue();
    }

    /**
     * Query whether CacheBlocks is set.
     *
     * @return If CacheBlocks is set, return true; otherwise, return false.
     */
    public boolean hasSetCacheBlock() {
        return this.cacheBlocks.isValueSet();
    }

    public String getStartColumn() {
        if (!this.startColumn.isValueSet()) {
            throw new IllegalStateException("The value of startColumn is not set.");
        }
        return startColumn.getValue();
    }

    public void setStartColumn(String startColumn) {
        this.startColumn.setValue(startColumn);
    }

    public boolean hasSetStartColumn() {
        return this.startColumn.isValueSet();
    }

    public String getEndColumn() {
        if (!this.endColumn.isValueSet()) {
            throw new IllegalStateException("The value of endColumn is not set.");
        }
        return endColumn.getValue();
    }

    public void setEndColumn(String endColumn) {
        this.endColumn.setValue(endColumn);
    }

    public boolean hasSetEndColumn() {
        return this.endColumn.isValueSet();
    }

    public void copyTo(RowQueryCriteria target) {
        target.tableName = tableName;
        target.columnsToGet.addAll(columnsToGet);
        if (timeRange.isValueSet()) {
            target.timeRange.setValue(timeRange.getValue());
        }

        if (maxVersions.isValueSet()) {
            target.maxVersions.setValue(maxVersions.getValue());
        }

        if (cacheBlocks.isValueSet()) {
            target.cacheBlocks.setValue(cacheBlocks.getValue());
        }

        if (filter.isValueSet()) {
            target.filter.setValue(filter.getValue());
        }

        if (startColumn.isValueSet()) {
            target.startColumn.setValue(startColumn.getValue());
        }

        if (endColumn.isValueSet()) {
            target.endColumn.setValue(endColumn.getValue());
        }
    }
}
