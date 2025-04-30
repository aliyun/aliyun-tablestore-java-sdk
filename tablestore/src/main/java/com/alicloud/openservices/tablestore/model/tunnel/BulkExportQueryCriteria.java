package com.alicloud.openservices.tablestore.model.tunnel;

import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.filter.Filter;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class BulkExportQueryCriteria {

    /**
     * The name of the table being queried.
     */
    private String tableName;

    /**
     * The primary key values of the left boundary and the right boundary.
     */

    private PrimaryKey inclusiveStartPrimaryKey;

    private PrimaryKey exclusiveEndPrimaryKey;

    /**
     * The list of property column names to read. If it is empty, it means reading all columns of the row.
     */
    private Set<String> columnsToGet = new HashSet<String>();

    /**
     * The Filter used for this query.
     */
    private OptionalValue<Filter> filter = new OptionalValue<Filter>("Filter");

    /**
     *  Data type for row information
     */
    private DataBlockType dataBlockType = DataBlockType.DBT_SIMPLE_ROW_MATRIX;

    /**
     * Constructs a query condition for a table with the given name.
     * @param tableName
     *          The name of the table to query.
     */
    public BulkExportQueryCriteria(String tableName){
        Preconditions.checkArgument(tableName != null && !tableName.isEmpty(), "The name of table should not be null or empty.");
        this.tableName = tableName;
    }

    public void setDataBlockType(DataBlockType dataBlockType){
        Preconditions.checkArgument(dataBlockType == DataBlockType.DBT_PLAIN_BUFFER || dataBlockType == DataBlockType.DBT_SIMPLE_ROW_MATRIX,
                "Unknown DataBlockType.");
        this.dataBlockType = dataBlockType;
    }

    /**
     *  Data type for row information
     */
    public DataBlockType getDataBlockType(){
        return dataBlockType;
    }

    /**
     * Get the primary key value of the left boundary for the range query.
     * @return The primary key value of the left boundary for the range query.
     */
    public PrimaryKey getInclusiveStartPrimaryKey() {
        return inclusiveStartPrimaryKey;
    }

    /**
     * Range queries require the user to specify a range for the primary key. This range is a half-open interval that is closed on the left and open on the right, with inclusiveStartPrimaryKey being the left boundary of the interval.
     * If direction is FORWARD, then inclusiveStartPrimaryKey must be less than exclusiveEndPrimaryKey.
     * If direction is BACKWARD, then inclusiveStartPrimaryKey must be greater than exclusiveEndPrimaryKey.
     * inclusiveStartPrimaryKey must include all primary key columns defined in the table. The values of the columns can be defined as {@link PrimaryKeyValue#INF_MIN} or {@link PrimaryKeyValue#INF_MAX} to represent the entire range of possible values for that column.
     * @param inclusiveStartPrimaryKey The primary key value for the left boundary of the range query.
     */
    public void setInclusiveStartPrimaryKey(PrimaryKey inclusiveStartPrimaryKey) {
        Preconditions.checkArgument(inclusiveStartPrimaryKey != null && !inclusiveStartPrimaryKey.isEmpty(), "The inclusive start primary key should not be null.");
        this.inclusiveStartPrimaryKey = inclusiveStartPrimaryKey;
    }

    /**
     * Get the primary key value of the right boundary for the range query.
     * @return The primary key value of the right boundary for the range query.
     */
    public PrimaryKey getExclusiveEndPrimaryKey() {
        return exclusiveEndPrimaryKey;
    }

    /**
     * A range query requires the user to specify a range for a primary key. This range is a half-open interval that is closed on the left and open on the right, where exclusiveEndPrimaryKey is the right boundary of this interval.
     * If direction is FORWARD, then exclusiveEndPrimaryKey must be greater than inclusiveStartPrimaryKey.
     * If direction is BACKWARD, then exclusiveEndPrimaryKey must be less than inclusiveStartPrimaryKey.
     * exclusiveEndPrimaryKey must include all primary key columns defined in the table. The values of the columns can be defined as {@link PrimaryKeyValue#INF_MIN} or {@link PrimaryKeyValue#INF_MAX} to represent the entire range of values for that column.
     * @param exclusiveEndPrimaryKey The primary key value for the right boundary of the range query.
     */
    public void setExclusiveEndPrimaryKey(PrimaryKey exclusiveEndPrimaryKey) {
        Preconditions.checkArgument(exclusiveEndPrimaryKey != null && !exclusiveEndPrimaryKey.isEmpty(), "The exclusive end primary key should not be null.");
        this.exclusiveEndPrimaryKey = exclusiveEndPrimaryKey;
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
     * Add the columns to be read.
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
     * Check if the Filter is set.
     *
     * @return Returns true if the Filter is set, otherwise returns false.
     */
    public boolean hasSetFilter() {
        return this.filter.isValueSet();
    }

    public void copyTo(BulkExportQueryCriteria target) {
        target.setTableName(tableName);
        target.columnsToGet.addAll(columnsToGet);
        target.setDataBlockType(dataBlockType);
        target.setInclusiveStartPrimaryKey(inclusiveStartPrimaryKey);
        target.setExclusiveEndPrimaryKey(exclusiveEndPrimaryKey);
        if (filter.isValueSet()) {
            target.filter.setValue(filter.getValue());
        }
    }
}
