package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.CalculateHelper;
import com.alicloud.openservices.tablestore.core.utils.OptionalValue;

import java.util.ArrayList;
import java.util.List;

public class RowPutChange extends RowChange {

    /**
     * The collection of attribute columns for a row.
     */
    private List<Column> columnsToPut = new ArrayList<Column>();

    private OptionalValue<Long> timestamp = new OptionalValue<Long>("Timestamp");

    /**
     * Constructor.
     *
     * @param tableName The name of the table
     */
    public RowPutChange(String tableName) {
    	super(tableName);
    }

    /**
     * Constructor.
     *
     * @param tableName  The name of the table
     * @param primaryKey The primary key of the row
     */
    public RowPutChange(String tableName, PrimaryKey primaryKey) {
    	super(tableName, primaryKey);
    }

    /**
     * Constructor.
     * <p>Allows users to set a default timestamp; if the written column does not include a timestamp, this default timestamp will be used.</p>
     *
     * @param tableName  The name of the table
     * @param primaryKey The primary key of the row
     * @param ts         Default timestamp
     */
    public RowPutChange(String tableName, PrimaryKey primaryKey, long ts) {
    	super(tableName, primaryKey);
        this.timestamp.setValue(ts);
    }

    /**
     * Copy constructor
     *
     * @param toCopy
     */
    public RowPutChange(RowPutChange toCopy) {
        super(toCopy.getTableName(), toCopy.getPrimaryKey());
        if (toCopy.timestamp.isValueSet()) {
            timestamp.setValue(toCopy.timestamp.getValue());
        }

        columnsToPut.addAll(toCopy.columnsToPut);
    }

    /**
     * Write a new attribute column.
     *
     * @param column
     * @return this (for invocation chain)
     */
    public RowPutChange addColumn(Column column) {
        this.columnsToPut.add(column);
        return this;
    }

    /**
     * Write a new attribute column.
     * <p>If {@link #timestamp} has been set, then the default timestamp will be used.</p>
     *
     * @param name  the name of the attribute column
     * @param value the value of the attribute column
     * @return this (for invocation chain)
     */
    public RowPutChange addColumn(String name, ColumnValue value) {
        Column column = null;
        if (this.timestamp.isValueSet()) {
            column = new Column(name, value, this.timestamp.getValue());
        } else {
            column = new Column(name, value);
        }

        this.columnsToPut.add(column);
        return this;
    }

    /**
     * Write a new attribute column.
     *
     * @param name  the name of the attribute column
     * @param value the value of the attribute column
     * @param ts    the timestamp of the attribute column
     * @return this (for invocation chain)
     */
    public RowPutChange addColumn(String name, ColumnValue value, long ts) {
        this.columnsToPut.add(new Column(name, value, ts));
        return this;
    }

    /**
     * Write a new batch of attribute columns.
     * <p>The order of writing attribute columns is consistent with the order in the list.</p>
     *
     * @param columns List of attribute columns
     * @return this (for invocation chain)
     */
    public RowPutChange addColumns(List<Column> columns) {
        this.columnsToPut.addAll(columns);
        return this;
    }

    /**
     * Write a new batch of attribute columns.
     * <p>The order of writing attribute columns is consistent with the order in the array.</p>
     *
     * @param columns
     * @return this (for invocation chain)
     */
    public RowPutChange addColumns(Column[] columns) {
        for (Column column : columns) {
            this.columnsToPut.add(column);
        }
        return this;
    }

    /**
     * Get the list of all attribute columns to be written.
     *
     * @return List of attribute columns
     */
    public List<Column> getColumnsToPut() {
        return this.columnsToPut;
    }

    /**
     * Get the list of all attribute columns that have the same name as the specified one.
     *
     * @param name The name of the attribute column
     * @return If the corresponding attribute columns are found, return a list containing these elements; otherwise, return an empty list.
     */
    public List<Column> getColumnsToPut(String name) {
        List<Column> result = new ArrayList<Column>();

        for (Column col : columnsToPut) {
            if (col.getName().equals(name)) {
                result.add(col);
            }
        }
        return result;
    }

    @Override
    public int getDataSize() {
        int valueTotalSize = 0;
        for (Column col : columnsToPut) {
            valueTotalSize += col.getDataSize();
        }
        return getPrimaryKey().getDataSize() + valueTotalSize;
    }

    /**
     * Check if there is already a property column with the same name written, ignoring whether the timestamp and value are equal.
     *
     * @param name The property column name
     * @return Returns true if it exists, otherwise returns false
     */
    public boolean has(String name) {
        for (Column col : columnsToPut) {
            if (col.getName().equals(name)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check if there is an attribute column written with the same name and timestamp, ignoring whether the values are equal.
     *
     * @param name Attribute column name
     * @param ts   Attribute column timestamp
     * @return Returns true if it exists, otherwise returns false
     */
    public boolean has(String name, long ts) {
        for (Column col : columnsToPut) {
            if (col.getName().equals(name) && (col.hasSetTimestamp() && col.getTimestamp() == ts)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check if there are attribute columns written with the same name and value, ignoring whether the timestamps are equal.
     *
     * @param name  attribute column name
     * @param value attribute column value
     * @return returns true if exists, otherwise returns false
     */
    public boolean has(String name, ColumnValue value) {
        for (Column col : columnsToPut) {
            if (col.getName().equals(name) && col.getValue().equals(value)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check if there is an attribute column written with the same name, same timestamp, and same value.
     *
     * @param name  Attribute column name
     * @param ts    Attribute column timestamp
     * @param value Attribute column value
     * @return Returns true if it exists, otherwise returns false
     */
    public boolean has(String name, long ts, ColumnValue value) {
        for (Column col : columnsToPut) {
            if (col.getName().equals(name) && (col.hasSetTimestamp() && col.getTimestamp() == ts) &&
                    value.equals(col.getValue())) {
                return true;
            }
        }
        return false;
    }
}
