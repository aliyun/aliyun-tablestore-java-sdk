package com.alicloud.openservices.tablestore.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.alicloud.openservices.tablestore.core.utils.CalculateHelper;
import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Pair;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class RowUpdateChange extends RowChange {
    public static enum Type {
        /**
         * Represents the value of a specific version written to this Column.
         */
        PUT,

        /**
         * Represents deleting a specific version of this Column, where the timestamp of the version equals {@link Column#timestamp}.
         */
        DELETE,

        /**
         * Represents deleting all version values of this Column.
         */
        DELETE_ALL,

        /**
         * Represents performing an atomic add on the latest version of this column.
         */
        INCREMENT
    }

    /**
     * All the attribute columns to be updated.
     * <p>If the type is {@link Type#PUT}, it means writing an attribute column.</p>
     * <p>If the type is {@link Type#DELETE}, it means deleting a specific version of an attribute column; the value in the corresponding Column is invalid.</p>
     * <p>If the type is {@link Type#DELETE_ALL}, it means deleting all versions of an attribute column; both the value and timestamp in the corresponding Column are invalid.</p>
     */
    private List<Pair<Column, Type>> columnsToUpdate = new ArrayList<Pair<Column, Type>>();

    private OptionalValue<Long> timestamp = new OptionalValue<Long>("Timestamp");

    /**
     * Constructor.
     * <p>The table name cannot be null or empty.</p>
     *
     * @param tableName The name of the table
     */
    public RowUpdateChange(String tableName) {
    	super(tableName);
    }

    /**
     * Constructor.
     * <p>The table name cannot be null or empty.</p>
     * <p>The primary key of the row cannot be null or empty.</p>
     *
     * @param tableName  The name of the table
     * @param primaryKey The primary key of the row
     */
    public RowUpdateChange(String tableName, PrimaryKey primaryKey) {
    	super(tableName, primaryKey);
    }

    /**
     * Constructor.
     * <p>Allows users to set a default timestamp; if the written column does not include a timestamp, this default timestamp will be used.</p>
     * <p>The default timestamp is irrelevant to delete actions.</p>
     * <p>The table name cannot be null or empty.</p>
     * <p>The primary key of the row cannot be null or empty.</p>
     *
     * @param tableName  The name of the table
     * @param primaryKey The primary key of the row
     * @param ts         Default timestamp
     */
    public RowUpdateChange(String tableName, PrimaryKey primaryKey, long ts) {
    	super(tableName, primaryKey);
        this.timestamp.setValue(ts);
    }

    /**
     * Copy constructor
     *
     * @param toCopy
     */
    public RowUpdateChange(RowUpdateChange toCopy) {
        super(toCopy.getTableName(), toCopy.getPrimaryKey());
        if (toCopy.timestamp.isValueSet()) {
            timestamp.setValue(toCopy.timestamp.getValue());
        }

        columnsToUpdate.addAll(toCopy.columnsToUpdate);
    }

    /**
     * Write a new attribute column.
     *
     * @param column
     * @return this (for invocation chain)
     */
    public RowUpdateChange put(Column column) {
        this.columnsToUpdate.add(new Pair<Column, Type>(column, Type.PUT));
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
    public RowUpdateChange put(String name, ColumnValue value) {
        Column column = null;
        if (this.timestamp.isValueSet()) {
            column = new Column(name, value, this.timestamp.getValue());
        } else {
            column = new Column(name, value);
        }

        this.columnsToUpdate.add(new Pair<Column, Type>(column, Type.PUT));
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
    public RowUpdateChange put(String name, ColumnValue value, long ts) {
        this.columnsToUpdate.add(new Pair<Column, Type>(new Column(name, value, ts), Type.PUT));
        return this;
    }

    /**
     * Write a new batch of attribute columns.
     * <p>The order of writing attribute columns is consistent with the order in the list.</p>
     *
     * @param columns List of attribute columns
     * @return this (for invocation chain)
     */
    public RowUpdateChange put(List<Column> columns) {
        for (Column col : columns) {
            put(col);
        }
        return this;
    }

    /**
     * Delete a specific version of an attribute column.
     *
     * @param name the name of the attribute column
     * @param ts   the timestamp of the attribute column
     * @return this for chain invocation
     */
    public RowUpdateChange deleteColumn(String name, long ts) {
        this.columnsToUpdate.add(new Pair<Column, Type>(new Column(name, ColumnValue.INTERNAL_NULL_VALUE, ts), Type.DELETE));
        return this;
    }

    /**
     * Delete all versions of a certain property column.
     *
     * @param name the name of the property column
     * @return this for chain invocation
     */
    public RowUpdateChange deleteColumns(String name) {
        this.columnsToUpdate.add(new Pair<Column, Type>(new Column(name, ColumnValue.INTERNAL_NULL_VALUE), Type.DELETE_ALL));
        return this;
    }

    public RowUpdateChange increment(Column column) {
        this.columnsToUpdate.add(new Pair<Column, Type>(column, Type.INCREMENT));
        return this;
    }

    /**
     * Get all the columns to be updated.
     * <p>If the type is {@link Type#PUT}, it indicates writing a property column, and the corresponding Column is the property column to be written.</p>
     * <p>If the type is {@link Type#DELETE}, it indicates deleting a specific version of a property column, and the value in the corresponding Column is invalid.</p>
     * <p>If the type is {@link Type#DELETE_ALL}, it indicates deleting all versions of a property column, and both the value and timestamp in the corresponding Column are invalid.</p>
     *
     * @return all the columns to be updated
     */
    public List<Pair<Column, Type>> getColumnsToUpdate() {
        return this.columnsToUpdate;
    }

    @Override
    public int getDataSize() {
        int valueTotalSize = 0;
        for (Pair<Column, Type> col : columnsToUpdate) {
            valueTotalSize += col.getFirst().getDataSize();
        }
        return getPrimaryKey().getDataSize() + valueTotalSize;
    }
}
