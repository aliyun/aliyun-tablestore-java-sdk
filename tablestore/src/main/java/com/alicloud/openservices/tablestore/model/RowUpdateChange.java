package com.alicloud.openservices.tablestore.model;

import java.util.ArrayList;
import java.util.List;

import com.alicloud.openservices.tablestore.core.utils.GsonUtils;
import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Pair;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

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
        INCREMENT,

        /**
         * Represents performing a json update operation on the latest version of this column.
         */
        JSON_SET,
        JSON_INSERT,
        JSON_REPLACE,
        JSON_REMOVE,
        JSON_ARRAY_APPEND,
        JSON_ARRAY_INSERT,
        JSON_ARRAY_REMOVE
    }

    // json update operand fields
    private static final String JU_PATH = "path";
    private static final String JU_VALUE = "value";
    private static final String JU_EXTENSION = "extension";
    private static final String JU_AUTO_CREATE_OBJECT = "autoCreateObject";
    private static final String JU_IF_ABSENT = "ifAbsent";

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
     * Perform a JSON_SET operation on the latest version of the specified column.
     * <p>Sets the value at the specified path in the JSON document. If the path already exists, the value is replaced;
     * if the path does not exist, the value is inserted.</p>
     *
     * @param name             the name of the attribute column
     * @param path             the MySQL-style JSON path starting with '$'
     * @param value            the JSON value for update, represented as a serialized string; ColumnType must be STRING
     * @param autoCreateObject whether to automatically create intermediate object nodes that do not exist along the path;
     *                         defaults to false if not specified.
     *                         Note: 1) only controls the behavior of intermediate nodes; leaf node behavior is determined
     *                         by the interface semantics; 2) only supports creating objects within object types, does not
     *                         support creating arrays or extending existing arrays
     * @return this for chain invocation
     */
    public RowUpdateChange jsonSet(String name, String path, ColumnValue value, boolean autoCreateObject) {
        JsonObject extension = new JsonObject();
        extension.addProperty(JU_AUTO_CREATE_OBJECT, autoCreateObject);
        value = buildJsonUpdateOperand(path, value, extension);
        this.columnsToUpdate.add(new Pair<Column, Type>(new Column(name, value), Type.JSON_SET));
        return this;
    }

    public RowUpdateChange jsonSet(String name, String path, ColumnValue value) {
        return jsonSet(name, path, value, false);
    }

    /**
     * Perform a JSON_INSERT operation on the latest version of the specified column.
     * <p>Inserts the value at the specified path in the JSON document only if the path does not already exist.
     * If the path already exists, an error will be reported.</p>
     *
     * @param name             the name of the attribute column
     * @param path             the MySQL-style JSON path starting with '$'
     * @param value            the JSON value for update, represented as a serialized string; ColumnType must be STRING
     * @param autoCreateObject whether to automatically create intermediate object nodes that do not exist along the path;
     *                         defaults to false if not specified.
     *                         Note: 1) only controls the behavior of intermediate nodes; leaf node behavior is determined
     *                         by the interface semantics; 2) only supports creating objects within object types, does not
     *                         support creating arrays or extending existing arrays
     * @return this for chain invocation
     */
    public RowUpdateChange jsonInsert(String name, String path, ColumnValue value, boolean autoCreateObject) {
        JsonObject extension = new JsonObject();
        extension.addProperty(JU_AUTO_CREATE_OBJECT, autoCreateObject);
        value = buildJsonUpdateOperand(path, value, extension);
        this.columnsToUpdate.add(new Pair<Column, Type>(new Column(name, value), Type.JSON_INSERT));
        return this;
    }

    public RowUpdateChange jsonInsert(String name, String path, ColumnValue value) {
        return jsonInsert(name, path, value, false);
    }

    /**
     * Perform a JSON_REPLACE operation on the latest version of the specified column.
     * <p>Replaces the value at the specified path in the JSON document only if the path already exists.
     * If the path does not exist, an error will be reported.</p>
     *
     * @param name             the name of the attribute column
     * @param path             the MySQL-style JSON path starting with '$'
     * @param value            the JSON value for update, represented as a serialized string; ColumnType must be STRING
     * @param autoCreateObject whether to automatically create intermediate object nodes that do not exist along the path;
     *                         defaults to false if not specified.
     *                         Note: 1) only controls the behavior of intermediate nodes; leaf node behavior is determined
     *                         by the interface semantics; 2) only supports creating objects within object types, does not
     *                         support creating arrays or extending existing arrays
     * @return this for chain invocation
     */
    public RowUpdateChange jsonReplace(String name, String path, ColumnValue value, boolean autoCreateObject) {
        JsonObject extension = new JsonObject();
        extension.addProperty(JU_AUTO_CREATE_OBJECT, autoCreateObject);
        value = buildJsonUpdateOperand(path, value, extension);
        this.columnsToUpdate.add(new Pair<Column, Type>(new Column(name, value), Type.JSON_REPLACE));
        return this;
    }

    public RowUpdateChange jsonReplace(String name, String path, ColumnValue value) {
        return jsonReplace(name, path, value, false);
    }

    /**
     * Perform a JSON_REMOVE operation on the latest version of the specified column.
     * <p>Removes the element at the specified path from the JSON document.</p>
     *
     * @param name the name of the attribute column
     * @param path the MySQL-style JSON path starting with '$'
     * @return this for chain invocation
     */
    public RowUpdateChange jsonRemove(String name, String path) {
        ColumnValue value = buildJsonUpdateOperand(path, null, null);
        this.columnsToUpdate.add(new Pair<Column, Type>(new Column(name, value), Type.JSON_REMOVE));
        return this;
    }

    /**
     * Perform a JSON_ARRAY_APPEND operation on the latest version of the specified column.
     * <p>Appends the value to the end of the JSON array at the specified path.</p>
     *
     * @param name     the name of the attribute column
     * @param path     the MySQL-style JSON path starting with '$'
     * @param value    the JSON value for update, represented as a serialized string; ColumnType must be STRING
     * @param ifAbsent if true, the value is appended only when no equal element already exists in the array;
     *                 if false, the value is always appended regardless of existing elements.
     *                 Defaults to false if not specified
     * @return this for chain invocation
     */
    public RowUpdateChange jsonArrayAppend(String name, String path, ColumnValue value, boolean ifAbsent) {
        JsonObject extension = new JsonObject();
        extension.addProperty(JU_IF_ABSENT, ifAbsent);
        value = buildJsonUpdateOperand(path, value, extension);
        this.columnsToUpdate.add(new Pair<Column, Type>(new Column(name, value), Type.JSON_ARRAY_APPEND));
        return this;
    }

    public RowUpdateChange jsonArrayAppend(String name, String path, ColumnValue value) {
        return jsonArrayAppend(name, path, value, false);
    }

    /**
     * Perform a JSON_ARRAY_INSERT operation on the latest version of the specified column.
     * <p>Inserts the value at the specified position in the JSON array at the given path.</p>
     *
     * @param name  the name of the attribute column
     * @param path  the MySQL-style JSON path starting with '$'
     * @param value the JSON value for update, represented as a serialized string; ColumnType must be STRING
     * @return this for chain invocation
     */
    public RowUpdateChange jsonArrayInsert(String name, String path, ColumnValue value) {
        value = buildJsonUpdateOperand(path, value, null);
        this.columnsToUpdate.add(new Pair<Column, Type>(new Column(name, value), Type.JSON_ARRAY_INSERT));
        return this;
    }

    /**
     * Perform a JSON_ARRAY_REMOVE operation on the latest version of the specified column.
     * <p>Removes all elements from the JSON array at the specified path that are equal to the given value.</p>
     *
     * @param name  the name of the attribute column
     * @param path  the MySQL-style JSON path starting with '$'
     * @param value the JSON value to match for removal, represented as a serialized string; ColumnType must be STRING.
     *              Elements in the array that are equal to the JSON object parsed from this string will be removed.
     * @return this for chain invocation
     */
    public RowUpdateChange jsonArrayRemove(String name, String path, ColumnValue value) {
        value = buildJsonUpdateOperand(path, value, null);
        this.columnsToUpdate.add(new Pair<Column, Type>(new Column(name, value), Type.JSON_ARRAY_REMOVE));
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

    private static ColumnValue buildJsonUpdateOperand(String path, ColumnValue value, JsonObject extension) {
        JsonObject obj = new JsonObject();
        obj.addProperty(JU_PATH, path);
        if (value != null) {
            obj.add(JU_VALUE, JsonParser.parseString(value.asString()));
        }
        if (extension != null) {
            obj.add(JU_EXTENSION, extension);
        }
        return new ColumnValue(GsonUtils.toJson(obj), ColumnType.STRING);
    }
}
