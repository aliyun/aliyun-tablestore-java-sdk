package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

public class RecordColumn implements Jsonizable {

    public enum ColumnType {
        /**
         * PUT type
         * Indicates writing to this column
         */
        PUT,

        /**
         * DELETE_ONE_VERSION type
         * Indicates deleting a specific version of this column
         */
        DELETE_ONE_VERSION,

        /**
         * DELETE_ALL_VERSION type
         * Indicates deleting all versions of the column
         */
        DELETE_ALL_VERSION
    }

    /**
     * Column-related data, including column name, column value, and timestamp (version number).
     */
    private Column column;

    /**
     * The type of column
     */
    private ColumnType columnType;

    public RecordColumn(Column column, ColumnType columnType) {
        this.column = column;
        this.columnType = columnType;
    }

    /**
     * Get the relevant data of the column
     * @return Relevant data of the column
     */
    public Column getColumn() {
        return this.column;
    }

    /**
     * Set the relevant data for the column
     */
    public void setColumn(Column column) {
        this.column = column;
    }

    /**
     * Get the type of the column
     * @return the type of the column
     */
    public ColumnType getColumnType() {
        return columnType;
    }

    @Override
    public String toString() {
        return column.toString() + ",ColumnType:" + columnType;
    }

    @Override
    public int hashCode() {
        return column.hashCode() ^ columnType.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof RecordColumn)) {
            return false;
        }

        RecordColumn col = (RecordColumn) o;
        return this.column.equals(col.getColumn()) && this.columnType.equals(col.getColumnType());
    }

    @Override
    public String jsonize() {
        StringBuilder sb = new StringBuilder();
        jsonize(sb, "\n  ");
        return sb.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        sb.append("{\"ColumnType\": \"");
        sb.append(columnType.toString());
        sb.append("\", \"Column\": ");
        column.jsonize(sb, newline + "  ");
        sb.append("}");
    }
}
