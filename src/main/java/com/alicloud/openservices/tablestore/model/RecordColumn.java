package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

public class RecordColumn implements Jsonizable {

    public enum ColumnType {
        /**
         * PUT类型
         * 表示写入该列
         */
        PUT,

        /**
         * DELETE_ONE_VERSION类型
         * 表示删除该列的某一版本
         */
        DELETE_ONE_VERSION,

        /**
         * DELETE_ALL_VERSION类型
         * 表示删除该列的全部版本
         */
        DELETE_ALL_VERSION
    }

    /**
     * 列的相关数据，包含列名、列值和时间戳(版本号)
     */
    private Column column;

    /**
     * 列的类型
     */
    private ColumnType columnType;

    public RecordColumn(Column column, ColumnType columnType) {
        this.column = column;
        this.columnType = columnType;
    }

    /**
     * 获取列的相关数据
     * @return 列的相关数据
     */
    public Column getColumn() {
        return this.column;
    }

    /**
     * 获取列的类型
     * @return 列的类型
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
