package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.CalculateHelper;
import com.alicloud.openservices.tablestore.core.utils.OptionalValue;

import java.util.ArrayList;
import java.util.List;

public class RowPutChange extends RowChange {

    /**
     * 行的属性列集合。
     */
    private List<Column> columnsToPut = new ArrayList<Column>();

    private OptionalValue<Long> timestamp = new OptionalValue<Long>("Timestamp");

    /**
     * 构造函数。
     *
     * @param tableName  表的名称
     */
    public RowPutChange(String tableName) {
    	super(tableName);
    }

    /**
     * 构造函数。
     *
     * @param tableName  表的名称
     * @param primaryKey 行的主键
     */
    public RowPutChange(String tableName, PrimaryKey primaryKey) {
    	super(tableName, primaryKey);
    }

    /**
     * 构造函数。
     * <p>允许用户设置一个默认的时间戳，若写入的列没有带时间戳，则会使用该默认时间戳。</p>
     *
     * @param tableName  表的名称
     * @param primaryKey 行的主键
     * @param ts         默认时间戳
     */
    public RowPutChange(String tableName, PrimaryKey primaryKey, long ts) {
    	super(tableName, primaryKey);
        this.timestamp.setValue(ts);
    }

    /**
     * 拷贝构造函数
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
     * 新写入一个属性列。
     *
     * @param column
     * @return this (for invocation chain)
     */
    public RowPutChange addColumn(Column column) {
        this.columnsToPut.add(column);
        return this;
    }

    /**
     * 新写入一个属性列。
     * <p>若设置过{@link #timestamp}，则使用该默认的时间戳。</p>
     *
     * @param name  属性列的名称
     * @param value 属性列的值
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
     * 新写入一个属性列。
     *
     * @param name  属性列的名称
     * @param value 属性列的值
     * @param ts    属性列的时间戳
     * @return this (for invocation chain)
     */
    public RowPutChange addColumn(String name, ColumnValue value, long ts) {
        this.columnsToPut.add(new Column(name, value, ts));
        return this;
    }

    /**
     * 新写入一批属性列。
     * <p>属性列的写入顺序与列表中的顺序一致。</p>
     *
     * @param columns 属性列列表
     * @return this (for invocation chain)
     */
    public RowPutChange addColumns(List<Column> columns) {
        this.columnsToPut.addAll(columns);
        return this;
    }

    /**
     * 新写入一批属性列。
     * <p>属性列的写入顺序与数组中的顺序一致。</p>
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
     * 获取所有要写入的属性列列表。
     *
     * @return 属性列列表
     */
    public List<Column> getColumnsToPut() {
        return this.columnsToPut;
    }

    /**
     * 获取名称与指定名称相同的所有属性列的列表。
     *
     * @param name 属性列名称
     * @return 若找到对应的属性列，则返回包含这些元素的列表，否则返回一个空列表。
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
     * 检查是否已经有相同名称的属性列写入，忽略时间戳和值是否相等。
     *
     * @param name 属性列名称
     * @return 若有返回true，否则返回false
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
     * 检查是否有相同名称和相同时间戳的属性列写入，忽略值是否相等。
     *
     * @param name 属性列名称
     * @param ts   属性列时间戳
     * @return 若有返回true，否则返回false
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
     * 检查是否有相同名称和相同值的属性列写入，忽略时间戳是否相等。
     *
     * @param name  属性列名称
     * @param value 属性列值
     * @return 若有返回true，否则返回false
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
     * 检查是否有相同名称、相同时间戳并且相同值的属性列写入。
     *
     * @param name  属性列名称
     * @param ts    属性列时间戳
     * @param value 属性列值
     * @return 若有返回true，否则返回false
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
