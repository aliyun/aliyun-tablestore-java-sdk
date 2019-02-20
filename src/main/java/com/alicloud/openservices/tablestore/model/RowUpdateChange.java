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
         * 代表写入该Column的某个特定版本的值。
         */
        PUT,

        /**
         * 代表删除该Column的某个特定版本，版本号的时间戳等于{@link Column#timestamp}。
         */
        DELETE,

        /**
         * 代表删除该Column的所有版本的值。
         */
        DELETE_ALL,

        /**
         * 代表对该column的最新版本执行原子加。
         */
        INCREMENT
    }

    /**
     * 所有要更新的属性列。
     * <p>若类型为{@link Type#PUT}，则代表写入一个属性列。</p>
     * <p>若类型为{@link Type#DELETE}，则代表删除一个属性列的某个特定版本，对应的Column中的value无效。</p>
     * <p>若类型为{@link Type#DELETE_ALL}，则代表删除一个属性列的所有版本，对应的Column中的value和timestamp均无效。</p>
     */
    private List<Pair<Column, Type>> columnsToUpdate = new ArrayList<Pair<Column, Type>>();

    private OptionalValue<Long> timestamp = new OptionalValue<Long>("Timestamp");

    /**
     * 构造函数。
     * <p>表的名称不能为null或者为空。</p>
     *
     * @param tableName  表的名称
     */
    public RowUpdateChange(String tableName) {
    	super(tableName);
    }

    /**
     * 构造函数。
     * <p>表的名称不能为null或者为空。</p>
     * <p>行的主键不能为null或者为空。</p>
     *
     * @param tableName  表的名称
     * @param primaryKey 行的主键
     */
    public RowUpdateChange(String tableName, PrimaryKey primaryKey) {
    	super(tableName, primaryKey);
    }

    /**
     * 构造函数。
     * <p>允许用户设置一个默认的时间戳，若写入的列没有带时间戳，则会使用该默认时间戳。</p>
     * <p>默认的时间戳与删除动作无关，。</p>
     * <p>表的名称不能为null或者为空。</p>
     * <p>行的主键不能为null或者为空。</p>
     *
     * @param tableName  表的名称
     * @param primaryKey 行的主键
     * @param ts         默认时间戳
     */
    public RowUpdateChange(String tableName, PrimaryKey primaryKey, long ts) {
    	super(tableName, primaryKey);
        this.timestamp.setValue(ts);
    }

    /**
     * 拷贝构造函数
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
     * 新写入一个属性列。
     *
     * @param column
     * @return this (for invocation chain)
     */
    public RowUpdateChange put(Column column) {
        this.columnsToUpdate.add(new Pair<Column, Type>(column, Type.PUT));
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
     * 新写入一个属性列。
     *
     * @param name  属性列的名称
     * @param value 属性列的值
     * @param ts    属性列的时间戳
     * @return this (for invocation chain)
     */
    public RowUpdateChange put(String name, ColumnValue value, long ts) {
        this.columnsToUpdate.add(new Pair<Column, Type>(new Column(name, value, ts), Type.PUT));
        return this;
    }

    /**
     * 新写入一批属性列。
     * <p>属性列的写入顺序与列表中的顺序一致。</p>
     *
     * @param columns 属性列列表
     * @return this (for invocation chain)
     */
    public RowUpdateChange put(List<Column> columns) {
        for (Column col : columns) {
            put(col);
        }
        return this;
    }

    /**
     * 删除某一属性列的特定版本。
     *
     * @param name 属性列的名称
     * @param ts   属性列的时间戳
     * @return this for chain invocation
     */
    public RowUpdateChange deleteColumn(String name, long ts) {
        this.columnsToUpdate.add(new Pair<Column, Type>(new Column(name, ColumnValue.INTERNAL_NULL_VALUE, ts), Type.DELETE));
        return this;
    }

    /**
     * 删除某一属性列的所有版本。
     *
     * @param name 属性列的名称
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
     * 获取所有要更新的列。
     * <p>若类型为{@link Type#PUT}，则代表写入一个属性列，对应的Column即要写入的属性列。</p>
     * <p>若类型为{@link Type#DELETE}，则代表删除一个属性列的某个特定版本，对应的Column中的value无效。</p>
     * <p>若类型为{@link Type#DELETE_ALL}，则代表删除一个属性列的所有版本，对应的Column中的value和timestamp均无效。</p>
     *
     * @return 所有要更新的列
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
