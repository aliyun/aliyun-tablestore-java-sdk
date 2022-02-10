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
     * 查询的表的名称。
     */
    private String tableName;

    /**
     * 左边界的主键值 和 右边界的主键值。
     */

    private PrimaryKey inclusiveStartPrimaryKey;

    private PrimaryKey exclusiveEndPrimaryKey;

    /**
     * 要读取的属性列名列表，若为空，则代表读取该行所有的列。
     */
    private Set<String> columnsToGet = new HashSet<String>();

    /**
     * 本次查询使用的Filter。
     */
    private OptionalValue<Filter> filter = new OptionalValue<Filter>("Filter");

    /**
     *  行信息的数据类型
     */
    private DataBlockType dataBlockType = DataBlockType.DBT_SIMPLE_ROW_MATRIX;

    /**
     * 构造一个在给定名称的表中查询的条件。
     * @param tableName
     *          查询的表名。
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
     *  行信息的数据类型
     */
    public DataBlockType getDataBlockType(){
        return dataBlockType;
    }

    /**
     * 获取范围查询的左边界的主键值。
     * @return 范围查询的左边界的主键值。
     */
    public PrimaryKey getInclusiveStartPrimaryKey() {
        return inclusiveStartPrimaryKey;
    }

    /**
     * 范围查询需要用户指定一个主键的范围，该范围是一个左闭右开的区间，inclusiveStartPrimaryKey为该区间的左边界。
     * 若direction为FORWARD，则inclusiveStartPrimaryKey必须小于exclusiveEndPrimaryKey。
     * 若direction为BACKWARD，则inclusiveStartPrimaryKey必须大于exclusiveEndPrimaryKey。
     * inclusiveStartPrimaryKey必须包含表中定义的所有主键列，列的值可以定义{@link PrimaryKeyValue#INF_MIN}或者{@link PrimaryKeyValue#INF_MAX}用于表示该列的所有取值范围。
     * @param inclusiveStartPrimaryKey 范围查询的左边界的主键值。
     */
    public void setInclusiveStartPrimaryKey(PrimaryKey inclusiveStartPrimaryKey) {
        Preconditions.checkArgument(inclusiveStartPrimaryKey != null && !inclusiveStartPrimaryKey.isEmpty(), "The inclusive start primary key should not be null.");
        this.inclusiveStartPrimaryKey = inclusiveStartPrimaryKey;
    }

    /**
     * 获取范围查询的右边界的主键值。
     * @return 范围查询的右边界的主键值。
     */
    public PrimaryKey getExclusiveEndPrimaryKey() {
        return exclusiveEndPrimaryKey;
    }

    /**
     * 范围查询需要用户指定一个主键的范围，该范围是一个左闭右开的区间，exclusiveEndPrimaryKey为该区间的右边界。
     * 若direction为FORWARD，则exclusiveEndPrimaryKey必须大于inclusiveStartPrimaryKey。
     * 若direction为BACKWARD，则exclusiveEndPrimaryKey必须小于inclusiveStartPrimaryKey。
     * exclusiveEndPrimaryKey必须包含表中定义的所有主键列，列的值可以定义{@link PrimaryKeyValue#INF_MIN}或者{@link PrimaryKeyValue#INF_MAX}用于表示该列的所有取值范围。
     * @param exclusiveEndPrimaryKey 范围查询的右边界的主键值。
     */
    public void setExclusiveEndPrimaryKey(PrimaryKey exclusiveEndPrimaryKey) {
        Preconditions.checkArgument(exclusiveEndPrimaryKey != null && !exclusiveEndPrimaryKey.isEmpty(), "The exclusive end primary key should not be null.");
        this.exclusiveEndPrimaryKey = exclusiveEndPrimaryKey;
    }

    /**
     * 设置查询的表名。
     *
     * @param tableName 表的名称。
     */
    public void setTableName(String tableName) {
        Preconditions.checkArgument(
                tableName != null && !tableName.isEmpty(),
                "The name of table should not be null or empty.");
        this.tableName = tableName;
    }

    /**
     * 返回查询的表名。
     *
     * @return 表的名称
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * 返回要读取的列的名称列表（只读）。
     *
     * @return 列的名称的列表（只读）。
     */
    public Set<String> getColumnsToGet() {
        return Collections.unmodifiableSet(columnsToGet);
    }

    /**
     * 添加要读取的列。
     *
     * @param columnName 要返回列的名称。
     */
    public void addColumnsToGet(String columnName) {
        Preconditions.checkArgument(columnName != null && !columnName.isEmpty(), "Column's name should not be null or empty.");
        this.columnsToGet.add(columnName);
    }

    /**
     * 添加要读取的列。
     *
     * @param columnNames 要返回列的名称。
     */
    public void addColumnsToGet(String[] columnNames) {
        Preconditions.checkNotNull(columnNames, "columnNames should not be null.");
        for (int i = 0; i < columnNames.length; ++i) {
            addColumnsToGet(columnNames[i]);
        }
    }

    /**
     * 添加要读取的列。
     *
     * @param columnsToGet
     */
    public void addColumnsToGet(Collection<String> columnsToGet) {
        this.columnsToGet.addAll(columnsToGet);
    }

    /**
     * 将设置过的要读取的列的名称列表清空。
     */
    public void clearColumnsToGet() {
        this.columnsToGet.clear();
    }

    /**
     * 返回要读取的列的个数。
     *
     * @return 要读取的列的个数。
     */
    public int numColumnsToGet() {
        return this.columnsToGet.size();
    }

    /**
     * 设置本次查询使用的Filter。
     *
     * @param filter
     */
    public void setFilter(Filter filter) {
        Preconditions.checkNotNull(filter, "The filter should not be null");
        this.filter.setValue(filter);
    }

    /**
     * 获取本次查询使用的Filter。
     *
     * @return Filter
     * @throws java.lang.IllegalStateException 若没有设置Filter
     */
    public Filter getFilter() {
        if (!this.filter.isValueSet()) {
            throw new IllegalStateException("The value of filter is not set.");
        }
        return this.filter.getValue();
    }

    /**
     * 查询是否设置了Filter。
     *
     * @return 若设置了Filter，则返回true，否则返回false。
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
