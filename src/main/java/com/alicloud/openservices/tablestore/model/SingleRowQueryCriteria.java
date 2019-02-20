package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

/**
 * 从TableStore表中查询单行数据所需的参数，可以支持以下几种读取行为:
 * <ul>
 * <li>读取某些列或所有列的某个特定版本</li>
 * <li>读取某些列或所有列的某个版本范围内的所有版本或最大的N个版本</li>
 * <li>读取某些列或所有列的最大的N个版本(N最小为 1,最大为MaxVersions)</li>
 * </ul>
 */
public class SingleRowQueryCriteria extends RowQueryCriteria implements IRow {

    private PrimaryKey primaryKey;

    /**
     * 用于行内流式读, 标记位置和状态信息.
     */
    private OptionalValue<byte[]> token = new OptionalValue<byte[]>("Token");

    /**
     * 构造一个在给定名称的表中查询的条件。
     *
     * @param tableName 查询的表名
     */
    public SingleRowQueryCriteria(String tableName) {
        super(tableName);
    }

    /**
     * 构造一个在给定名称的表中查询的条件。
     *
     * @param tableName 查询的表名
     * @param primaryKey 行的主键
     */
    public SingleRowQueryCriteria(String tableName, PrimaryKey primaryKey) {
        super(tableName);
        setPrimaryKey(primaryKey);
    }

    /**
     * 设置行的主键。
     *
     * @param primaryKey 行的主键。
     */
    public void setPrimaryKey(PrimaryKey primaryKey) {
        Preconditions.checkArgument(primaryKey != null && !primaryKey.isEmpty(), "The row's primary key should not be null or empty.");
        this.primaryKey = primaryKey;
    }

    public byte[] getToken() {
        if (!this.token.isValueSet()) {
            throw new IllegalStateException("The value of token is not set.");
        }
        return token.getValue();
    }

    public void setToken(byte[] token) {
        if (token != null) {
            this.token.setValue(token);
        }
    }

    public boolean hasSetToken() {
        return this.token.isValueSet();
    }

    @Override
    public int compareTo(IRow row) {
        return this.primaryKey.compareTo(row.getPrimaryKey());
    }

    @Override
    public PrimaryKey getPrimaryKey() {
        return this.primaryKey;
    }
}
