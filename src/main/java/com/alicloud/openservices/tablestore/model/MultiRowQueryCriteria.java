package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * 从TableStore表中查询多行数据所需的参数，可以支持以下几种读取行为:
 * <ul>
 * <li>读取某些列或所有列的某个特定版本</li>
 * <li>读取某些列或所有列的某个版本范围内的所有版本或最大的N个版本</li>
 * <li>读取某些列或所有列的最大的N个版本(N最小为 1,最大为MaxVersions)</li>
 * </ul>
 * <p>注意：读取参数不能为每行单独设置，多行必须使用相同的查询参数。</p>
 */
public class MultiRowQueryCriteria extends RowQueryCriteria {

    private List<PrimaryKey> rowKeys;
    private List<byte[]> tokens;

    public MultiRowQueryCriteria(String tableName) {
        super(tableName);
        rowKeys = new ArrayList<PrimaryKey>();
        tokens = new ArrayList<byte[]>();
    }

    /**
     * 向多行查询条件中插入要查询的行的主键。
     *
     * @param primaryKey 要查询的行的主键。
     * @return this (for invocation chain)
     */
    public MultiRowQueryCriteria addRow(PrimaryKey primaryKey) {
        Preconditions.checkArgument(primaryKey != null && !primaryKey.isEmpty(), "The primary key added should not be null.");
        this.rowKeys.add(primaryKey);
        this.tokens.add(new byte[0]);
        return this;
    }

    /**
     * 向多行查询条件中插入要查询的行的主键。
     *
     * @param primaryKey 要查询的行的主键。
     * @return this (for invocation chain)
     */
    public MultiRowQueryCriteria addRow(PrimaryKey primaryKey, byte[] token) {
        Preconditions.checkArgument(primaryKey != null && !primaryKey.isEmpty(), "The primary key added should not be null.");
        this.rowKeys.add(primaryKey);
        this.tokens.add(token);
        return this;
    }

    /**
     * 获取该表中所要要查询的行的主键。
     *
     * @return 所有行的主键。
     */
    public List<PrimaryKey> getRowKeys() {
        return rowKeys;
    }

    /**
     * 设置该表中所有要查询的行的主键。
     *
     * @param primaryKeys 所有行的主键。
     */
    public void setRowKeys(List<PrimaryKey> primaryKeys) {
        Preconditions.checkArgument(primaryKeys != null && !primaryKeys.isEmpty(), "The rows to get should not be null or empty.");
        clear();
        rowKeys.addAll(primaryKeys);
        for (int i = 0; i < primaryKeys.size(); i++) {
            tokens.add(new byte[0]);
        }
    }

    public List<byte[]> getTokens() {
        return tokens;
    }

    /**
     * 获取某行的主键。
     * <p>若该行index不存在，则返回null。</p>
     *
     * @param index 该行的索引
     * @return 若该行存在，则返回该行主键，否则返回null
     */
    public PrimaryKey get(int index) {
        Preconditions.checkArgument(index >= 0, "The index should not be negative.");

        if (rowKeys == null || rowKeys.isEmpty()) {
            return null;
        }

        if (index >= rowKeys.size()) {
            return null;
        }
        return rowKeys.get(index);
    }

    /**
     * 清空要查询的所有行。
     */
    public void clear() {
        this.rowKeys.clear();
    }

    /**
     * 获取要查询的行的个数。
     *
     * @return 行数。
     */
    public int size() {
        return rowKeys.size();
    }

    public boolean isEmpty() {
        return rowKeys.isEmpty();
    }

    public MultiRowQueryCriteria cloneWithoutRowKeys() {
        MultiRowQueryCriteria newCriteria = new MultiRowQueryCriteria(this.getTableName());
        this.copyTo(newCriteria);
        return newCriteria;
    }
}
