package com.aliyun.openservices.ots.model;

import com.aliyun.openservices.ots.utils.Preconditions;

import java.util.ArrayList;
import java.util.List;

public class MultiRowQueryCriteria extends RowQueryCriteria {
    
    private List<RowPrimaryKey> rowKeys;

    public MultiRowQueryCriteria(String tableName) {
        super(tableName);
        rowKeys = new ArrayList<RowPrimaryKey>();
    }

    /**
     * 向多行查询条件中插入要查询的行的主键。
     * @param primaryKey 要查询的行的主键。
     */
    public void addRow(RowPrimaryKey primaryKey) {
        this.rowKeys.add(primaryKey);
    }

    /**
     * 获取该表中所要要查询的行的主键。
     * @return 所有行的主键。
     */
    public List<RowPrimaryKey> getRowKeys() {
        return rowKeys;
    }

    /**
     * 设置该表中所有要查询的行的主键。
     * @param primaryKeys 所有行的主键。
     */
    public void setRowKeys(List<RowPrimaryKey> primaryKeys) {
        this.rowKeys = primaryKeys;
    }

    /**
     * 获取某行的主键。
     * <p>若该行index不存在，则返回null。</p>
     *
     * @param index 该行的索引
     * @return 若该行存在，则返回该行主键，否则返回null
     */
    public RowPrimaryKey get(int index) {
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
