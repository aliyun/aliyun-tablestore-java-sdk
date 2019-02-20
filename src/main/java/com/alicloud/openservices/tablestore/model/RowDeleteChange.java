package com.alicloud.openservices.tablestore.model;

public class RowDeleteChange extends RowChange {

    /**
     * 构造函数。
     *
     * @param tableName 表的名称
     * @param primaryKey 要删除的行的主键
     */
    public RowDeleteChange(String tableName, PrimaryKey primaryKey) {
    	super(tableName, primaryKey);
    }
    
    /**
     * 构造函数。
     *
     * @param tableName 表的名称
     */
    public RowDeleteChange(String tableName) {
    	super(tableName);
    }

    @Override
    public int getDataSize() {
        return getPrimaryKey().getDataSize();
    }
}
