package com.aliyun.openservices.ots.model;

/**
 * 表示获取一行数据的查询条件。
 *
 */
public class SingleRowQueryCriteria extends RowQueryCriteria{
    private RowPrimaryKey primaryKey = new RowPrimaryKey();

    /**
     * 构造一个在给定名称的表中查询的条件。
     * @param tableName 查询的表名。
     */
    public SingleRowQueryCriteria(String tableName){
        super(tableName);
    }

    /**
     * 获取主键（Primary Key）列名称与值的对应字典（只读）。
     * @return 主键（Primary Key）列名称与值的对应字典（只读）。
     */
    public RowPrimaryKey getRowPrimaryKey() {
        return primaryKey;
    }

    /**
     * 设置主键（Primary Key）。
     * @param primaryKey 行的主键。
     */
    public void setPrimaryKey(RowPrimaryKey primaryKey){
        this.primaryKey = primaryKey;
    }
}
