package com.alicloud.openservices.tablestore.model;


import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class StartLocalTransactionRequest implements Request{

    /**
     * 表的名称。
     */
    private String tableName;

    /**
     * 表的主键。
     */
    private PrimaryKey primaryKey;

    /**
     *  设置表的名称。
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     *  获取表的名称。
     *
     * @return 表的名称
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * 添加主键（Primary Key）列的名称和值。
     * @param primaryKey 行的主键。
     */
    public void setPrimaryKey(PrimaryKey primaryKey){
        Preconditions.checkNotNull(primaryKey, "primaryKey");

        this.primaryKey = primaryKey;
    }

    /**
     * 获取该行的主键。
     *
     * @return 行的主键
     */
    public PrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    /**
     * 初始化StartLocalTransactionRequest实例。
     *
     * @param tableName 表名。
     * @param key  主键，对于本地事务，只需要指定第一个主键即可。
     */
    public StartLocalTransactionRequest(String tableName, PrimaryKey key) {
        setTableName(tableName);
        setPrimaryKey(key);
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_START_LOCAL_TRANSACTION;
    }
}
