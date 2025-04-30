package com.alicloud.openservices.tablestore.model;


import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class StartLocalTransactionRequest implements Request{

    /**
     * The name of the table.
     */
    private String tableName;

    /**
     * The primary key of the table.
     */
    private PrimaryKey primaryKey;

    /**
     * Set the name of the table.
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * Get the name of the table.
     *
     * @return the name of the table
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Add the name and value of the primary key (Primary Key) column.
     * @param primaryKey The primary key of the row.
     */
    public void setPrimaryKey(PrimaryKey primaryKey){
        Preconditions.checkNotNull(primaryKey, "primaryKey");

        this.primaryKey = primaryKey;
    }

    /**
     * Get the primary key of this row.
     *
     * @return the primary key of the row
     */
    public PrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    /**
     * Initialize the StartLocalTransactionRequest instance.
     *
     * @param tableName The name of the table.
     * @param key The primary key. For a local transaction, only the first primary key needs to be specified.
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
