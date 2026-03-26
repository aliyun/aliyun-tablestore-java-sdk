package com.alicloud.openservices.tablestore.model;


import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import java.util.ArrayList;
import java.util.List;

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
     * The primary keys of the rows.
     */
    private List<PrimaryKey> rowKeys;

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
     * Get the primary keys of the rows to be locked.
     *
     * @return the list of primary keys
     */
    public List<PrimaryKey> getRowKeys() {
        return rowKeys;
    }

    /**
     * Set the primary keys of the rows to be locked.
     * This will replace the existing list with the provided list.
     *
     * @param rowKeys the list of primary keys to set
     */
    public void setRowKeys(final List<PrimaryKey> rowKeys) {
        Preconditions.checkArgument(rowKeys != null && !rowKeys.isEmpty(), "The rowKeys list should not be null or empty.");
        this.rowKeys = rowKeys;
    }

    /**
     * Add a primary key to the list of rows to be locked.
     *
     * @param rowKey the primary key to add
     */
    public void addRowKey(final PrimaryKey rowKey) {
        Preconditions.checkArgument(rowKey != null && !rowKey.isEmpty(), "The rowKey should not be null or empty.");
        if (rowKeys == null) {
            rowKeys = new ArrayList<>();
        }
        this.rowKeys.add(rowKey);
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

    public StartLocalTransactionRequest(String tableName, PrimaryKey key, List<PrimaryKey> rowKeys) {
        setTableName(tableName);
        setPrimaryKey(key);
        setRowKeys(rowKeys);
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_START_LOCAL_TRANSACTION;
    }
}
