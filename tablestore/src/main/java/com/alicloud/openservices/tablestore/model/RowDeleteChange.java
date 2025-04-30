package com.alicloud.openservices.tablestore.model;

public class RowDeleteChange extends RowChange {

    /**
     * Constructor.
     *
     * @param tableName The name of the table
     * @param primaryKey The primary key of the row to be deleted
     */
    public RowDeleteChange(String tableName, PrimaryKey primaryKey) {
    	super(tableName, primaryKey);
    }
    
    /**
     * Constructor.
     *
     * @param tableName The name of the table
     */
    public RowDeleteChange(String tableName) {
    	super(tableName);
    }

    @Override
    public int getDataSize() {
        return getPrimaryKey().getDataSize();
    }
}
