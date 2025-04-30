package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class ListStreamRequest implements Request {

    /**
     * The name of the table, optional parameter.
     * If the table name is set, only get the Stream under this table.
     */
    private String tableName;

    /**
     * Get all Streams, without limiting to a specific table
     */
    public ListStreamRequest() {
    }

    /**
     * Get the Stream of a specific table
     * @param tableName
     */
    public ListStreamRequest(String tableName) {
        setTableName(tableName);
    }

    /**
     * Get the name of the table.
     * @return The name of the table.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Sets the name of the table. After setting the table name, Streams under that table will be retrieved only.
     * @param tableName The name of the table.
     */
    public void setTableName(String tableName) {
        Preconditions.checkArgument(tableName != null && !tableName.isEmpty(), "The name of table should not be null or empty.");
        this.tableName = tableName;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_LIST_STREAM;
    }
}
