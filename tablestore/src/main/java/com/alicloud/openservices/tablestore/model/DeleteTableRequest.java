package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class DeleteTableRequest implements Request {
    /**
     * The name of the table.
     */
    private String tableName;
    
    public DeleteTableRequest(String tableName) {
        setTableName(tableName);
    }

    /**
     * Get the name of the table.
     * @return The name of the table.
     */
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        Preconditions.checkArgument(
            tableName != null && !tableName.isEmpty(),
            "The name of table should not be null or empty.");
        this.tableName = tableName;
    }

    public String getOperationName() {
        return OperationNames.OP_DELETE_TABLE;
    }

}
