package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class DescribeTableRequest implements Request {
    /**
     * The name of the table.
     */
    private String tableName;
    
    public DescribeTableRequest() {
    }
    
    /**
     * Constructs a DescribeTableRequest object and specifies the name of the table.
     * @param tableName The name of the table.
     */
    public DescribeTableRequest(String tableName) {
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
     * Sets the name of the table.
     * @param tableName The name of the table.
     */
    public void setTableName(String tableName) {
        Preconditions.checkArgument(
            tableName != null && !tableName.isEmpty(),
            "The name of table should not be null or empty.");
        this.tableName = tableName;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_DESCRIBE_TABLE;
    }
}
