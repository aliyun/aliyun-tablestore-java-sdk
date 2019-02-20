package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class DescribeTableRequest implements Request {
    /**
     * 表的名称。
     */
    private String tableName;
    
    public DescribeTableRequest() {
    }
    
    /**
     * 构造DescribeTableRequest对象，并指定表的名称。
     * @param tableName 表的名称。
     */
    public DescribeTableRequest(String tableName) {
        setTableName(tableName);
    }

    /**
     * 获取表的名称。
     * @return 表的名称。
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * 设置表的名称。
     * @param tableName 表的名称。
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
