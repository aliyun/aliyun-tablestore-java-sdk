package com.aliyun.openservices.ots.model;

import static com.aliyun.openservices.ots.utils.CodingUtils.*;

public class DescribeTableRequest {
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
        assertParameterNotNull(tableName, "tableName");
        this.tableName = tableName;
    }
}
