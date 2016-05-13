package com.aliyun.openservices.ots.model;

import static com.aliyun.openservices.ots.utils.CodingUtils.*;

public class DeleteTableRequest {
    /**
     * 表的名称。
     */
    private String tableName;
    
    public DeleteTableRequest() {
        this("");
    }
    
    public DeleteTableRequest(String tableName) {
        setTableName(tableName);
    }

    /**
     * 获取表的名称。
     * @return 表的名称。
     */
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        assertParameterNotNull(tableName, "tableName");
        this.tableName = tableName;
    }

}
