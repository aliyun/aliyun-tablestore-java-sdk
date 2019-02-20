package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class ListStreamRequest implements Request {

    /**
     * 表的名称，可选参数
     * 若设置了表的名称，则只获取该表下的Stream
     */
    private String tableName;

    /**
     * 获取所有的Stream，不限定表
     */
    public ListStreamRequest() {
    }

    /**
     * 获取特定表的Stream
     * @param tableName
     */
    public ListStreamRequest(String tableName) {
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
     * 设置表的名称。设置表的名称后，将只获取该表下的Stream。
     * @param tableName 表的名称。
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
