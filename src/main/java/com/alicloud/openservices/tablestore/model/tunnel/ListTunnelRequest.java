package com.alicloud.openservices.tablestore.model.tunnel;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

/**
 * ListTunnelRequest包含列举某张表下的Tunnel所必须的一些参数，包括表名。
 */
public class ListTunnelRequest implements Request {
    /**
     * 表名。
     */
    private String tableName;

    /**
     * 初始化ListTunnelRequest实例。
     * @param tableName 表名。
     */
    public ListTunnelRequest(String tableName) {
        setTableName(tableName);
    }

    /**
     * 获取表名。
     * @return 表名。
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * 设置表名。
     * @param tableName 表名。
     */
    public void setTableName(String tableName) {
        //Preconditions.checkArgument(tableName != null && !tableName.isEmpty(),
        //    "The table name should not be empty.");
        this.tableName = tableName;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_LIST_TUNNEL;
    }
}
