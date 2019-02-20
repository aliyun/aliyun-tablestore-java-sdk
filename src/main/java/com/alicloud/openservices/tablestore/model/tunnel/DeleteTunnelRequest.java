package com.alicloud.openservices.tablestore.model.tunnel;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

/**
 * DeleteTunnelRequest包含删除一个Tunnel所必须的一些参数，包括表名、Tunnel名。
 */
public class DeleteTunnelRequest implements Request {
    /**
     * 表名。
     */
    private String tableName;

    /**
     * Tunnel名。
     */
    private String tunnelName;

    /**
     * 初始化DeleteTunnelRequest实例。
     *
     * @param tableName  表名。
     * @param tunnelName Tunnel名。
     */
    public DeleteTunnelRequest(String tableName, String tunnelName) {
        setTableName(tableName);
        setTunnelName(tunnelName);
    }

    /**
     * 获取表名。
     *
     * @return 表名。
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * 设置表名。
     *
     * @param tableName 表名。
     */
    public void setTableName(String tableName) {
        Preconditions.checkArgument(tableName != null && !tableName.isEmpty(),
            "The table name should not be empty.");
        this.tableName = tableName;
    }

    /**
     * 获取Tunnel名。
     *
     * @return Tunnel名。
     */
    public String getTunnelName() {
        return tunnelName;
    }

    /**
     * 设置Tunnel名。
     *
     * @param tunnelName Tunnel名。
     */
    public void setTunnelName(String tunnelName) {
        Preconditions.checkArgument(tunnelName != null && !tunnelName.isEmpty(),
            "The tunnel name should not be empty.");
        this.tunnelName = tunnelName;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_DELETE_TUNNEL;
    }
}
