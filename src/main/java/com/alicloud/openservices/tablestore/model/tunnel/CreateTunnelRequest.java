package com.alicloud.openservices.tablestore.model.tunnel;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

/**
 * CreateTunnelRequest包含创建一个新Tunnel所必须的一些参数，包括表名、Tunnel名和Tunnel的类型。
 */
public class CreateTunnelRequest implements Request {
    /**
     * 表名。
     */
    private String tableName;

    /**
     * Tunnel名。
     */
    private String tunnelName;

    /**
     * Tunnel类型, 目前支持全量、增量和全量加增量三类。
     */
    private TunnelType tunnelType;

    /**
     * Tunnel时间范围配置。
     */
    private StreamTunnelConfig streamTunnelConfig;

    /**
     * 初始化CreateTunnelRequest实例。
     *
     * @param tableName  表名。
     * @param tunnelName Tunnel名。
     * @param tunnelType Tunnel类型。
     */
    public CreateTunnelRequest(String tableName, String tunnelName,
                               TunnelType tunnelType) {
        setTableName(tableName);
        setTunnelName(tunnelName);
        setTunnelType(tunnelType);
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

    /**
     * 获取Tunnel类型。
     *
     * @return Tunnel类型。
     */
    public TunnelType getTunnelType() {
        return tunnelType;
    }

    /**
     * 设置Tunnel类型。
     *
     * @param tunnelType Tunnel类型。
     */
    public void setTunnelType(TunnelType tunnelType) {
        Preconditions.checkArgument(tunnelType != null,
            "The tunnel type should not be null.");
        this.tunnelType = tunnelType;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_CREATE_TUNNEL;
    }

    public StreamTunnelConfig getStreamTunnelConfig() {
        return streamTunnelConfig;
    }

    public void setStreamTunnelConfig(StreamTunnelConfig streamTunnelConfig) {
        this.streamTunnelConfig = streamTunnelConfig;
    }
}
