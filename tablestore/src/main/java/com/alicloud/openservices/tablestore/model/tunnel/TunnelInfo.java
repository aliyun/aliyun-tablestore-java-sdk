package com.alicloud.openservices.tablestore.model.tunnel;

public class TunnelInfo {
    /**
     * The name of the Tunnel.
     */
    private String tunnelName;
    /**
     * The ID of the Tunnel.
     */
    private String tunnelId;
    /**
     * The type of Tunnel, which includes three categories: full amount (BaseData), incremental (Stream), and full amount plus incremental (BaseAndStream).
     */
    private TunnelType tunnelType;
    /**
     * The name of the table.
     */
    private String tableName;
    /**
     * Instance name.
     */
    private String instanceName;
    /**
     * The phase of the Tunnel, which includes three types: initialization (InitBaseDataAndStreamShard), full processing (ProcessBaseData), and incremental processing (ProcessStream).
     */
    private TunnelStage stage;
    /**
     * Whether the data has expired, if this value returns true, please contact the Tablestore technical support team on DingTalk as soon as possible.
     */
    private boolean expired;

    /**
     * Time range configuration corresponding to the Tunnel.
     */
    private StreamTunnelConfig streamTunnelConfig;

    /**
     * The creation time of the Tunnel.
     */
    private long createTime;

    /**
     * Get the Tunnel name.
     * @return The Tunnel name.
     */
    public String getTunnelName() {
        return tunnelName;
    }

    /**
     * Set the Tunnel name.
     * @param tunnelName The name of the Tunnel.
     */
    public void setTunnelName(String tunnelName) {
        this.tunnelName = tunnelName;
    }

    /**
     * Get the Tunnel ID.
     * @return Tunnel ID.
     */
    public String getTunnelId() {
        return tunnelId;
    }

    /**
     * Set the Tunnel ID.
     * @param tunnelId The Tunnel ID.
     */
    public void setTunnelId(String tunnelId) {
        this.tunnelId = tunnelId;
    }

    /**
     * Get the Tunnel type.
     * @return Tunnel type.
     */
    public TunnelType getTunnelType() {
        return tunnelType;
    }

    /**
     * Set the Tunnel type.
     * @param tunnelType The type of Tunnel.
     */
    public void setTunnelType(TunnelType tunnelType) {
        this.tunnelType = tunnelType;
    }

    /**
     * Get the table name.
     * @return The table name.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Set the table name.
     * @param tableName The name of the table.
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * Get the instance name.
     * @return The instance name.
     */
    public String getInstanceName() {
        return instanceName;
    }

    /**
     * Set the instance name.
     * @param instanceName The name of the instance.
     */
    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    /**
     * Get the phase of the Tunnel.
     * @return The phase of the Tunnel.
     */
    public TunnelStage getStage() {
        return stage;
    }

    /**
     * Sets the stage of the Tunnel.
     * @param stage The stage of the Tunnel.
     */
    public void setStage(TunnelStage stage) {
        this.stage = stage;
    }

    /**
     * Check if the data is expired.
     * @return Whether the data is expired.
     */
    public boolean isExpired() {
        return expired;
    }

    /**
     * Set whether the data has expired.
     * @param expired Whether the data has expired
     */
    public void setExpired(boolean expired) {
        this.expired = expired;
    }

    public StreamTunnelConfig getStreamTunnelConfig() {
        return streamTunnelConfig;
    }

    public void setStreamTunnelConfig(StreamTunnelConfig streamTunnelConfig) {
        this.streamTunnelConfig = streamTunnelConfig;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{").append("TunnelName: ").append(tunnelName).append(", TunnelId: ")
            .append(tunnelId).append(", TunnelType: ").append(tunnelType).append(", TableName: ").append(tableName)
            .append(", InstanceName: ").append(instanceName).append(", Stage: ").append(stage.name())
            .append(", Expired: ").append(expired).append(", StreamTunnelConfig: ").append(streamTunnelConfig)
                .append(", CreateTime: ").append(createTime).append("}");
        return sb.toString();
    }
}
