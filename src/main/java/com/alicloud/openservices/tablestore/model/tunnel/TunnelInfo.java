package com.alicloud.openservices.tablestore.model.tunnel;

public class TunnelInfo {
    /**
     * Tunnel的名称。
     */
    private String tunnelName;
    /**
     * Tunnel的ID。
     */
    private String tunnelId;
    /**
     * Tunnel的类型，有全量(BaseData), 增量(Stream)和全量加增量(BaseAndStream)三类。
     */
    private TunnelType tunnelType;
    /**
     * 表的名称。
     */
    private String tableName;
    /**
     * 实例名称。
     */
    private String instanceName;
    /**
     * Tunnel所处的阶段，有初始化(InitBaseDataAndStreamShard), 全量处理(ProcessBaseData)和增量处理(ProcessStream)三类。
     */
    private TunnelStage stage;
    /**
     * 数据是否超期, 若该值返回true, 请及时在钉钉上联系 表格存储技术支持。
     */
    private boolean expired;

    /**
     * Tunnel对应的时间范围配置。
     */
    private StreamTunnelConfig streamTunnelConfig;

    /**
     * Tunnel创建时间。
     */
    private long createTime;

    /**
     * 获取Tunnel名称。
     * @return Tunnel名称。
     */
    public String getTunnelName() {
        return tunnelName;
    }

    /**
     * 设置Tunnel名称。
     * @param tunnelName Tunnel名称。
     */
    public void setTunnelName(String tunnelName) {
        this.tunnelName = tunnelName;
    }

    /**
     * 获取Tunnel ID。
     * @return Tunnel ID。
     */
    public String getTunnelId() {
        return tunnelId;
    }

    /**
     * 设置Tunnel ID。
     * @param tunnelId Tunnel ID。
     */
    public void setTunnelId(String tunnelId) {
        this.tunnelId = tunnelId;
    }

    /**
     * 获取Tunnel类型。
     * @return Tunnel类型。
     */
    public TunnelType getTunnelType() {
        return tunnelType;
    }

    /**
     * 设置Tunnel类型。
     * @param tunnelType Tunnel类型。
     */
    public void setTunnelType(TunnelType tunnelType) {
        this.tunnelType = tunnelType;
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
        this.tableName = tableName;
    }

    /**
     * 获取实例名。
     * @return 实例名。
     */
    public String getInstanceName() {
        return instanceName;
    }

    /**
     * 设置实例名。
     * @param instanceName 实例名。
     */
    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    /**
     * 获取Tunnel所处的阶段。
     * @return Tunnel所处的阶段。
     */
    public TunnelStage getStage() {
        return stage;
    }

    /**
     * 设置Tunnel所处的阶段。
     * @param stage Tunnel所处的阶段。
     */
    public void setStage(TunnelStage stage) {
        this.stage = stage;
    }

    /**
     * 获取数据是否超期。
     * @return 数据是否超期。
     */
    public boolean isExpired() {
        return expired;
    }

    /**
     * 设置数据是否过期。
     * @param expired 数据是否超期
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
