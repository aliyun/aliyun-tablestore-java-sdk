package com.alicloud.openservices.tablestore.model.tunnel;

import java.util.Date;

public class ChannelInfo {
    /**
     * Channel的ID。
     */
    private String channelId;
    /**
     * Channel的类型, 目前支持BaseData(全量)和增量(Stream)两类。
     */
    private ChannelType channelType;
    /**
     * Channel的状态，有等待(WAIT), 打开(OPEN), 关闭中(CLOSING), 关闭(CLOSE)和结束(TERMINATED)五种。
     */
    private ChannelStatus channelStatus;
    /**
     * Tunnel客户端的ID标识, 默认由客户端主机名和随机串拼接而成。
     */
    private String clientId;
    /**
     * Channel消费增量数据的最新时间点，默认值为1970年1月1日(UTC), 全量类型无此概念。
     */
    private Date channelConsumePoint;

    /**
     * Channel增量消费的RPO(Recovery Point Objective)时间, 单位为毫秒, 全量类型无此概念。
     */
    private long channelRpo;

    /**
     * Channel同步的数据条数。
     */
    private long channelCount;

    /**
     * 获取Channel ID。
     * @return Channel的ID。
     */
    public String getChannelId() {
        return channelId;
    }

    /**
     * 设置ChannelID。
     * @param channelId Channel ID。
     */
    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    /**
     * 获取Channel类型。
     * @return Channel类型。
     */
    public ChannelType getChannelType() {
        return channelType;
    }

    /**
     * 设置Channel类型。
     * @param channelType Channel类型。
     */
    public void setChannelType(ChannelType channelType) {
        this.channelType = channelType;
    }

    /**
     * 获取Channel的状态。
     * @return Channel的状态。
     */
    public ChannelStatus getChannelStatus() {
        return channelStatus;
    }

    /**
     * 设置Channel的状态。
     * @param channelStatus Channel的状态。
     */
    public void setChannelStatus(ChannelStatus channelStatus) {
        this.channelStatus = channelStatus;
    }

    /**
     * 获取Tunnel Client的 ID。
     * @return Tunnel Client ID。
     */
    public String getClientId() {
        return clientId;
    }

    /**
     * 设置Tunnel Client的ID。
     * @param clientId Tunnel Client ID。
     */
    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    /**
     * 获取当前Channel的RPO(Recovery Point Objective)时间, 单位为毫秒。
     * @return
     */
    public long getChannelRpo() {
        return channelRpo;
    }

    public void setChannelRpo(long channelRpo) {
        this.channelRpo = channelRpo;
    }

    /**
     * 获取当前Channel消费数据的最新时间点。
     * @return Channel消费数据的最新时间点。
     */
    public Date getChannelConsumePoint() {
        return channelConsumePoint;
    }

    /**
     * 内部接口，请勿使用。
     */
    public void setChannelConsumePoint(Date channelConsumePoint) {
        this.channelConsumePoint = channelConsumePoint;
    }

    public long getChannelCount() {
        return channelCount;
    }

    public void setChannelCount(long channelCount) {
        this.channelCount = channelCount;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{").append("ChannelId: ").append(channelId).append(", ChannelType: ").append(channelType)
            .append(", ChannelStatus: ").append(channelStatus).append(", ClientId: ").append(clientId)
            .append(", ChannelConsumePoint: ").append(channelConsumePoint).append(", ChannelRpo: ")
            .append(channelRpo).append(", ChannelCount: ").append(channelCount).append("}");
        return sb.toString();
    }
}
