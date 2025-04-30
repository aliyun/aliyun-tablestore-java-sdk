package com.alicloud.openservices.tablestore.model.tunnel;

import java.util.Date;

public class ChannelInfo {
    /**
     * The ID of the Channel.
     */
    private String channelId;
    /**
     * The type of Channel, currently supports two types: BaseData (full amount) and Incremental (Stream).
     */
    private ChannelType channelType;
    /**
     * The status of the Channel, there are five statuses: waiting (WAIT), open (OPEN), closing (CLOSING), closed (CLOSE), and terminated (TERMINATED).
     */
    private ChannelStatus channelStatus;
    /**
     * The ID of the Tunnel client, which is defaultly composed of the client hostname and a random string.
     */
    private String clientId;
    /**
     * The latest time point for consuming incremental data in the Channel, the default value is January 1, 1970 (UTC). This concept does not apply to full-amount types.
     */
    private Date channelConsumePoint;

    /**
     * Channel incremental consumption RPO (Recovery Point Objective) time, in milliseconds. This concept does not apply to full-type consumption.
     */
    private long channelRpo;

    /**
     * The number of data rows synchronized by Channel.
     */
    private long channelCount;

    /**
     * Get the Channel ID.
     * @return The ID of the Channel.
     */
    public String getChannelId() {
        return channelId;
    }

    /**
     * Set the ChannelID.
     * @param channelId Channel ID.
     */
    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    /**
     * Get the Channel type.
     * @return The Channel type.
     */
    public ChannelType getChannelType() {
        return channelType;
    }

    /**
     * Set the Channel type.
     * @param channelType The type of Channel.
     */
    public void setChannelType(ChannelType channelType) {
        this.channelType = channelType;
    }

    /**
     * Get the status of the Channel.
     * @return The status of the Channel.
     */
    public ChannelStatus getChannelStatus() {
        return channelStatus;
    }

    /**
     * Set the status of the Channel.
     * @param channelStatus The status of the Channel.
     */
    public void setChannelStatus(ChannelStatus channelStatus) {
        this.channelStatus = channelStatus;
    }

    /**
     * Get the ID of the Tunnel Client.
     * @return Tunnel Client ID.
     */
    public String getClientId() {
        return clientId;
    }

    /**
     * Set the ID for the Tunnel Client.
     * @param clientId Tunnel Client ID.
     */
    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    /**
     * Get the RPO (Recovery Point Objective) time of the current Channel, in milliseconds.
     * @return
     */
    public long getChannelRpo() {
        return channelRpo;
    }

    public void setChannelRpo(long channelRpo) {
        this.channelRpo = channelRpo;
    }

    /**
     * Get the latest timestamp of the current Channel consumption data.
     * @return The latest timestamp of the Channel consumption data.
     */
    public Date getChannelConsumePoint() {
        return channelConsumePoint;
    }

    /**
     * Internal interface, do not use.
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
