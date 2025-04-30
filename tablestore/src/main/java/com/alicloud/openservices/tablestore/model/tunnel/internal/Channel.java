package com.alicloud.openservices.tablestore.model.tunnel.internal;

import com.alicloud.openservices.tablestore.model.tunnel.ChannelStatus;

/**
 * The meta information of the Channel. The state of each Channel at a certain version is unique (from the server's perspective).
 */
public class Channel {
    /**
     * The ID of the Channel.
     */
    private String channelId;
    /**
     * The version of the Channel.
     */
    private long version;
    /**
     * The status of the Channel.
     */
    private ChannelStatus status;

    public Channel() {
    }

    public Channel(Channel channel) {
        this(channel.getChannelId(), channel.getVersion(), channel.getStatus());
    }

    public Channel(String channelId, long version, ChannelStatus status) {
        this.channelId = channelId;
        this.version = version;
        this.status = status;
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public ChannelStatus getStatus() {
        return status;
    }

    public void setStatus(ChannelStatus status) {
        this.status = status;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[ChannelId: ").append(channelId).append(", Version: ").append(version)
            .append(", ChannelStatus: ").append(status.name()).append("]");
        return sb.toString();
    }
}
