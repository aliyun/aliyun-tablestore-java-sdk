package com.alicloud.openservices.tablestore.model.tunnel.internal;

import com.alicloud.openservices.tablestore.model.tunnel.ChannelStatus;

/**
 * Channel的元信息，每一个Channel在某一个版本的状态都是唯一的(服务端角度)。
 */
public class Channel {
    /**
     * Channel的ID。
     */
    private String channelId;
    /**
     * Channel的版本。
     */
    private long version;
    /**
     * Channel的状态。
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
