package com.alicloud.openservices.tablestore.tunnel.worker;

import com.alicloud.openservices.tablestore.model.tunnel.ChannelStatus;
import com.alicloud.openservices.tablestore.model.tunnel.internal.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailedChannelConnect implements IChannelConnect {
    private static final Logger LOG = LoggerFactory.getLogger(FailedChannelConnect.class);

    private TunnelStateMachine stateMachine;
    private Channel currentChannel;

    public FailedChannelConnect(TunnelStateMachine stateMachine) {
        this.stateMachine = stateMachine;
    }

    public Channel getCurrentChannel() {
        return currentChannel;
    }

    public void setCurrentChannel(Channel currentChannel) {
        this.currentChannel = currentChannel;
    }

    @Override
    public synchronized void notifyStatus(Channel channel) {
        if (currentChannel != null && currentChannel.getVersion() > channel.getVersion()) {
            return;
        }
        currentChannel = new Channel(channel);
        switch (currentChannel.getStatus()) {
            case CLOSE:
                break;
            case CLOSING:
            case OPEN:
                currentChannel.setVersion(currentChannel.getVersion() + 1);
                currentChannel.setStatus(ChannelStatus.CLOSE);
                stateMachine.updateStatus(currentChannel);
                break;
            case TERMINATED:
                break;
            default:
                LOG.error("unexpected channel status {}, channelId: {}", currentChannel.getStatus().name(),
                    currentChannel.getChannelId());
        }
    }

    @Override
    public synchronized boolean closed() {
        if (currentChannel == null) {
            return false;
        }
        return currentChannel.getStatus().equals(ChannelStatus.CLOSE) ||
            currentChannel.getStatus().equals(ChannelStatus.TERMINATED);
    }

    @Override
    public void close() {

    }

}
