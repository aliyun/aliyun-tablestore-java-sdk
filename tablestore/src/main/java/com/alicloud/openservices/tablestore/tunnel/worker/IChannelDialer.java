package com.alicloud.openservices.tablestore.tunnel.worker;

public interface IChannelDialer {
    IChannelConnect channelDial(String tunnelId, String clientId, String channelId, String token,
                                       IChannelProcessor processor, TunnelStateMachine state);
    void shutdown();
}
