package com.alicloud.openservices.tablestore.tunnel.unittest;

import com.alicloud.openservices.tablestore.tunnel.worker.IChannelConnect;
import com.alicloud.openservices.tablestore.tunnel.worker.IChannelDialer;
import com.alicloud.openservices.tablestore.tunnel.worker.IChannelProcessor;
import com.alicloud.openservices.tablestore.tunnel.worker.TunnelStateMachine;

public class MockChannelDialer implements IChannelDialer {
    @Override
    public IChannelConnect channelDial(String tunnelId, String clientId, String channelId, String token,
                                       IChannelProcessor processor, TunnelStateMachine state) {
        return null;
    }

    @Override
    public void shutdown() {

    }
}
