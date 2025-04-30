package com.alicloud.openservices.tablestore.tunnel.worker;

public interface IChannelProcessorFactory {
    IChannelProcessor createProcessor(String tunnelId, String clientId, String channelId, ICheckpointer checkpointer);
}
