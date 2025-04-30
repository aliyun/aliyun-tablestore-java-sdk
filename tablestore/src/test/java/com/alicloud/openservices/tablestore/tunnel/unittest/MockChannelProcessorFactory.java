package com.alicloud.openservices.tablestore.tunnel.unittest;

import com.alicloud.openservices.tablestore.tunnel.worker.IChannelProcessor;
import com.alicloud.openservices.tablestore.tunnel.worker.IChannelProcessorFactory;
import com.alicloud.openservices.tablestore.tunnel.worker.ICheckpointer;


public class MockChannelProcessorFactory implements IChannelProcessorFactory {
    public static final String BAD_CHANNEL_ID = "cid-bad";

    @Override
    public IChannelProcessor createProcessor(String tunnelId, String clientId, String channelId,
                                             ICheckpointer checkpointer) {
        if (BAD_CHANNEL_ID.equals(channelId)) {
             return new MockBadChannelProcessor();
        }
        return new MockChannelProcessor();
    }
}
