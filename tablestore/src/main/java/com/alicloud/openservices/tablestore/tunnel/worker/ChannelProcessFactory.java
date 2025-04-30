package com.alicloud.openservices.tablestore.tunnel.worker;

/**
 * Factory class for Channel data processors with record data consumption checkpoint functionality.
 */
public class ChannelProcessFactory implements IChannelProcessorFactory {
    private TunnelWorkerConfig config;

    public ChannelProcessFactory(TunnelWorkerConfig config) {
        this.config = config;
    }

    @Override
    public IChannelProcessor createProcessor(String tunnelId, String clientId, String channelId,
                                             ICheckpointer checkpointer) {
        DefaultChannelProcessor processor = new DefaultChannelProcessor(config.getChannelProcessor(), checkpointer,
            config.getCheckpointIntervalInMillis());
        return processor;
    }

}
