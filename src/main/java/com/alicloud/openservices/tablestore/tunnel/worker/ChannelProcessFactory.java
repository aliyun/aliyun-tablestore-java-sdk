package com.alicloud.openservices.tablestore.tunnel.worker;

/**
 * 带记录数据消费位点功能的Channel数据处理器的工厂类。
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
