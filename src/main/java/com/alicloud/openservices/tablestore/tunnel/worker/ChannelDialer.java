package com.alicloud.openservices.tablestore.tunnel.worker;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.alicloud.openservices.tablestore.TunnelClientInterface;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.tunnel.pipeline.ProcessDataBackoff;
import com.alicloud.openservices.tablestore.tunnel.pipeline.ProcessDataPipeline;
import com.alicloud.openservices.tablestore.tunnel.pipeline.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChannelDialer implements IChannelDialer {
    private static final Logger LOG = LoggerFactory.getLogger(ChannelDialer.class);

    private final TunnelClientInterface client;
    private final TunnelWorkerConfig config;
    private final ExecutorService channelHelperExecutor;

    public ChannelDialer(TunnelClientInterface client, TunnelWorkerConfig config) {
        Preconditions.checkNotNull(client, "Tunnel client cannot be null.");
        Preconditions.checkNotNull(config, "Tunnel worker config cannot be null.");

        this.client = client;
        this.config = config;
        this.channelHelperExecutor = Executors.newCachedThreadPool(new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "channel-helper-executor-" + counter.getAndIncrement());
            }
        });
    }

    @Override
    public IChannelConnect channelDial(String tunnelId, String clientId, String channelId, String token,
                                       IChannelProcessor processor, TunnelStateMachine stateMachine) {
        LOG.info("Channel dialer create new channel connect, tunnelId: {}, clientId: {}, channelId: {}, token: {}",
            tunnelId, clientId, channelId, token);
        ChannelConnect channelConnect = new ChannelConnect();
        channelConnect.setTunnelId(tunnelId);
        channelConnect.setClientId(clientId);
        channelConnect.setChannelId(channelId);
        channelConnect.setToken(token);
        channelConnect.setClient(client);
        channelConnect.setProcessor(processor);
        channelConnect.setStateMachine(stateMachine);
        channelConnect.setFinished(new AtomicBoolean(false));
        channelConnect.setStreamChannel(Utils.isStreamToken(token));
        channelConnect.setProcessPipeline(new ProcessDataPipeline(channelConnect, channelHelperExecutor,
            config.getReadRecordsExecutor(), config.getProcessRecordsExecutor()));
        if (channelConnect.isStreamChannel()) {
            channelConnect.getProcessPipeline().setBackoff(new ProcessDataBackoff());
        }
        channelConnect.setChannelExecutorService(channelHelperExecutor);
        return channelConnect;
    }

    @Override
    public void shutdown() {
        LOG.info("Shutdown pipeline helper executor.");
        channelHelperExecutor.shutdownNow();
    }
}
