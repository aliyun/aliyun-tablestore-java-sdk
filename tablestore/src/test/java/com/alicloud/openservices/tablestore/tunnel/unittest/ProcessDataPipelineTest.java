package com.alicloud.openservices.tablestore.tunnel.unittest;

import com.alicloud.openservices.tablestore.TunnelClientInterface;
import com.alicloud.openservices.tablestore.model.tunnel.ChannelStatus;
import com.alicloud.openservices.tablestore.model.tunnel.internal.Channel;
import com.alicloud.openservices.tablestore.tunnel.pipeline.ProcessDataPipeline;
import com.alicloud.openservices.tablestore.tunnel.worker.*;
import junit.framework.TestCase;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

public class ProcessDataPipelineTest extends TestCase {
    private static final String TUNNEL_ID = "test-tunnel-id-zr";
    private static final String CLIENT_ID = "test-client-id-zr";
    private static final String CHANNEL_ID = "test-channel-id-zr";

    private TunnelWorkerConfig config;
    private TunnelClientInterface tunnelClient;
    private ChannelConnect channelConnect;
    private IChannelProcessor processor;

    @Override
    protected void setUp() throws Exception {
        processor = new MockChannelProcessor();
        config = new TunnelWorkerConfig(new MockChannelProcessor());
        config.setReadRecordsExecutor((ThreadPoolExecutor) Executors.newFixedThreadPool(1));
        config.setProcessRecordsExecutor((ThreadPoolExecutor) Executors.newFixedThreadPool(1));
        config.setChannelHelperExecutor((ThreadPoolExecutor) Executors.newFixedThreadPool(1));
        tunnelClient = new MockTunnelClient();
        channelConnect = new ChannelConnect();
        channelConnect.setTunnelId(TUNNEL_ID);
        channelConnect.setClientId(CLIENT_ID);
        channelConnect.setChannelId(CHANNEL_ID);
        channelConnect.setClient(tunnelClient);
        channelConnect.setToken("test-token" + System.currentTimeMillis());
        channelConnect.setProcessor(processor);
        channelConnect.setStateMachine(null);
        channelConnect.setFinished(new AtomicBoolean(false));
        channelConnect.setStreamChannel(true);
        channelConnect.setChannelExecutorService(config.getChannelHelperExecutor());
    }

    @Override
    protected void tearDown() throws Exception {
        config.shutdown();
        config = null;
    }

    public void testNormalProcess() {
        channelConnect.setCurrentChannel(new Channel(CHANNEL_ID, 10, ChannelStatus.OPEN));
        channelConnect.setStatus(ChannelConnectStatus.RUNNING);
        ProcessDataPipeline pipeline = new ProcessDataPipeline(channelConnect, config.getChannelHelperExecutor(),
                config.getReadRecordsExecutor(), config.getProcessRecordsExecutor());
        channelConnect.setProcessPipeline(pipeline);
        pipeline.run();

        int loopTimes = 10;
        try {
            Thread.sleep((loopTimes + 1) * ((MockChannelProcessor) processor).getProcessTimeInMillis());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assertEquals(loopTimes, ((MockChannelProcessor) processor).getProcessCount());
    }

    public void testClosingProcess() {
        channelConnect.setCurrentChannel(new Channel(CHANNEL_ID, 10, ChannelStatus.OPEN));
        channelConnect.setStatus(ChannelConnectStatus.RUNNING);
        ProcessDataPipeline pipeline = new ProcessDataPipeline(channelConnect, config.getChannelHelperExecutor(),
                config.getReadRecordsExecutor(), config.getProcessRecordsExecutor());
        channelConnect.setProcessPipeline(pipeline);
        pipeline.run();

        try {
            Thread.sleep(3 * ((MockChannelProcessor) processor).getProcessTimeInMillis());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        channelConnect.setCurrentChannel(new Channel(CHANNEL_ID, 11, ChannelStatus.CLOSING));
        channelConnect.setStatus(ChannelConnectStatus.CLOSING);

        int loopTimes = 10;
        try {
            Thread.sleep((loopTimes + 1) * ((MockChannelProcessor) processor).getProcessTimeInMillis());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assertTrue(((MockChannelProcessor) processor).getProcessCount() < loopTimes);
    }
}
