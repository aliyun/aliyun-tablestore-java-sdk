package com.alicloud.openservices.tablestore.tunnel.unittest;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.alicloud.openservices.tablestore.model.tunnel.ChannelStatus;
import com.alicloud.openservices.tablestore.model.tunnel.internal.Channel;
import com.alicloud.openservices.tablestore.tunnel.worker.ChannelDialer;
import com.alicloud.openservices.tablestore.tunnel.worker.FailedChannelConnect;
import com.alicloud.openservices.tablestore.tunnel.worker.TunnelStateMachine;
import com.alicloud.openservices.tablestore.tunnel.worker.TunnelWorkerConfig;
import junit.framework.TestCase;
import org.junit.Assert;

public class FailedChannelConnectTest extends TestCase {
    private static final String TUNNEL_ID = "test-tunnel-id-zr";
    private static final String CLIENT_ID = "test-client-id-zr";

    private static final String CHANNEL_ID = "test-channel-id-zr";

    private FailedChannelConnect channelConnect;
    private TunnelWorkerConfig config;
    private ChannelDialer channelDialer;

    @Override
    protected void setUp() throws Exception {
        config = new TunnelWorkerConfig(new MockChannelProcessor());
        config.setReadRecordsExecutor((ThreadPoolExecutor)Executors.newFixedThreadPool(1));
        config.setProcessRecordsExecutor((ThreadPoolExecutor)Executors.newFixedThreadPool(1));
        channelDialer = new ChannelDialer(new MockTunnelClient(), config);
        channelConnect = new FailedChannelConnect(new TunnelStateMachine(TUNNEL_ID, CLIENT_ID, channelDialer,
            new MockChannelProcessorFactory(), new MockTunnelClient()));
    }

    @Override
    protected void tearDown() throws Exception {
        config.shutdown();
        config = null;
        channelConnect.close();
        channelConnect = null;
        channelDialer.shutdown();
    }

    public void testFailConn_NotifyStatus_NilWithOpen() {
        Channel channel = new Channel(CHANNEL_ID, 0, ChannelStatus.OPEN);
        channelConnect.notifyStatus(channel);
        Channel retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.CLOSE, retChannel.getStatus());
        Assert.assertEquals(channel.getVersion() + 1, retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertTrue(channelConnect.closed());
    }

    public void testFailConn_NotifyStatus_NilWithClosing() {
        Channel channel = new Channel(CHANNEL_ID, 0, ChannelStatus.CLOSING);
        channelConnect.notifyStatus(channel);
        Channel retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.CLOSE, retChannel.getStatus());
        Assert.assertEquals(channel.getVersion() + 1, retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertTrue(channelConnect.closed());
    }

    public void testFailConn_NotifyStatus_NilWithClosed() {
        Channel channel = new Channel(CHANNEL_ID, 0, ChannelStatus.CLOSE);
        channelConnect.notifyStatus(channel);
        Channel retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.CLOSE, retChannel.getStatus());
        Assert.assertEquals(channel.getVersion(), retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertTrue(channelConnect.closed());
    }

    public void testFailConn_NotifyStatus_NilWithTerminated() {
        Channel channel = new Channel(CHANNEL_ID, 0, ChannelStatus.TERMINATED);
        channelConnect.notifyStatus(channel);
        Channel retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.TERMINATED, retChannel.getStatus());
        Assert.assertEquals(channel.getVersion(), retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertTrue(channelConnect.closed());
    }

    public void testFailConn_NotifyStatus_CloseWithClosing() {
        channelConnect.setCurrentChannel(new Channel(CHANNEL_ID, 1, ChannelStatus.CLOSE));
        Channel channel = new Channel(CHANNEL_ID, 1, ChannelStatus.CLOSING);
        channelConnect.notifyStatus(channel);
        Channel retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.CLOSE, retChannel.getStatus());
        Assert.assertEquals(channel.getVersion() + 1, retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertTrue(channelConnect.closed());
    }

    public void testFailConn_NotifyStatus_CloseWithOpen() {
        channelConnect.setCurrentChannel(new Channel(CHANNEL_ID, 1, ChannelStatus.CLOSE));
        Channel channel = new Channel(CHANNEL_ID, 1, ChannelStatus.OPEN);
        channelConnect.notifyStatus(channel);
        Channel retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.CLOSE, retChannel.getStatus());
        Assert.assertEquals(channel.getVersion() + 1, retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertTrue(channelConnect.closed());
    }

    public void testFailConn_NotifyStatus_CloseWithCloseNewVersion() {
        channelConnect.setCurrentChannel(new Channel(CHANNEL_ID, 1, ChannelStatus.CLOSE));
        Channel channel = new Channel(CHANNEL_ID, 2, ChannelStatus.CLOSE);
        channelConnect.notifyStatus(channel);
        Channel retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.CLOSE, retChannel.getStatus());
        Assert.assertEquals(channel.getVersion(), retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertTrue(channelConnect.closed());
    }

    public void testFailConn_NotifyStatus_CloseWithCloseSameVersion() {
        channelConnect.setCurrentChannel(new Channel(CHANNEL_ID, 1, ChannelStatus.CLOSE));
        Channel channel = new Channel(CHANNEL_ID, 1, ChannelStatus.CLOSE);
        channelConnect.notifyStatus(channel);
        Channel retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.CLOSE, retChannel.getStatus());
        Assert.assertEquals(channel.getVersion(), retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertTrue(channelConnect.closed());
    }

    public void testFailConn_NotifyStatus_CloseWithTerminated() {
        channelConnect.setCurrentChannel(new Channel(CHANNEL_ID, 1, ChannelStatus.CLOSE));
        Channel channel = new Channel(CHANNEL_ID, 2, ChannelStatus.TERMINATED);
        channelConnect.notifyStatus(channel);
        Channel retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.TERMINATED, retChannel.getStatus());
        Assert.assertEquals(channel.getVersion(), retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertTrue(channelConnect.closed());
    }

    public void testFailConn_NotifyStatus_CloseWithCloseOldVersion() {
        channelConnect.setCurrentChannel(new Channel(CHANNEL_ID, 1, ChannelStatus.CLOSE));
        Channel channel = new Channel(CHANNEL_ID, 0, ChannelStatus.CLOSE);
        channelConnect.notifyStatus(channel);
        Channel retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.CLOSE, retChannel.getStatus());
        Assert.assertEquals(1, retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertTrue(channelConnect.closed());
    }
}
