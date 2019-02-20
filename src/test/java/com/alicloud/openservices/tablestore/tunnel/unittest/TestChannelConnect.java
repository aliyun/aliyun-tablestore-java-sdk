package com.alicloud.openservices.tablestore.tunnel.unittest;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.alicloud.openservices.tablestore.model.tunnel.ChannelStatus;
import com.alicloud.openservices.tablestore.model.tunnel.internal.Channel;
import com.alicloud.openservices.tablestore.tunnel.worker.ChannelConnect;
import com.alicloud.openservices.tablestore.tunnel.worker.ChannelConnectStatus;
import com.alicloud.openservices.tablestore.tunnel.worker.ChannelDialer;
import com.alicloud.openservices.tablestore.tunnel.worker.TunnelStateMachine;
import com.alicloud.openservices.tablestore.tunnel.worker.TunnelWorkerConfig;
import junit.framework.TestCase;
import org.junit.Assert;

public class TestChannelConnect extends TestCase {
    private static final String TUNNEL_ID = "test-tunnel-id-zr";
    private static final String CLIENT_ID = "test-client-id-zr";

    private static final String CHANNEL_ID = "test-channel-id-zr";
    private static final int SLEEP_MILLIS = 2000;

    private ChannelConnect channelConnect;
    private TunnelWorkerConfig config;
    private ChannelDialer channelDialer;

    @Override
    protected void setUp() throws Exception {
        config = new TunnelWorkerConfig(new MockChannelProcessor());
        config.setReadRecordsExecutor((ThreadPoolExecutor)Executors.newFixedThreadPool(1));
        config.setProcessRecordsExecutor((ThreadPoolExecutor)Executors.newFixedThreadPool(1));
        channelDialer = new ChannelDialer(new MockTunnelClient(), config);
        channelConnect = (ChannelConnect)channelDialer.channelDial(TUNNEL_ID, CLIENT_ID, CHANNEL_ID, "token",
            config.getChannelProcessor(), new TunnelStateMachine(TUNNEL_ID, CLIENT_ID, channelDialer,
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

    private void sleepGrace() {
        try {
            Thread.sleep(SLEEP_MILLIS);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testChannelConnect_NotifyStatus_NilWithOpen() {
        Channel channel = new Channel(CHANNEL_ID, 0, ChannelStatus.OPEN);
        channelConnect.notifyStatus(channel);
        Channel retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.OPEN, retChannel.getStatus());
        Assert.assertEquals(channel.getVersion(), retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertEquals(ChannelConnectStatus.RUNNING, channelConnect.getStatus());
    }

    public void testChannelConnect_NotifyStatus_NilWithClose() {
        Channel channel = new Channel(CHANNEL_ID, 0, ChannelStatus.CLOSE);
        channelConnect.notifyStatus(channel);
        Channel retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.CLOSE, retChannel.getStatus());
        Assert.assertEquals(channel.getVersion(), retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertEquals(ChannelConnectStatus.CLOSED, channelConnect.getStatus());
    }

    public void testChannelConnect_NotifyStatus_NilWithClosing() {
        Channel channel = new Channel(CHANNEL_ID, 0, ChannelStatus.CLOSING);
        channelConnect.notifyStatus(channel);
        Channel retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.CLOSE, retChannel.getStatus());
        Assert.assertEquals(channel.getVersion() + 1, retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertEquals(ChannelConnectStatus.CLOSED, channelConnect.getStatus());
    }

    public void testChannelConnect_NotifyStatus_NilWithTerminated() {
        Channel channel = new Channel(CHANNEL_ID, 0, ChannelStatus.TERMINATED);
        channelConnect.notifyStatus(channel);
        Channel retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.TERMINATED, retChannel.getStatus());
        Assert.assertEquals(channel.getVersion(), retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertEquals(ChannelConnectStatus.CLOSED, channelConnect.getStatus());
    }

    public void testChannelConnect_NotifyStatus_ClosedWithClosed() {
        channelConnect.notifyStatus(new Channel(CHANNEL_ID, 1, ChannelStatus.CLOSE));
        Channel channel = new Channel(CHANNEL_ID, 1, ChannelStatus.CLOSE);
        channelConnect.notifyStatus(channel);
        Channel retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.CLOSE, retChannel.getStatus());
        Assert.assertEquals(channel.getVersion(), retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertEquals(ChannelConnectStatus.CLOSED, channelConnect.getStatus());
    }

    public void testChannelConnect_NotifyStatus_ClosedWithClosing() {
        channelConnect.notifyStatus(new Channel(CHANNEL_ID, 1, ChannelStatus.CLOSE));
        Channel channel = new Channel(CHANNEL_ID, 1, ChannelStatus.CLOSING);
        channelConnect.notifyStatus(channel);
        Channel retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.CLOSE, retChannel.getStatus());
        Assert.assertEquals(channel.getVersion() + 1, retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertEquals(ChannelConnectStatus.CLOSED, channelConnect.getStatus());
    }

    public void testChannelConnect_NotifyStatus_ClosedWithOpen() {
        channelConnect.notifyStatus(new Channel(CHANNEL_ID, 1, ChannelStatus.CLOSE));
        Channel channel = new Channel(CHANNEL_ID, 1, ChannelStatus.OPEN);
        channelConnect.notifyStatus(channel);
        Channel retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.CLOSE, retChannel.getStatus());
        Assert.assertEquals(channel.getVersion() + 1, retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertEquals(ChannelConnectStatus.CLOSED, channelConnect.getStatus());
    }

    public void testChannelConnect_NotifyStatus_ClosedWithTerminated() {
        channelConnect.notifyStatus(new Channel(CHANNEL_ID, 1, ChannelStatus.CLOSE));
        Channel channel = new Channel(CHANNEL_ID, 1, ChannelStatus.TERMINATED);
        channelConnect.notifyStatus(channel);
        Channel retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.TERMINATED, retChannel.getStatus());
        Assert.assertEquals(channel.getVersion(), retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertEquals(ChannelConnectStatus.CLOSED, channelConnect.getStatus());
    }

    public void testChannelConnect_NotifyStatus_ClosingWithClose() {
        channelConnect.notifyStatus(new Channel(CHANNEL_ID, 0, ChannelStatus.OPEN));
        sleepGrace();
        channelConnect.notifyStatus(new Channel(CHANNEL_ID, 1, ChannelStatus.CLOSING));
        Channel channel = new Channel(CHANNEL_ID, 1, ChannelStatus.CLOSE);
        channelConnect.notifyStatus(channel);
        sleepGrace();
        channelConnect.checkAndUpdateChannelStatus();
        Channel retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.CLOSE, retChannel.getStatus());
        Assert.assertEquals(channel.getVersion() + 1, retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertEquals(ChannelConnectStatus.CLOSED, channelConnect.getStatus());
    }

    public void testChannelConnect_NotifyStatus_ClosingWithClosing() {
        channelConnect.notifyStatus(new Channel(CHANNEL_ID, 0, ChannelStatus.OPEN));
        sleepGrace();
        channelConnect.notifyStatus(new Channel(CHANNEL_ID, 1, ChannelStatus.CLOSING));
        Channel channel = new Channel(CHANNEL_ID, 1, ChannelStatus.CLOSING);
        channelConnect.notifyStatus(channel);
        sleepGrace();
        channelConnect.checkAndUpdateChannelStatus();
        Channel retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.CLOSE, retChannel.getStatus());
        Assert.assertEquals(channel.getVersion() + 1, retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertEquals(ChannelConnectStatus.CLOSED, channelConnect.getStatus());
    }

    public void testChannelConnect_NotifyStatus_ClosingWithOpen() {
        channelConnect.notifyStatus(new Channel(CHANNEL_ID, 0, ChannelStatus.OPEN));
        sleepGrace();
        channelConnect.notifyStatus(new Channel(CHANNEL_ID, 1, ChannelStatus.CLOSING));
        System.out.println(channelConnect);
        Channel channel = new Channel(CHANNEL_ID, 1, ChannelStatus.OPEN);
        channelConnect.notifyStatus(channel);
        sleepGrace();
        channelConnect.checkAndUpdateChannelStatus();
        Channel retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.CLOSE, retChannel.getStatus());
        Assert.assertEquals(channel.getVersion() + 1, retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertEquals(ChannelConnectStatus.CLOSED, channelConnect.getStatus());
    }

    public void testChannelConnect_NotifyStatus_ClosingWithTerminated() {
        channelConnect.notifyStatus(new Channel(CHANNEL_ID, 0, ChannelStatus.OPEN));
        sleepGrace();
        channelConnect.notifyStatus(new Channel(CHANNEL_ID, 1, ChannelStatus.CLOSING));
        Channel channel = new Channel(CHANNEL_ID, 1, ChannelStatus.TERMINATED);
        channelConnect.notifyStatus(channel);
        sleepGrace();
        channelConnect.checkAndUpdateChannelStatus();
        Channel retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.TERMINATED, retChannel.getStatus());
        Assert.assertEquals(channel.getVersion() + 1, retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertEquals(ChannelConnectStatus.CLOSED, channelConnect.getStatus());
    }

    public void testChannelConnect_NotifyStatus_ClosingWithClosingOld() {
        channelConnect.notifyStatus(new Channel(CHANNEL_ID, 0, ChannelStatus.OPEN));
        sleepGrace();
        channelConnect.notifyStatus(new Channel(CHANNEL_ID, 1, ChannelStatus.CLOSING));
        Channel channel = new Channel(CHANNEL_ID, 0, ChannelStatus.CLOSING);
        channelConnect.notifyStatus(channel);
        sleepGrace();
        channelConnect.checkAndUpdateChannelStatus();
        Channel retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.CLOSE, retChannel.getStatus());
        Assert.assertEquals(2, retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertEquals(ChannelConnectStatus.CLOSED, channelConnect.getStatus());
    }

    public void testChannelConnect_NotifyStatus_OpenWithClose() {
        channelConnect.notifyStatus(new Channel(CHANNEL_ID, 1, ChannelStatus.OPEN));
        Channel channel = new Channel(CHANNEL_ID, 1, ChannelStatus.CLOSE);
        channelConnect.notifyStatus(channel);
        Channel retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.CLOSE, retChannel.getStatus());
        Assert.assertEquals(channel.getVersion(), retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertEquals(ChannelConnectStatus.CLOSED, channelConnect.getStatus());
        sleepGrace();
        channelConnect.checkAndUpdateChannelStatus();
        retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.CLOSE, retChannel.getStatus());
        Assert.assertEquals(channel.getVersion() + 1, retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertEquals(ChannelConnectStatus.CLOSED, channelConnect.getStatus());
    }

    public void testChannelConnect_NotifyStatus_OpenWithClosing() {
        channelConnect.notifyStatus(new Channel(CHANNEL_ID, 1, ChannelStatus.OPEN));
        Channel channel = new Channel(CHANNEL_ID, 1, ChannelStatus.CLOSING);
        channelConnect.notifyStatus(channel);
        Channel retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.CLOSING, retChannel.getStatus());
        Assert.assertEquals(channel.getVersion(), retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertEquals(ChannelConnectStatus.CLOSING, channelConnect.getStatus());
        sleepGrace();
        channelConnect.checkAndUpdateChannelStatus();
        retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.CLOSE, retChannel.getStatus());
        Assert.assertEquals(channel.getVersion() + 1, retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertEquals(ChannelConnectStatus.CLOSED, channelConnect.getStatus());
    }

    public void testChannelConnect_NotifyStatus_OpenWithOpen() {
        channelConnect.notifyStatus(new Channel(CHANNEL_ID, 1, ChannelStatus.OPEN));
        Channel channel = new Channel(CHANNEL_ID, 1, ChannelStatus.OPEN);
        channelConnect.notifyStatus(channel);
        Channel retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.OPEN, retChannel.getStatus());
        Assert.assertEquals(channel.getVersion(), retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertEquals(ChannelConnectStatus.RUNNING, channelConnect.getStatus());
        sleepGrace();
        channelConnect.checkAndUpdateChannelStatus();
        retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.OPEN, retChannel.getStatus());
        Assert.assertEquals(channel.getVersion(), retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertEquals(ChannelConnectStatus.RUNNING, channelConnect.getStatus());
    }

    public void testChannelConnect_NotifyStatus_OpenWithTerminated() {
        channelConnect.notifyStatus(new Channel(CHANNEL_ID, 1, ChannelStatus.OPEN));
        Channel channel = new Channel(CHANNEL_ID, 1, ChannelStatus.TERMINATED);
        channelConnect.notifyStatus(channel);
        Channel retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.TERMINATED, retChannel.getStatus());
        Assert.assertEquals(channel.getVersion(), retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertEquals(ChannelConnectStatus.CLOSED, channelConnect.getStatus());
        sleepGrace();
        channelConnect.checkAndUpdateChannelStatus();
        retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.TERMINATED, retChannel.getStatus());
        Assert.assertEquals(channel.getVersion() + 1, retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertEquals(ChannelConnectStatus.CLOSED, channelConnect.getStatus());
    }

    public void testChannelConnect_NotifyStatus_OpenWithClosingOld() {
        channelConnect.notifyStatus(new Channel(CHANNEL_ID, 1, ChannelStatus.OPEN));
        Channel channel = new Channel(CHANNEL_ID, 0, ChannelStatus.CLOSING);
        channelConnect.notifyStatus(channel);
        Channel retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.OPEN, retChannel.getStatus());
        Assert.assertEquals(1, retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertEquals(ChannelConnectStatus.RUNNING, channelConnect.getStatus());
        sleepGrace();
        channelConnect.checkAndUpdateChannelStatus();
        retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.OPEN, retChannel.getStatus());
        Assert.assertEquals(1, retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertEquals(ChannelConnectStatus.RUNNING, channelConnect.getStatus());
    }

    public void testChannelConnect_NotifyStatus_TerminatedWithClose() {
        channelConnect.notifyStatus(new Channel(CHANNEL_ID, 1, ChannelStatus.TERMINATED));
        Channel channel = new Channel(CHANNEL_ID, 1, ChannelStatus.CLOSE);
        channelConnect.notifyStatus(channel);
        Channel retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.CLOSE, retChannel.getStatus());
        Assert.assertEquals(channel.getVersion(), retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertEquals(ChannelConnectStatus.CLOSED, channelConnect.getStatus());
    }

    public void testChannelConnect_NotifyStatus_TerminatedWithClosing() {
        channelConnect.notifyStatus(new Channel(CHANNEL_ID, 1, ChannelStatus.TERMINATED));
        Channel channel = new Channel(CHANNEL_ID, 1, ChannelStatus.CLOSING);
        channelConnect.notifyStatus(channel);
        Channel retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.TERMINATED, retChannel.getStatus());
        Assert.assertEquals(channel.getVersion() + 1, retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertEquals(ChannelConnectStatus.CLOSED, channelConnect.getStatus());
    }

    public void testChannelConnect_NotifyStatus_TerminatedWithOpen() {
        channelConnect.notifyStatus(new Channel(CHANNEL_ID, 1, ChannelStatus.TERMINATED));
        Channel channel = new Channel(CHANNEL_ID, 1, ChannelStatus.OPEN);
        channelConnect.notifyStatus(channel);
        Channel retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.TERMINATED, retChannel.getStatus());
        Assert.assertEquals(channel.getVersion() + 1, retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertEquals(ChannelConnectStatus.CLOSED, channelConnect.getStatus());
    }

    public void testChannelConnect_NotifyStatus_TerminatedWithTerminated() {
        channelConnect.notifyStatus(new Channel(CHANNEL_ID, 1, ChannelStatus.TERMINATED));
        Channel channel = new Channel(CHANNEL_ID, 1, ChannelStatus.TERMINATED);
        channelConnect.notifyStatus(channel);
        Channel retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.TERMINATED, retChannel.getStatus());
        Assert.assertEquals(channel.getVersion(), retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertEquals(ChannelConnectStatus.CLOSED, channelConnect.getStatus());
    }

    public void testChannelConnect_NotifyStatus_TerminatedWithClosingOld() {
        channelConnect.notifyStatus(new Channel(CHANNEL_ID, 1, ChannelStatus.TERMINATED));
        Channel channel = new Channel(CHANNEL_ID, 0, ChannelStatus.CLOSING);
        channelConnect.notifyStatus(channel);
        Channel retChannel = channelConnect.getCurrentChannel();
        Assert.assertEquals(ChannelStatus.TERMINATED, retChannel.getStatus());
        Assert.assertEquals(1, retChannel.getVersion());
        Assert.assertEquals(channel.getChannelId(), retChannel.getChannelId());
        Assert.assertEquals(ChannelConnectStatus.CLOSED, channelConnect.getStatus());
    }
}
