package com.alicloud.openservices.tablestore.tunnel.unittest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.alicloud.openservices.tablestore.model.tunnel.ChannelStatus;
import com.alicloud.openservices.tablestore.model.tunnel.internal.Channel;
import com.alicloud.openservices.tablestore.tunnel.worker.ChannelDialer;
import com.alicloud.openservices.tablestore.tunnel.worker.IChannelConnect;
import com.alicloud.openservices.tablestore.tunnel.worker.TunnelStateMachine;
import com.alicloud.openservices.tablestore.tunnel.worker.TunnelWorkerConfig;
import junit.framework.TestCase;


public class TestTunnelStateMachine extends TestCase {
    private static final String TUNNEL_ID = "test-tunnel-id-zr";
    private static final String CLIENT_ID = "test-client-id-zr";

    private static final String CHANNEL_ID = "test-channel-id-zr";
    private static final int SLEEP_MILLIS = 2000;

    private TunnelWorkerConfig config;
    private TunnelStateMachine stateMachine;
    private ChannelDialer channelDialer;

    @Override
    protected void setUp() throws Exception {
        System.out.println("begin setup");
        MockTunnelClient.finishedChannels = new ArrayList<String>();
        config = new TunnelWorkerConfig(new MockChannelProcessor());
        config.setReadRecordsExecutor((ThreadPoolExecutor)Executors.newFixedThreadPool(1));
        config.setProcessRecordsExecutor((ThreadPoolExecutor)Executors.newFixedThreadPool(1));
        channelDialer = new ChannelDialer(new MockTunnelClient(), config);
        stateMachine = new TunnelStateMachine(TUNNEL_ID, CLIENT_ID, channelDialer, new MockChannelProcessorFactory(),
            new MockTunnelClient());
    }

    @Override
    protected void tearDown() throws Exception {
        System.out.println("begin teardown");
        config.shutdown();
        config = null;
        stateMachine.close();
        channelDialer.shutdown();
    }

    private HashMap<String, Channel> toMap(List<Channel> channels) {
        HashMap<String, Channel> channelHashMap = new HashMap<String, Channel>();
        for (Channel channel : channels) {
            channelHashMap.put(channel.getChannelId(), channel);
        }
        return channelHashMap;
    }

    private int getOpenChannelCount(List<IChannelConnect> connects) {
        int count = 0;
        for (IChannelConnect connect : connects) {
            if (!connect.closed()) {
                count++;
            }
        }
        return count;
    }

    private boolean channelEquals(Channel lc, Channel rc) {
        assertEquals(lc.getChannelId(), rc.getChannelId());
        assertEquals(lc.getVersion(), rc.getVersion());
        assertEquals(lc.getStatus(), rc.getStatus());
        return true;
    }

    private void sleepGrace() {
        try {
            Thread.sleep(SLEEP_MILLIS);
        } catch (Exception e) {
            System.out.println(e.getCause() + ":" + e.getMessage());
        }
    }

    public void testBatchUpdateStatus() {
        // step1: two open, two closing
        List<Channel> heartbeatChannels = Arrays.asList(
            new Channel("cid-1", 0, ChannelStatus.OPEN),
            new Channel("cid-2", 0, ChannelStatus.OPEN),
            new Channel("cid-3", 1, ChannelStatus.CLOSING),
            new Channel("cid-4", 1, ChannelStatus.CLOSING));
        stateMachine.batchUpdateChannels(heartbeatChannels);
        Map<String, Channel> targetChannelMap = toMap(stateMachine.batchGetChannels());
        assertTrue(channelEquals(new Channel("cid-1", 0, ChannelStatus.OPEN), targetChannelMap.get("cid-1")));
        assertTrue(channelEquals(new Channel("cid-2", 0, ChannelStatus.OPEN), targetChannelMap.get("cid-2")));
        assertTrue(channelEquals(new Channel("cid-3", 2, ChannelStatus.CLOSE), targetChannelMap.get("cid-3")));
        assertTrue(channelEquals(new Channel("cid-4", 2, ChannelStatus.CLOSE), targetChannelMap.get("cid-4")));
        assertEquals(2, getOpenChannelCount(stateMachine.batchGetChannelConnects()));

        // step2: one open, one closing, two new
        heartbeatChannels = Arrays.asList(
            new Channel("cid-1", 0, ChannelStatus.OPEN),
            new Channel("cid-2", 1, ChannelStatus.CLOSING),
            new Channel("cid-4", 2, ChannelStatus.OPEN),
            new Channel("cid-5", 0, ChannelStatus.OPEN));
        stateMachine.batchUpdateChannels(heartbeatChannels);
        sleepGrace();
        stateMachine.batchUpdateChannels(heartbeatChannels);
        targetChannelMap = toMap(stateMachine.batchGetChannels());
        assertTrue(channelEquals(new Channel("cid-1", 0, ChannelStatus.OPEN), targetChannelMap.get("cid-1")));
        assertTrue(channelEquals(new Channel("cid-2", 2, ChannelStatus.CLOSE), targetChannelMap.get("cid-2")));
        assertTrue(channelEquals(new Channel("cid-4", 2, ChannelStatus.OPEN), targetChannelMap.get("cid-4")));
        assertTrue(channelEquals(new Channel("cid-5", 0, ChannelStatus.OPEN), targetChannelMap.get("cid-5")));
        assertEquals(3, getOpenChannelCount(stateMachine.batchGetChannelConnects()));

        // step3: three open, one bad new, one new
        heartbeatChannels = Arrays.asList(
            new Channel("cid-1", 0, ChannelStatus.OPEN),
            new Channel("cid-4", 2, ChannelStatus.OPEN),
            new Channel("cid-5", 0, ChannelStatus.OPEN),
            new Channel("cid-bad", 0, ChannelStatus.OPEN),
            new Channel("cid-6", 0, ChannelStatus.OPEN));
        stateMachine.batchUpdateChannels(heartbeatChannels);
        sleepGrace();
        stateMachine.batchUpdateChannels(heartbeatChannels);
        targetChannelMap = toMap(stateMachine.batchGetChannels());
        assertTrue(channelEquals(new Channel("cid-1", 0, ChannelStatus.OPEN), targetChannelMap.get("cid-1")));
        assertTrue(channelEquals(new Channel("cid-4", 2, ChannelStatus.OPEN), targetChannelMap.get("cid-4")));
        assertTrue(channelEquals(new Channel("cid-5", 0, ChannelStatus.OPEN), targetChannelMap.get("cid-5")));
        assertTrue(channelEquals(new Channel("cid-bad", 1, ChannelStatus.CLOSE), targetChannelMap.get("cid-bad")));
        assertTrue(channelEquals(new Channel("cid-6", 0, ChannelStatus.OPEN), targetChannelMap.get("cid-6")));
        assertEquals(4, getOpenChannelCount(stateMachine.batchGetChannelConnects()));

        // step4: four open, one old version.
        heartbeatChannels = Arrays.asList(
            new Channel("cid-1", 0, ChannelStatus.OPEN),
            new Channel("cid-4", 1, ChannelStatus.CLOSING),
            new Channel("cid-5", 0, ChannelStatus.OPEN),
            new Channel("cid-6", 0, ChannelStatus.OPEN));
        stateMachine.batchUpdateChannels(heartbeatChannels);
        sleepGrace();
        stateMachine.batchUpdateChannels(heartbeatChannels);
        targetChannelMap = toMap(stateMachine.batchGetChannels());
        assertTrue(channelEquals(new Channel("cid-1", 0, ChannelStatus.OPEN), targetChannelMap.get("cid-1")));
        assertTrue(channelEquals(new Channel("cid-4", 2, ChannelStatus.OPEN), targetChannelMap.get("cid-4")));
        assertTrue(channelEquals(new Channel("cid-5", 0, ChannelStatus.OPEN), targetChannelMap.get("cid-5")));
        assertTrue(channelEquals(new Channel("cid-6", 0, ChannelStatus.OPEN), targetChannelMap.get("cid-6")));
        assertEquals(4, getOpenChannelCount(stateMachine.batchGetChannelConnects()));

        // step5: four open, one finished
        heartbeatChannels = Arrays.asList(
            new Channel("cid-1", 0, ChannelStatus.OPEN),
            new Channel("cid-4", 2, ChannelStatus.OPEN),
            new Channel("cid-5", 0, ChannelStatus.OPEN),
            new Channel("cid-6", 0, ChannelStatus.OPEN));
        stateMachine.batchUpdateChannels(heartbeatChannels);
        MockTunnelClient.finishedChannels = Arrays.asList("cid-6");
        sleepGrace();
        stateMachine.batchUpdateChannels(heartbeatChannels);
        targetChannelMap = toMap(stateMachine.batchGetChannels());
        assertTrue(channelEquals(new Channel("cid-1", 0, ChannelStatus.OPEN), targetChannelMap.get("cid-1")));
        assertTrue(channelEquals(new Channel("cid-4", 2, ChannelStatus.OPEN), targetChannelMap.get("cid-4")));
        assertTrue(channelEquals(new Channel("cid-5", 0, ChannelStatus.OPEN), targetChannelMap.get("cid-5")));
        assertTrue(channelEquals(new Channel("cid-6", 1, ChannelStatus.TERMINATED), targetChannelMap.get("cid-6")));
        assertEquals(3, getOpenChannelCount(stateMachine.batchGetChannelConnects()));
        MockTunnelClient.finishedChannels = new ArrayList<String>();

        // step6: three open, somehow one missing(cid-5), one failconn
        heartbeatChannels = Arrays.asList(
            new Channel("cid-1", 0, ChannelStatus.OPEN),
            new Channel("cid-4", 2, ChannelStatus.OPEN),
            new Channel("cid-getCheckpointFailed", 1, ChannelStatus.OPEN));
        stateMachine.batchUpdateChannels(heartbeatChannels);
        sleepGrace();
        stateMachine.batchUpdateChannels(heartbeatChannels);
        targetChannelMap = toMap(stateMachine.batchGetChannels());
        assertTrue(channelEquals(new Channel("cid-1", 0, ChannelStatus.OPEN), targetChannelMap.get("cid-1")));
        assertTrue(channelEquals(new Channel("cid-4", 2, ChannelStatus.OPEN), targetChannelMap.get("cid-4")));
        assertTrue(channelEquals(new Channel("cid-getCheckpointFailed", 2, ChannelStatus.CLOSE),
            targetChannelMap.get("cid-getCheckpointFailed")));
        assertEquals(2, getOpenChannelCount(stateMachine.batchGetChannelConnects()));

        // step7: all finished.
        heartbeatChannels = Arrays.asList(
            new Channel("cid-1", 0, ChannelStatus.OPEN),
            new Channel("cid-4", 2, ChannelStatus.OPEN));
        stateMachine.batchUpdateChannels(heartbeatChannels);
        MockTunnelClient.finishedChannels = Arrays.asList("cid-1", "cid-4");
        sleepGrace();
        stateMachine.batchUpdateChannels(heartbeatChannels);
        targetChannelMap = toMap(stateMachine.batchGetChannels());
        assertTrue(channelEquals(new Channel("cid-1", 1, ChannelStatus.TERMINATED), targetChannelMap.get("cid-1")));
        assertTrue(channelEquals(new Channel("cid-4", 3, ChannelStatus.TERMINATED), targetChannelMap.get("cid-4")));
        assertEquals(0, getOpenChannelCount(stateMachine.batchGetChannelConnects()));
        MockTunnelClient.finishedChannels = new ArrayList<String>();
    }

    public void testUpdateStatus() {
        List<Channel> heartbeatChannels = Arrays.asList(
            new Channel("cid-0", 0, ChannelStatus.OPEN),
            new Channel("cid-1", 0, ChannelStatus.OPEN),
            new Channel("cid-2", 0, ChannelStatus.OPEN),
            new Channel("cid-3", 0, ChannelStatus.OPEN));
        stateMachine.batchUpdateChannels(heartbeatChannels);
        sleepGrace();
        HashMap<String, Channel> targetChannelMap;

        // 1. update redundant channel
        System.out.println(stateMachine.batchGetChannelConnects());
        stateMachine.updateStatus(new Channel("cidNotExist", 1, ChannelStatus.OPEN));
        assertEquals(4, getOpenChannelCount(stateMachine.batchGetChannelConnects()));

        // 2. update channel to high version
        stateMachine.updateStatus(new Channel("cid-1", 1, ChannelStatus.OPEN));
        assertEquals(4, getOpenChannelCount(stateMachine.batchGetChannelConnects()));
        targetChannelMap = toMap(stateMachine.batchGetChannels());
        channelEquals(new Channel("cid-1", 1, ChannelStatus.OPEN), targetChannelMap.get("cid-1"));

        // 3. update channel to low version.
        stateMachine.updateStatus(new Channel("cid-1", 0, ChannelStatus.OPEN));
        assertEquals(4, getOpenChannelCount(stateMachine.batchGetChannelConnects()));
        targetChannelMap = toMap(stateMachine.batchGetChannels());
        channelEquals(new Channel("cid-1", 1, ChannelStatus.OPEN), targetChannelMap.get("cid-1"));

        // 4. update channel with closed conn
        stateMachine.updateStatus(new Channel("cid-1", 2, ChannelStatus.CLOSE));
        assertEquals(4, getOpenChannelCount(stateMachine.batchGetChannelConnects()));
        targetChannelMap = toMap(stateMachine.batchGetChannels());
        channelEquals(new Channel("cid-1", 2, ChannelStatus.CLOSE), targetChannelMap.get("cid-1"));
    }
}
