package com.alicloud.openservices.tablestore.tunnel.functiontest;

import com.alicloud.openservices.tablestore.TunnelClient;
import com.alicloud.openservices.tablestore.model.tunnel.*;
import junit.framework.TestCase;
import org.junit.Assert;

public class TestBackFillTunnel extends TestCase {
    private static final String ENDPOINT = "http://zhuoran-test.ali-cn-hangzhou.ots.aliyuncs.com";
    private static final String ACCESS_ID = "";
    private static final String ACCESS_KEY = "";
    private static final String INSTANCE_NAME = "zhuoran-test";
    private static final String TABLE_NAME = "table_test";

    private TunnelClient client;

    @Override
    public void setUp() throws Exception {
        client = new TunnelClient(ENDPOINT, ACCESS_ID, ACCESS_KEY, INSTANCE_NAME);
    }

    @Override
    public void tearDown() throws Exception {
        client.shutdown();
        client = null;
    }

    public void testCreateTunnelWithoutBackFill() {
        String tunnelName = "test_zr" + System.currentTimeMillis();
        try {
            client.createTunnel(new CreateTunnelRequest(TABLE_NAME, tunnelName, TunnelType.Stream));

            DescribeTunnelResponse describeResp = client.describeTunnel(new DescribeTunnelRequest(TABLE_NAME, tunnelName));
            StreamTunnelConfig streamTunnelConfig = describeResp.getTunnelInfo().getStreamTunnelConfig();
            Assert.assertNotNull(streamTunnelConfig);
            Assert.assertEquals(StartOffsetFlag.LATEST, streamTunnelConfig.getFlag());
            Assert.assertEquals(0, streamTunnelConfig.getStartOffset());
            Assert.assertEquals(0, streamTunnelConfig.getEndOffset());
        } finally {
            client.deleteTunnel(new DeleteTunnelRequest(TABLE_NAME, tunnelName));
        }
    }

    public void testCreateTunnelWithDefaultBackFill() {
        String tunnelName = "test_zr" + System.currentTimeMillis();
        try {
            CreateTunnelRequest createRequest = new CreateTunnelRequest(TABLE_NAME, tunnelName, TunnelType.Stream);
            createRequest.setStreamTunnelConfig(new StreamTunnelConfig());
            client.createTunnel(createRequest);

            DescribeTunnelResponse describeResp = client.describeTunnel(new DescribeTunnelRequest(TABLE_NAME, tunnelName));
            StreamTunnelConfig streamTunnelConfig = describeResp.getTunnelInfo().getStreamTunnelConfig();
            Assert.assertNotNull(streamTunnelConfig);
            Assert.assertEquals(StartOffsetFlag.LATEST, streamTunnelConfig.getFlag());
            Assert.assertEquals(0, streamTunnelConfig.getStartOffset());
            Assert.assertEquals(0, streamTunnelConfig.getEndOffset());
        } finally {
            client.deleteTunnel(new DeleteTunnelRequest(TABLE_NAME, tunnelName));
        }
    }

    public void testCreateTunnelWithEarliestBackFill() {
        String tunnelName = "test_zr" + System.currentTimeMillis();
        try {
            CreateTunnelRequest createRequest = new CreateTunnelRequest(TABLE_NAME, tunnelName, TunnelType.Stream);
            createRequest.setStreamTunnelConfig(new StreamTunnelConfig(StartOffsetFlag.EARLIEST));
            client.createTunnel(createRequest);

            DescribeTunnelResponse describeResp = client.describeTunnel(new DescribeTunnelRequest(TABLE_NAME, tunnelName));
            StreamTunnelConfig streamTunnelConfig = describeResp.getTunnelInfo().getStreamTunnelConfig();
            Assert.assertNotNull(streamTunnelConfig);
            Assert.assertEquals(StartOffsetFlag.EARLIEST, streamTunnelConfig.getFlag());
            Assert.assertEquals(0, streamTunnelConfig.getStartOffset());
            Assert.assertEquals(0, streamTunnelConfig.getEndOffset());
        } finally {
            client.deleteTunnel(new DeleteTunnelRequest(TABLE_NAME, tunnelName));
        }
    }

    public void testCreateTunnelWithRangeBackFill() {
        String tunnelName = "test_zr" + System.currentTimeMillis();
        try {
            CreateTunnelRequest createRequest = new CreateTunnelRequest(TABLE_NAME, tunnelName, TunnelType.Stream);
            long beginTime = System.currentTimeMillis();
            long endTime = beginTime + 3600000;
            createRequest.setStreamTunnelConfig(new StreamTunnelConfig(beginTime, endTime));
            client.createTunnel(createRequest);

            DescribeTunnelResponse describeResp = client.describeTunnel(new DescribeTunnelRequest(TABLE_NAME, tunnelName));
            StreamTunnelConfig streamTunnelConfig = describeResp.getTunnelInfo().getStreamTunnelConfig();
            Assert.assertNotNull(streamTunnelConfig);
            Assert.assertEquals(StartOffsetFlag.LATEST, streamTunnelConfig.getFlag());
            Assert.assertEquals(beginTime, streamTunnelConfig.getStartOffset());
            Assert.assertEquals(endTime, streamTunnelConfig.getEndOffset());
        } finally {
            client.deleteTunnel(new DeleteTunnelRequest(TABLE_NAME, tunnelName));
        }
    }

    public void testCreateTunnelWithInvalidBackFill() {
        String tunnelName = "test_zr" + System.currentTimeMillis();
        try {
            CreateTunnelRequest createRequest = new CreateTunnelRequest(TABLE_NAME, tunnelName, TunnelType.Stream);
            long beginTime = System.currentTimeMillis();
            long endTime = beginTime - 3600000; // less than beginTime
            createRequest.setStreamTunnelConfig(new StreamTunnelConfig(beginTime, endTime));
            client.createTunnel(createRequest);
            // cannot achieve below logic.
            Assert.fail();
        } catch (Exception e) {
            System.out.println(e);
        } finally {
            // do nothing.
        }
    }

}
