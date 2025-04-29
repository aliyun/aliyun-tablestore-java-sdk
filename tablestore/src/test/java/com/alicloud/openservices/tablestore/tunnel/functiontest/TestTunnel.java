package com.alicloud.openservices.tablestore.tunnel.functiontest;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.TunnelClient;
import com.alicloud.openservices.tablestore.model.tunnel.*;

public class TestTunnel {
    private static final String Endpoint = "";
    private static final String AccessId = "";
    private static final String AccessKey = "";
    private static final String InstanceName = "";

    private static final String TableName = "";

    public static void main(String[] args) {
        TunnelClient client = new TunnelClient(Endpoint, AccessId, AccessKey, InstanceName);
        try {
            String tunnelName = "test_zr" + System.currentTimeMillis();

            //  create tunnel
            System.out.println("Begin CreateTunnel");
//            createTunnel(client, tunnelName);
            createTunnelWithBackFill(client, tunnelName);
            System.out.println("++++++++++++++++++++++++++++++++++++");

            //  list tunnel
            System.out.println("Begin ListTunnel");
            listTunnel(client);
            System.out.println("++++++++++++++++++++++++++++++++++++");

            // smoke describe tunnel
            System.out.println("Begin DescribeTunnel");
            describeTunnel(client, tunnelName);
            System.out.println("++++++++++++++++++++++++++++++++++++");

            // delete tunnel
            System.out.println("Begin DeleteTunnel");
            deleteTunnel(client, tunnelName);
            System.out.println("++++++++++++++++++++++++++++++++++++");
        } catch (TableStoreException e) {
            e.printStackTrace();
        } catch (ClientException e) {
            e.printStackTrace();
        } finally {
            client.shutdown();
        }
    }

    static void createTunnel(TunnelClient client, String tunnelName) {
        CreateTunnelRequest request = new CreateTunnelRequest(TableName, tunnelName, TunnelType.BaseData);
        CreateTunnelResponse resp = client.createTunnel(request);
        System.out.println("RequestId: " + resp.getRequestId());
        System.out.println("TunnelId: " + resp.getTunnelId());
    }

    static void createTunnelWithBackFill(TunnelClient client, String tunnelName) {
        CreateTunnelRequest request = new CreateTunnelRequest(TableName, tunnelName, TunnelType.BaseData);
        StreamTunnelConfig streamTunnelConfig = new StreamTunnelConfig();
        streamTunnelConfig.setFlag(StartOffsetFlag.EARLIEST);
        streamTunnelConfig.setEndOffset(System.currentTimeMillis());
        request.setStreamTunnelConfig(streamTunnelConfig);
        CreateTunnelResponse resp = client.createTunnel(request);
        System.out.println("RequestId: " + resp.getRequestId());
        System.out.println("TunnelId: " + resp.getTunnelId());
    }

    static void listTunnel(TunnelClient client) {
        ListTunnelRequest request = new ListTunnelRequest(TableName);
        ListTunnelResponse resp = client.listTunnel(request);
        System.out.println("RequestId: " + resp.getRequestId());
        for (TunnelInfo info : resp.getTunnelInfos()) {
            System.out.println("TunnelInfo::::::");
            System.out.println("\tTunnelName: " + info.getTunnelName());
            System.out.println("\tTunnelId: " + info.getTunnelId());
            System.out.println("\tTunnelType: " + info.getTunnelType());
            System.out.println("\tTableName: " + info.getTableName());
            System.out.println("\tInstanceName: " + info.getInstanceName());
            System.out.println("\tStage: " + info.getStage());
            System.out.println("\tExpired: " + info.isExpired());
        }
    }

    static void describeTunnel(TunnelClient client, String tunnelName) {
        DescribeTunnelRequest request = new DescribeTunnelRequest(TableName, tunnelName);
        DescribeTunnelResponse resp = client.describeTunnel(request);
        System.out.println("RequestId: " + resp.getRequestId());
        System.out.println("TunnelRpo: " + resp.getTunnelRpo() + "ms");
        System.out.println("TunnelConsumePoint: " + resp.getTunnelConsumePoint());
        System.out.println("TunnelInfo::::::");
        TunnelInfo ti = resp.getTunnelInfo();
        System.out.println("\tTunnelName: " + ti.getTunnelName());
        System.out.println("\tTunnelId: " + ti.getTunnelId());
        System.out.println("\tTunnelType: " + ti.getTunnelType());
        System.out.println("\tTableName: " + ti.getTableName());
        System.out.println("\tInstanceName: " + ti.getInstanceName());
        System.out.println("\tStage: " + ti.getStage());
        System.out.println("\tExpired: " + ti.isExpired());
        System.out.println("\tStreamTunnelConfig: " + ti.getStreamTunnelConfig());
        System.out.println("\tCreateTime: " + ti.getCreateTime());
        for (ChannelInfo ci : resp.getChannelInfos()) {
            System.out.println("ChannelInfo::::::");
            System.out.println("\tChannelId: " + ci.getChannelId());
            System.out.println("\tChannelType: " + ci.getChannelType());
            System.out.println("\tClientId: " + ci.getClientId());
            System.out.println("\tChannelRpo: " + ci.getChannelRpo() + "ms");
            System.out.println("\tChannelConsumePoint: " + ci.getChannelConsumePoint());
        }
    }

    static void deleteTunnel(TunnelClient client, String tunnelName) {
        DeleteTunnelRequest request = new DeleteTunnelRequest(TableName, tunnelName);
        DeleteTunnelResponse resp = client.deleteTunnel(request);
        System.out.println("RequestId: " + resp.getRequestId());
    }
}
