package com.alicloud.openservices.tablestore.tunnel.functiontest;

import com.alicloud.openservices.tablestore.TunnelClient;
import com.alicloud.openservices.tablestore.model.tunnel.CreateTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.CreateTunnelResponse;
import com.alicloud.openservices.tablestore.model.tunnel.DeleteTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.DescribeTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.DescribeTunnelResponse;
import com.alicloud.openservices.tablestore.model.tunnel.TunnelStage;
import com.alicloud.openservices.tablestore.model.tunnel.TunnelType;
import com.alicloud.openservices.tablestore.tunnel.worker.IChannelProcessor;
import com.alicloud.openservices.tablestore.tunnel.worker.ProcessRecordsInput;
import com.alicloud.openservices.tablestore.tunnel.worker.TunnelWorker;
import com.alicloud.openservices.tablestore.tunnel.worker.TunnelWorkerConfig;

public class TestTunnelWorker {
    private static final String Endpoint = "";
    private static final String AccessId = "";
    private static final String AccessKey = "";
    private static final String InstanceName = "";
    private static final String TableName = "";

    static class SimpleProcessor implements IChannelProcessor {
        @Override
        public void process(ProcessRecordsInput input) {
            System.out.println("Default record processor, would print records count");
            System.out.println(
                String.format("Process %d records, NextToken: %s", input.getRecords().size(), input.getNextToken()));
            try {
                // Mock Record Process.
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void shutdown() {
            System.out.println("Mock shutdown");
        }
    }

    public static void main(String[] args) {
        TunnelClient client = new TunnelClient(Endpoint, AccessId, AccessKey, InstanceName);
        TunnelWorkerConfig config = new TunnelWorkerConfig(new SimpleProcessor());
        String tunnelName1 = "test-tunnel-1";
        CreateTunnelResponse createTunnelResponse =
            client.createTunnel(new CreateTunnelRequest(TableName, tunnelName1, TunnelType.BaseData));
        String tunnelId1 = createTunnelResponse.getTunnelId();
        TunnelWorker worker1 = new TunnelWorker(tunnelId1, client, config);
        try {
            System.out.println("worker running....");
            worker1.connectAndWorking();
            // 这里会等待一段时间，保证测试的全量可以消费完，具体实现时，可以后台启一个线程进行定期的查询和判断。
            Thread.sleep(120000);
            DescribeTunnelResponse resp = client.describeTunnel(new DescribeTunnelRequest(TableName, tunnelName1));
            if (TunnelStage.ProcessStream.equals(resp.getTunnelInfo().getStage())) {
                System.out.println("Base data consume finished, shutdown worker.");
                worker1.shutdown();
                System.out.println("Delete Tunnel " + tunnelId1);
                client.deleteTunnel(new DeleteTunnelRequest(TableName, tunnelName1));
            }
            System.out.println("worker1 shutdown....");
        } catch (Exception e) {
            e.printStackTrace();
            worker1.shutdown();
        }

        // 再创建一个新的Tunnel, 进行消费。
        String tunnelName2 = "test-tunnel-2";
        CreateTunnelResponse createTunnelResponse2 =
            client.createTunnel(new CreateTunnelRequest(TableName, tunnelName2, TunnelType.BaseData));
        String tunnelId2 = createTunnelResponse2.getTunnelId();
        TunnelWorker worker2 = new TunnelWorker(tunnelId2, client, config);
        try {
            System.out.println("worker2 running...");
            worker2.connectAndWorking();
            Thread.sleep(60000);

            worker2.shutdown();
            client.deleteTunnel(new DeleteTunnelRequest(TableName, tunnelName2));

            // 下面两个资源(内部都有线程池)只有在真正需要退出的时候，再shutdown。如果不小心shutdown的话，也要new一个新的出来。
            client.shutdown();
            config.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
            worker2.shutdown();
            client.shutdown();
        }
    }
}
