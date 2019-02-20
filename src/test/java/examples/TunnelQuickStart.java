package examples;

import com.alicloud.openservices.tablestore.TunnelClient;
import com.alicloud.openservices.tablestore.model.tunnel.CreateTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.CreateTunnelResponse;
import com.alicloud.openservices.tablestore.model.tunnel.TunnelType;
import com.alicloud.openservices.tablestore.tunnel.worker.IChannelProcessor;
import com.alicloud.openservices.tablestore.tunnel.worker.ProcessRecordsInput;
import com.alicloud.openservices.tablestore.tunnel.worker.TunnelWorker;
import com.alicloud.openservices.tablestore.tunnel.worker.TunnelWorkerConfig;

public class TunnelQuickStart {
    private static class SimpleProcessor implements IChannelProcessor {
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

    public static void main() throws Exception {
        // 1. 初始化Tunnel Client。
        final String endPoint = "";
        final String accessKeyId = "";
        final String accessKeySecret = "";
        final String instanceName = "";

        TunnelClient tunnelClient = new TunnelClient(endPoint, accessKeyId, accessKeySecret, instanceName);

        // 2. 创建Tunnel(此步骤需要提前建好一张测试表，可以使用SyncClient的createTable或者使用官网控制台等方式来创建)。
        final String tableName = "testTable";
        final String tunnelName = "testTunnel";
        CreateTunnelRequest request = new CreateTunnelRequest(tableName, tunnelName, TunnelType.BaseAndStream);
        CreateTunnelResponse resp = tunnelClient.createTunnel(request);
        // tunnelId会用于后续TunnelWorker的初始化, 该值同样可以通过ListTunnel或者DescribeTunnel获取。
        String tunnelId = resp.getTunnelId();
        System.out.println("Create Tunnel, Id: " + tunnelId);

        // 3. 用户自定义数据消费Callback, 开始自动化的数据消费。
        // TunnelWorkerConfig里面还有更多的高级参数，这里不做展开，会有专门的文档介绍。
        TunnelWorkerConfig config = new TunnelWorkerConfig(new SimpleProcessor());
        TunnelWorker worker = new TunnelWorker(tunnelId, tunnelClient, config);
        try {
            worker.connectAndWorking();
        } catch (Exception e) {
            e.printStackTrace();
            worker.shutdown();
            tunnelClient.shutdown();
        }
    }
}
