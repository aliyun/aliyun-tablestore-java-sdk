package com.alicloud.openservices.tablestore.tunnel.functiontest;

import com.alicloud.openservices.tablestore.TunnelClient;
import com.alicloud.openservices.tablestore.tunnel.worker.IChannelProcessor;
import com.alicloud.openservices.tablestore.tunnel.worker.ProcessRecordsInput;
import com.alicloud.openservices.tablestore.tunnel.worker.TunnelWorker;
import com.alicloud.openservices.tablestore.tunnel.worker.TunnelWorkerConfig;

public class TestTunnelWorker {
    private static final String Endpoint = "";
    private static final String AccessId = "";
    private static final String AccessKey = "";
    private static final String InstanceName = "";

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
        TunnelWorker worker = new TunnelWorker("480e3a3b-6672-46b1-b970-e611232dcd53", client, config);
        try {
            System.out.println("worker running....");
            worker.connectAndWorking();
            Thread.sleep(50000);
            worker.shutdown();
            System.out.println("worker shutdown....");
        } catch (Exception e) {
            e.printStackTrace();
            worker.shutdown();
        }

        // Reconstruct and connect.
        TunnelWorker worker2 = new TunnelWorker("480e3a3b-6672-46b1-b970-e611232dcd53", client, config);
        try {
            System.out.println("worker2 running...");
            worker2.connectAndWorking();
        } catch (Exception e) {
            e.printStackTrace();
            worker2.shutdown();
            client.shutdown();
        }
    }
}
