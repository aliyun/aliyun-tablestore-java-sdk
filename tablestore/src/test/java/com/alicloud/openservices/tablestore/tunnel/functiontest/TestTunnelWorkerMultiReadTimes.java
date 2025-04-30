package com.alicloud.openservices.tablestore.tunnel.functiontest;

import com.alicloud.openservices.tablestore.TunnelClient;
import com.alicloud.openservices.tablestore.tunnel.worker.IChannelProcessor;
import com.alicloud.openservices.tablestore.tunnel.worker.ProcessRecordsInput;
import com.alicloud.openservices.tablestore.tunnel.worker.TunnelWorker;
import com.alicloud.openservices.tablestore.tunnel.worker.TunnelWorkerConfig;

public class TestTunnelWorkerMultiReadTimes {
    private static final String ENDPOINT = "https://vehicle-test.cn-hangzhou.ots.aliyuncs.com";
    private static final String ACCESS_ID = "";
    private static final String ACCESS_KEY = "";
    private static final String INSTANCE_NAME = "vehicle-test";

    static class SimpleProcessor implements IChannelProcessor {
        @Override
        public void process(ProcessRecordsInput input) {
            System.out.println("Default record processor, print records count, timestamp: " + System.currentTimeMillis());
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
        TunnelClient client = new TunnelClient(ENDPOINT, ACCESS_ID, ACCESS_KEY, INSTANCE_NAME);
        TunnelWorkerConfig config = new TunnelWorkerConfig(new SimpleProcessor());
        config.setHeartbeatIntervalInSec(10);
        config.setHeartbeatTimeoutInSec(120);
        config.setMaxRetryIntervalInMillis(200);
        config.setReadMaxBytesPerRound(64 * 1024 * 1024);
        config.setReadMaxTimesPerRound(2);
        config.setMaxChannelParallel(1);
        TunnelWorker worker1 = new TunnelWorker("ac442770-feea-4e3c-941e-5f07c7ff5363", client, config);
        try {
            System.out.println("worker running....");
            worker1.connectAndWorking();
        } catch (Exception e) {
            e.printStackTrace();
            config.shutdown();
            worker1.shutdown();
            client.shutdown();
        }
        try {
            Thread.sleep(100000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        config.shutdown();
        worker1.shutdown();
        client.shutdown();
    }
}
