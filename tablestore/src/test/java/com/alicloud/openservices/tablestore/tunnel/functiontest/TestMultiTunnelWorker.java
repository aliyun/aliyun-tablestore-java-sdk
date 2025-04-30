package com.alicloud.openservices.tablestore.tunnel.functiontest;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

import com.alicloud.openservices.tablestore.TunnelClient;
import com.alicloud.openservices.tablestore.tunnel.worker.IChannelProcessor;
import com.alicloud.openservices.tablestore.tunnel.worker.ProcessRecordsInput;
import com.alicloud.openservices.tablestore.tunnel.worker.TunnelWorker;
import com.alicloud.openservices.tablestore.tunnel.worker.TunnelWorkerConfig;

public class TestMultiTunnelWorker {
    private static final String Endpoint = "";
    private static final String AccessId = "";
    private static final String AccessKey = "";
    private static final String InstanceName = "";

    // here is just a sample processor.
    static class SampleProcessor implements IChannelProcessor {
        private String tableName;
        private String key;

        public SampleProcessor(String tableName, String key) {
            this.tableName = tableName;
            this.key = key;
        }

        @Override
        public void process(ProcessRecordsInput input) {
            System.out.println(
                String.format("Table %s process %d records, NextToken: %s", tableName, input.getRecords().size(),
                    input.getNextToken()));
            if (input.getRecords().size() == 0) {
                try {
                    // Mock Record Process.
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public void shutdown() {
            System.out.println(String.format("Mock shutdown, tableName: %s, key: %s", tableName, key));
        }
    }

    public static void main(String[] args) {
        TunnelClient tunnelClient = new TunnelClient(Endpoint, AccessId, AccessKey, InstanceName);

        // for simply the main logic, we create several tunnels in advance.
        // you can use CreateTunnel or ListTunnel API to get TunnelId.
        List<String> tunnelIds = Arrays.asList(
            "9c0fbd17-65b7-4449-ac38-03e794e261aa",
            "2859e46f-48c4-4463-8663-bb63f9cae0a0",
            "06d1445b-4967-4508-9305-729f4821eb0a",
            "451e4661-f533-4192-b8b7-c1fe667aa76a");

        // Shared thread pool for TunnelWorkerConfig
        ThreadPoolExecutor readRecordsExecutor = TunnelWorkerConfig.newDefaultThreadPool("read-records-executor-");
        ThreadPoolExecutor processRecordsExecutor = TunnelWorkerConfig.newDefaultThreadPool(
            "process-records-executor-");
        for (String tunnelId : tunnelIds) {
            TunnelWorkerConfig workerConfig = new TunnelWorkerConfig(readRecordsExecutor, processRecordsExecutor,
                new SampleProcessor("table_" + tunnelId, "test_key_" + tunnelId));
            TunnelWorker tunnelWorker = new TunnelWorker(tunnelId, tunnelClient, workerConfig);

            try {
                System.out.println(String.format("TunnelWorker %s start...", tunnelId));
                tunnelWorker.connectAndWorking();
            } catch (Exception e) {
                e.printStackTrace();
                tunnelWorker.shutdown();
                // Attention: When multiple TunnelWorkerConfig instances share the same thread pool, tunnelWorkerConfig.shutdown() should not be called again.
                //            Because this will shut down readRecordsExecutor and processRecords.
            }
        }
    }
}
