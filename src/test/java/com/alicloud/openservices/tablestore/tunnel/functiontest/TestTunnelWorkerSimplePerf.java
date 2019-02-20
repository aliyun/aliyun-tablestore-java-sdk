package com.alicloud.openservices.tablestore.tunnel.functiontest;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.alicloud.openservices.tablestore.TunnelClient;
import com.alicloud.openservices.tablestore.tunnel.worker.IChannelProcessor;
import com.alicloud.openservices.tablestore.tunnel.worker.ProcessRecordsInput;
import com.alicloud.openservices.tablestore.tunnel.worker.TunnelWorker;
import com.alicloud.openservices.tablestore.tunnel.worker.TunnelWorkerConfig;
import com.google.gson.Gson;

public class TestTunnelWorkerSimplePerf {
    private static final String Endpoint = "";
    private static final String AccessId = "";
    private static final String AccessKey = "";
    private static final String InstanceName = "";

    private static class PerfElement {
        long timestamp;
        long speed;
        long totalCount;

        public PerfElement(long timestamp, long speed, long totalCount) {
            this.timestamp = timestamp;
            this.speed = speed;
            this.totalCount = totalCount;
        }
    }

    private static final Gson GSON = new Gson();
    private static final int CAL_INTERVAL_MILLIS = 5000;
    static class PerfProcessor implements IChannelProcessor {
        private static final AtomicLong counter = new AtomicLong(0);
        private static final AtomicLong latestTs = new AtomicLong(0);
        private static final AtomicLong allCount = new AtomicLong(0);

        @Override
        public void process(ProcessRecordsInput input) {
            counter.addAndGet(input.getRecords().size());
            allCount.addAndGet(input.getRecords().size());
            if (System.currentTimeMillis() - latestTs.get() > CAL_INTERVAL_MILLIS) {
                synchronized (PerfProcessor.class) {
                    if (System.currentTimeMillis() - latestTs.get() > CAL_INTERVAL_MILLIS) {
                        long seconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - latestTs.get());
                        PerfElement element = new PerfElement(System.currentTimeMillis(), counter.get() / seconds, allCount.get());
                        System.out.println(GSON.toJson(element));
                        counter.set(0);
                        latestTs.set(System.currentTimeMillis());
                    }
                }
            }
        }

        @Override
        public void shutdown() {
            System.out.println("Mock shutdown");
        }
    }

    public static void main(String[] args) {
        TunnelClient client = new TunnelClient(Endpoint, AccessId, AccessKey, InstanceName);
        TunnelWorkerConfig config = new TunnelWorkerConfig(new PerfProcessor());
        config.setHeartbeatIntervalInSec(15);
        TunnelWorker worker = new TunnelWorker("265c97a9-2c41-4cb5-abdf-8b9966c4f0b8", client, config);
        try {
            worker.connectAndWorking();
        } catch (Exception e) {
            e.printStackTrace();
            worker.shutdown();
            client.shutdown();
        }
    }
}
