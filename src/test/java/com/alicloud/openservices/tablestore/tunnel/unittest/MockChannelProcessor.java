package com.alicloud.openservices.tablestore.tunnel.unittest;

import com.alicloud.openservices.tablestore.tunnel.worker.IChannelProcessor;
import com.alicloud.openservices.tablestore.tunnel.worker.ProcessRecordsInput;

import java.util.concurrent.atomic.AtomicInteger;

public class MockChannelProcessor implements IChannelProcessor {
    private AtomicInteger processCount = new AtomicInteger(0);
    private int processTimeInMillis = 200;

    @Override
    public void process(ProcessRecordsInput input) {
        System.out.println("Mock process");
        try {
            Thread.sleep(processTimeInMillis);
        } catch (Exception e) {
            System.out.println(e.getCause() + ":" + e.getMessage());
        }
        processCount.incrementAndGet();
    }

    @Override
    public void shutdown() {
        System.out.println("Mock shutdown");
    }

    public int getProcessCount() {
        return processCount.get();
    }

    public void setProcessTimeInMillis(int processTimeInMillis) {
        this.processTimeInMillis = processTimeInMillis;
    }

    public int getProcessTimeInMillis() {
        return processTimeInMillis;
    }
}
