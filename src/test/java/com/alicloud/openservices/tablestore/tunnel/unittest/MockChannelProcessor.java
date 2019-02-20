package com.alicloud.openservices.tablestore.tunnel.unittest;

import com.alicloud.openservices.tablestore.tunnel.worker.IChannelProcessor;
import com.alicloud.openservices.tablestore.tunnel.worker.ProcessRecordsInput;

public class MockChannelProcessor implements IChannelProcessor {
    @Override
    public void process(ProcessRecordsInput input) {
        System.out.println("Mock process");
        try {
            Thread.sleep(200);
        } catch (Exception e) {
            System.out.println(e.getCause() + ":" + e.getMessage());
        }
    }

    @Override
    public void shutdown() {
        System.out.println("Mock shutdown");
    }
}
