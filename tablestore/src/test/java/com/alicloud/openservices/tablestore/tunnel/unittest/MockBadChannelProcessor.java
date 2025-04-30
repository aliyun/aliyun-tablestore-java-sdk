package com.alicloud.openservices.tablestore.tunnel.unittest;

import com.alicloud.openservices.tablestore.tunnel.worker.IChannelProcessor;
import com.alicloud.openservices.tablestore.tunnel.worker.ProcessRecordsInput;

public class MockBadChannelProcessor implements IChannelProcessor  {
    @Override
    public void process(ProcessRecordsInput input) {
        System.out.println("Mock bad process");
        try {
            Thread.sleep(200);
        } catch (Exception e) {
            System.out.println(e.getCause() + ":" + e.getMessage());
        }
        throw new RuntimeException("mock bad process");
    }

    @Override
    public void shutdown() {
        System.out.println("Mock shutdown");
    }
}
