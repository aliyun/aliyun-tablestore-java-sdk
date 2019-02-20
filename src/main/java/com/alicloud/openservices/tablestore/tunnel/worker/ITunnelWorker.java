package com.alicloud.openservices.tablestore.tunnel.worker;

public interface ITunnelWorker {
    void connectAndWorking() throws Exception;
    void shutdown();
}
