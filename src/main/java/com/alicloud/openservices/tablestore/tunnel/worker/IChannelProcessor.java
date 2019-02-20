package com.alicloud.openservices.tablestore.tunnel.worker;

public interface IChannelProcessor {
    /**
     * 增量数据的消费Callback.
     * @param input: 本次的增量数据，包括Stream Records和Next Token等信息。
     */
    void process(ProcessRecordsInput input);

    /**
     * 用户注册的Shutdown函数，用于注册资源(例如线程池，数据库连接)的回收等。
     */
    void shutdown();
}
