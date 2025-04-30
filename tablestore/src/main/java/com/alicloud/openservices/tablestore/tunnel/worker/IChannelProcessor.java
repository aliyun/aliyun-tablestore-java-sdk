package com.alicloud.openservices.tablestore.tunnel.worker;

public interface IChannelProcessor {
    /**
     * Consumption Callback for incremental data.
     * @param input: The incremental data for this request, including Stream Records and Next Token information.
     */
    void process(ProcessRecordsInput input);

    /**
     * User-registered Shutdown function, used for the reclamation of resources (such as thread pools, database connections), etc.
     */
    void shutdown();
}
