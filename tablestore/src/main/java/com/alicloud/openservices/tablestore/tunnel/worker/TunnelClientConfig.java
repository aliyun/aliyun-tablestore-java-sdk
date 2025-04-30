package com.alicloud.openservices.tablestore.tunnel.worker;

public class TunnelClientConfig {
    /**
     * Timeout, in seconds. Default value: 300s.
     */
    private long timeout = 300;

    /**
     * The identifier of the client, which defaults to the hostname.
     */
    private String clientTag = System.getProperty("os.name");

    public TunnelClientConfig() {
    }

    public TunnelClientConfig(long timeout, String clientTag) {
        this.timeout = timeout;
        this.clientTag = clientTag;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public String getClientTag() {
        return clientTag;
    }

    public void setClientTag(String clientTag) {
        this.clientTag = clientTag;
    }
}
