package com.alicloud.openservices.tablestore.tunnel.worker;

public class TunnelClientConfig {
    /**
     * 超时时间，单位为秒，默认为300s。
     */
    private long timeout = 300;

    /**
     * 客户端的标识，默认为主机名。
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
