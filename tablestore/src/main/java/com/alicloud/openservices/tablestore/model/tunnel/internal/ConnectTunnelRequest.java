package com.alicloud.openservices.tablestore.model.tunnel.internal;

import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;
import com.alicloud.openservices.tablestore.tunnel.worker.TunnelClientConfig;

public class ConnectTunnelRequest implements Request {
    private String tunnelId;
    private TunnelClientConfig config;

    public ConnectTunnelRequest(String tunnelId, TunnelClientConfig config) {
        this.tunnelId = tunnelId;
        this.config = config;
    }

    public String getTunnelId() {
        return tunnelId;
    }

    public void setTunnelId(String tunnelId) {
        this.tunnelId = tunnelId;
    }

    public TunnelClientConfig getConfig() {
        return config;
    }

    public void setConfig(TunnelClientConfig config) {
        this.config = config;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_CONNECT_TUNNEL;
    }
}
