package com.alicloud.openservices.tablestore.model.tunnel.internal;

import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

public class ShutdownTunnelRequest implements Request {
    private String tunnelId;
    private String clientId;

    public ShutdownTunnelRequest(String tunnelId, String clientId) {
        this.tunnelId = tunnelId;
        this.clientId = clientId;
    }

    public String getTunnelId() {
        return tunnelId;
    }

    public void setTunnelId(String tunnelId) {
        this.tunnelId = tunnelId;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_SHUTDOWN_TUNNEL;
    }
}
