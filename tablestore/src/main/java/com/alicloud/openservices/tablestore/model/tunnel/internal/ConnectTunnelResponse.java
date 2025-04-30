package com.alicloud.openservices.tablestore.model.tunnel.internal;

import com.alicloud.openservices.tablestore.model.Response;

public class ConnectTunnelResponse extends Response {
    private String clientId;

    public ConnectTunnelResponse(Response meta) {
        super(meta);
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }
}
