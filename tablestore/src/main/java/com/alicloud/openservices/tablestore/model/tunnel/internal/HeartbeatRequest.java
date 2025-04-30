package com.alicloud.openservices.tablestore.model.tunnel.internal;

import java.util.List;

import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

public class HeartbeatRequest implements Request {
    private String tunnelId;
    private String clientId;
    private List<Channel> channels;

    public HeartbeatRequest(String tunnelId, String clientId,
                            List<Channel> channels) {
        this.tunnelId = tunnelId;
        this.clientId = clientId;
        this.channels = channels;
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

    public List<Channel> getChannels() {
        return channels;
    }

    public void setChannels(List<Channel> channels) {
        this.channels = channels;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_HEARTBEAT;
    }
}
