package com.alicloud.openservices.tablestore.model.tunnel.internal;

import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

public class GetCheckpointRequest implements Request {
    /**
     * The ID of the Tunnel.
     */
    private String tunnelId;
    /**
     * Client identifier, which can be obtained from interfaces such as ListTunnel or DescribeTunnel.
     */
    private String clientId;
    /**
     * The ID of the Channel.
     */
    private String channelId;

    public GetCheckpointRequest(String tunnelId, String clientId, String channelId) {
        this.tunnelId = tunnelId;
        this.clientId = clientId;
        this.channelId = channelId;
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

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_GETCHECKPOINT;
    }
}
