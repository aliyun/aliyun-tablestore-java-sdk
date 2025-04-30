package com.alicloud.openservices.tablestore.model.tunnel;

import com.alicloud.openservices.tablestore.model.Response;

/**
 * The return result of the CreateTunnel operation.
 */
public class CreateTunnelResponse extends Response {
    /**
     * The ID of the created Tunnel.
     */
    private String tunnelId;

    public CreateTunnelResponse(Response meta) {
        super(meta);
    }

    /**
     * Get the created Tunnel ID.
     *
     * @return The created Tunnel ID.
     */
    public String getTunnelId() {
        return tunnelId;
    }

    /**
     * Internal interface, do not use.
     */
    public void setTunnelId(String tunnelId) {
        this.tunnelId = tunnelId;
    }
}
