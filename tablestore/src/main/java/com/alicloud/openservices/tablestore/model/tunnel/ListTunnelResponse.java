package com.alicloud.openservices.tablestore.model.tunnel;

import java.util.List;

import com.alicloud.openservices.tablestore.model.Response;
import com.alicloud.openservices.tablestore.model.tunnel.TunnelInfo;

/**
 * The return result of the ListTunnel operation.
 */
public class ListTunnelResponse extends Response {
    /**
     * List of Tunnel information.
     */
    private List<TunnelInfo> tunnelInfos;

    public ListTunnelResponse(Response meta) {
        super(meta);
    }

    public List<TunnelInfo> getTunnelInfos() {
        return tunnelInfos;
    }

    public void setTunnelInfos(List<TunnelInfo> tunnelInfos) {
        this.tunnelInfos = tunnelInfos;
    }
}
