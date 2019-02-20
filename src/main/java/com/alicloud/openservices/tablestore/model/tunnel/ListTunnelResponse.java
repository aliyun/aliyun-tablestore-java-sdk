package com.alicloud.openservices.tablestore.model.tunnel;

import java.util.List;

import com.alicloud.openservices.tablestore.model.Response;
import com.alicloud.openservices.tablestore.model.tunnel.TunnelInfo;

/**
 * ListTunnel操作的返回结果。
 */
public class ListTunnelResponse extends Response {
    /**
     * Tunnel信息的列表。
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
