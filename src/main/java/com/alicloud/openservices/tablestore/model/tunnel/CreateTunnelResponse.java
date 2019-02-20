package com.alicloud.openservices.tablestore.model.tunnel;

import com.alicloud.openservices.tablestore.model.Response;

/**
 * CreateTunnel操作的返回结果。
 */
public class CreateTunnelResponse extends Response {
    /**
     * 创建出的Tunnel的ID。
     */
    private String tunnelId;

    public CreateTunnelResponse(Response meta) {
        super(meta);
    }

    /**
     * 获取创建出的Tunnel ID。
     *
     * @return 创建出的Tunnel ID。
     */
    public String getTunnelId() {
        return tunnelId;
    }

    /**
     * 内部接口，请勿使用。
     */
    public void setTunnelId(String tunnelId) {
        this.tunnelId = tunnelId;
    }
}
