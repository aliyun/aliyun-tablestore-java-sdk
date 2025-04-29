package com.alicloud.openservices.tablestore.model.tunnel;

import java.util.Date;
import java.util.List;

import com.alicloud.openservices.tablestore.model.Response;

/**
 * The return result of the DescribeTunnel operation.
 */
public class DescribeTunnelResponse extends Response {
    /**
     * The latest time point for consuming incremental data in Tunnel, whose value equals the time point of the slowest Channel being consumed in Tunnel. By default, it is January 1, 1970 (UTC).
     */
    private Date tunnelConsumePoint;

    /**
     * The RPO (Recovery Point Objective) time for incremental consumption of the Tunnel, in milliseconds.
     */
    private long tunnelRpo;

    /**
     * Tunnel information.
     */
    private TunnelInfo tunnelInfo;
    /**
     * The list of Channel information under this Tunnel.
     */
    private List<ChannelInfo> channelInfos;

    public DescribeTunnelResponse(Response meta) {
        super(meta);
    }

    /**
     * The RPO (Recovery Point Objective) time of the current Tunnel, in milliseconds.
     *
     * @return The RPO of the current Tunnel.
     */
    public long getTunnelRpo() {
        return tunnelRpo;
    }

    /**
     * Internal interface, do not use.
     */
    public void setTunnelRpo(long tunnelRpo) {
        this.tunnelRpo = tunnelRpo;
    }

    /**
     * Get the current time point for Tunnel data consumption.
     * @return The time point for Tunnel data consumption.
     */
    public Date getTunnelConsumePoint() {
        return tunnelConsumePoint;
    }

    /**
     * Internal interface, do not use.
     */
    public void setTunnelConsumePoint(Date tunnelConsumePoint) {
        this.tunnelConsumePoint = tunnelConsumePoint;
    }

    /**
     * Internal interface, do not use.
     */

    public TunnelInfo getTunnelInfo() {
        return tunnelInfo;
    }

    /**
     * Internal interface, do not use.
     */
    public void setTunnelInfo(TunnelInfo tunnelInfo) {
        this.tunnelInfo = tunnelInfo;
    }

    /**
     * Get the list of Channel information under this Tunnel.
     * @return
     */
    public List<ChannelInfo> getChannelInfos() {
        return channelInfos;
    }

    /**
     * Internal interface, do not use.
     */
    public void setChannelInfos(List<ChannelInfo> channelInfos) {
        this.channelInfos = channelInfos;
    }

}
