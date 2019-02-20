package com.alicloud.openservices.tablestore.model.tunnel;

import java.util.Date;
import java.util.List;

import com.alicloud.openservices.tablestore.model.Response;

/**
 * DescribeTunnel操作的返回结果。
 */
public class DescribeTunnelResponse extends Response {
    /**
     * Tunnel消费增量数据的最新时间点，其值等于Tunnel中消费最慢的Channel的时间点，默认为1970年1月1日(UTC)。
     */
    private Date tunnelConsumePoint;

    /**
     * Tunnel的增量消费RPO(Recovery Point Objective)时间, 单位为毫秒。
     */
    private long tunnelRpo;

    /**
     * Tunnel信息。
     */
    private TunnelInfo tunnelInfo;
    /**
     * 该Tunnel下的Channel信息列表。
     */
    private List<ChannelInfo> channelInfos;

    public DescribeTunnelResponse(Response meta) {
        super(meta);
    }

    /**
     * 当前Tunnel的RPO(Recovery Point Objective)时间, 单位为毫秒。
     *
     * @return 当前Tunnel的RPO。
     */
    public long getTunnelRpo() {
        return tunnelRpo;
    }

    /**
     * 内部接口，请勿使用。
     */
    public void setTunnelRpo(long tunnelRpo) {
        this.tunnelRpo = tunnelRpo;
    }

    /**
     * 获取当前Tunnel消费数据的时间点。
     * @return Tunnel消费数据的时间点。
     */
    public Date getTunnelConsumePoint() {
        return tunnelConsumePoint;
    }

    /**
     * 内部接口，请勿使用。
     */
    public void setTunnelConsumePoint(Date tunnelConsumePoint) {
        this.tunnelConsumePoint = tunnelConsumePoint;
    }

    /**
     * 内部接口，请勿使用。
     */

    public TunnelInfo getTunnelInfo() {
        return tunnelInfo;
    }

    /**
     * 内部接口，请勿使用。
     */
    public void setTunnelInfo(TunnelInfo tunnelInfo) {
        this.tunnelInfo = tunnelInfo;
    }

    /**
     * 获取该Tunnel下的Channel信息列表。
     * @return
     */
    public List<ChannelInfo> getChannelInfos() {
        return channelInfos;
    }

    /**
     * 内部接口，请勿使用。
     */
    public void setChannelInfos(List<ChannelInfo> channelInfos) {
        this.channelInfos = channelInfos;
    }

}
