package com.alicloud.openservices.tablestore.model.tunnel.internal;

import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

public class CheckpointRequest implements Request {
    /**
     * Tunnel的ID。
     */
    private String tunnelId;
    /**
     * 客户端标识，可以从ListTunnel或者DescribeTunnel等接口获取。
     */
    private String clientId;
    /**
     * Channel的ID。
     */
    private String channelId;
    /**
     * 数据消费位点。
     */
    private String checkpoint;
    /**
     * Checkpoint对应的序列号，用于分布式环境下Checkpoint的保序。
     */
    private long sequenceNumber;

    public CheckpointRequest(String tunnelId, String clientId, String channelId, String checkpoint,
                             long sequenceNumber) {
        this.tunnelId = tunnelId;
        this.clientId = clientId;
        this.channelId = channelId;
        this.checkpoint = checkpoint;
        this.sequenceNumber = sequenceNumber;
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

    public String getCheckpoint() {
        return checkpoint;
    }

    public void setCheckpoint(String checkpoint) {
        this.checkpoint = checkpoint;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public void setSequenceNumber(long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_CHECKPOINT;
    }
}
