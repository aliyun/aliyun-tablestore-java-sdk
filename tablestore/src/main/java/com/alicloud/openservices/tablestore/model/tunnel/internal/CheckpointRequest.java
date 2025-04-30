package com.alicloud.openservices.tablestore.model.tunnel.internal;

import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

public class CheckpointRequest implements Request {
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
    /**
     * Data consumption point.
     */
    private String checkpoint;
    /**
     * Sequence number corresponding to the Checkpoint, used for maintaining order of Checkpoints in a distributed environment.
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
