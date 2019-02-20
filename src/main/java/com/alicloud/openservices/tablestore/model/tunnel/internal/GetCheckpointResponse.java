package com.alicloud.openservices.tablestore.model.tunnel.internal;

import com.alicloud.openservices.tablestore.model.Response;

public class GetCheckpointResponse extends Response {
    /**
     * 数据消费位点，Tunnel服务端封装，包含消费总数，消费时间，表主键等信息。
     */
    private String checkpoint;
    /**
     * Checkpoint对应的序列号，用于分布式环境下Checkpoint的保序。
     */
    private long sequenceNumber;

    public GetCheckpointResponse() {
    }

    public GetCheckpointResponse(Response meta) {
        super(meta);
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
}
