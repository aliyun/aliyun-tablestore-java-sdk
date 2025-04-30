package com.alicloud.openservices.tablestore.model.tunnel.internal;

import com.alicloud.openservices.tablestore.model.Response;

public class GetCheckpointResponse extends Response {
    /**
     * Data consumption checkpoint, encapsulated by the Tunnel service, includes information such as the total number of consumptions, consumption time, table primary key, etc.
     */
    private String checkpoint;
    /**
     * The sequence number corresponding to the Checkpoint, used for maintaining the order of Checkpoints in a distributed environment.
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
