package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.model.search.ScanQuery;

/**
 * Get the partition information of the data.
 */
public class ComputeSplitsResponse extends Response{

    /**
     * {@link ScanQuery} establishes a link to the server with this sessionId.
     */
    private byte[] sessionId;


    /**
     * <p>number of data partition. How to use:</p>
     * <p>when {@link ScanQuery} obtaining data, call {@link ScanQuery#setMaxParallel(Integer)} to set the maxParallel.</p>
     */
    private Integer splitsSize;



    public ComputeSplitsResponse(Response meta) {
        super(meta);
    }

    public Integer getSplitsSize() {
        return splitsSize;
    }

    public ComputeSplitsResponse setSplitsSize(Integer splitsSize) {
        this.splitsSize = splitsSize;
        return this;
    }

    public byte[] getSessionId() {
        return sessionId;
    }

    public ComputeSplitsResponse setSessionId(byte[] sessionId) {
        this.sessionId = sessionId;
        return this;
    }
}
