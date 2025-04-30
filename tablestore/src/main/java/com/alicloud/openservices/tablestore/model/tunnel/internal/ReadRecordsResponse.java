package com.alicloud.openservices.tablestore.model.tunnel.internal;

import java.util.List;

import com.alicloud.openservices.tablestore.model.Response;
import com.alicloud.openservices.tablestore.model.StreamRecord;

public class ReadRecordsResponse extends Response {
    /**
     * The list of records returned by the request.
     */
    private List<StreamRecord> records;

    /**
     * The start Token for reading the current Channel next time.
     */
    private String nextToken;

    /**
     * The byte size of ReadRecordsResponse, used for the sleep strategy when reading data. (Internal parameter)
     */
    private int memoizedSerializedSize;

    private Boolean mayMoreRecord;

    public ReadRecordsResponse() {

    }

    public ReadRecordsResponse(Response meta) {
        super(meta);
    }

    /**
     * Get the record list
     *
     * @return List of records
     */
    public List<StreamRecord> getRecords() {
        return records;
    }

    public void setRecords(List<StreamRecord> records) {
        this.records = records;
    }

    public String getNextToken() {
        return nextToken;
    }

    public void setNextToken(String nextToken) {
        this.nextToken = nextToken;
    }

    public int getMemoizedSerializedSize() {
        return memoizedSerializedSize;
    }

    public void setMemoizedSerializedSize(int memoizedSerializedSize) {
        this.memoizedSerializedSize = memoizedSerializedSize;
    }

    public Boolean getMayMoreRecord() {
        return mayMoreRecord;
    }

    public void setMayMoreRecord(Boolean mayMoreRecord) {
        this.mayMoreRecord = mayMoreRecord;
    }
}
