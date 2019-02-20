package com.alicloud.openservices.tablestore.tunnel.worker;

import java.util.List;

import com.alicloud.openservices.tablestore.model.StreamRecord;

public class ProcessRecordsInput {
    private List<StreamRecord> records;
    private String nextToken;
    private String traceId;

    public ProcessRecordsInput(List<StreamRecord> records, String nextToken, String traceId) {
        this.records = records;
        this.nextToken = nextToken;
        this.traceId = traceId;
    }

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

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    @Override
    public String toString() {
        //TODO
        return "";
    }
}
