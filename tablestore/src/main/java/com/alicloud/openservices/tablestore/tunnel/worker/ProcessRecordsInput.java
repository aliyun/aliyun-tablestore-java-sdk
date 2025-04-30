package com.alicloud.openservices.tablestore.tunnel.worker;

import java.util.List;

import com.alicloud.openservices.tablestore.model.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessRecordsInput {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessRecordsInput.class);

    private List<StreamRecord> records;
    private String nextToken;
    private String traceId;
    private String channelId;

    public ProcessRecordsInput(List<StreamRecord> records, String nextToken, String traceId) {
        this.records = records;
        this.nextToken = nextToken;
        this.traceId = traceId;
    }

    public ProcessRecordsInput(List<StreamRecord> records, String nextToken, String traceId, String channelId) {
        this.records = records;
        this.nextToken = nextToken;
        this.traceId = traceId;
        this.channelId = channelId;
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

    public String getChannelId() {
        return channelId;
    }

    public String getPartitionId() {
        String[] splits = channelId.split("_");
        if (splits.length != 2) {
            LOG.info("invalid channel id {}", channelId);
        }
        return splits[0];
    }

    @Override
    public String toString() {
        //TODO
        return "";
    }
}
