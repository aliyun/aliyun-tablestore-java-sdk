package com.alicloud.openservices.tablestore.tunnel.worker;

import java.util.List;

import com.alicloud.openservices.tablestore.model.StreamRecord;

public class ReadRecordsPipeResult {
    private boolean finished;
    private List<StreamRecord> records;
    private String requestId;
    private String nextToken;
    private long sleepMillis;

    /**
     * Task执行过程中抛出的异常，会catch保存在这里。
     */
    private Exception exception;

    public boolean isFinished() {
        return finished;
    }

    public void setFinished(boolean finished) {
        this.finished = finished;
    }

    public List<StreamRecord> getRecords() {
        return records;
    }

    public void setRecords(List<StreamRecord> records) {
        this.records = records;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getNextToken() {
        return nextToken;
    }

    public void setNextToken(String nextToken) {
        this.nextToken = nextToken;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    public long getSleepMillis() {
        return sleepMillis;
    }

    public void setSleepMillis(long sleepMillis) {
        this.sleepMillis = sleepMillis;
    }


}
