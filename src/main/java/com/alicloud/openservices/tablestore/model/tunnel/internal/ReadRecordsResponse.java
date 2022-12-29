package com.alicloud.openservices.tablestore.model.tunnel.internal;

import java.util.List;

import com.alicloud.openservices.tablestore.model.Response;
import com.alicloud.openservices.tablestore.model.StreamRecord;

public class ReadRecordsResponse extends Response {
    /**
     * 请求返回的记录列表
     */
    private List<StreamRecord> records;

    /**
     * 下次读取当前Channel的起始Token。
     */
    private String nextToken;

    /**
     * ReadRecordsResponse的字节大小，用于数据读取时的休眠策略。(内部参数)
     */
    private int memoizedSerializedSize;

    private Boolean mayMoreRecord;

    public ReadRecordsResponse() {

    }

    public ReadRecordsResponse(Response meta) {
        super(meta);
    }

    /**
     * 获取记录列表
     *
     * @return 记录列表
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
