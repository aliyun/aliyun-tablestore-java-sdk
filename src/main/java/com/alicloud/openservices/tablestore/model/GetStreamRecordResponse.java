package com.alicloud.openservices.tablestore.model;


import java.util.List;

public class GetStreamRecordResponse extends Response {

    /**
     * 请求返回的记录列表
     */
    private List<StreamRecord> records;

    /**
     * 用于发起下一次请求的ShardIterator
     */
    private String nextShardIterator;

    public GetStreamRecordResponse() {

    }

    public GetStreamRecordResponse(Response meta) {
        super(meta);
    }

    /**
     * 获取记录列表
     * @return 记录列表
     */
    public List<StreamRecord> getRecords() {
        return records;
    }

    public void setRecords(List<StreamRecord> records) {
        this.records = records;
    }

    /**
     * 获取NextShardIterator，用于发起下一次请求
     * @return nextShardIterator
     */
    public String getNextShardIterator() {
        return nextShardIterator;
    }

    public void setNextShardIterator(String nextShardIterator) {
        this.nextShardIterator = nextShardIterator;
    }
}
