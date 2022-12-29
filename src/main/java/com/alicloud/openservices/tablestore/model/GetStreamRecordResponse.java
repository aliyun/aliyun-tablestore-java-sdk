package com.alicloud.openservices.tablestore.model;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import static com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesResponseFactory.parseTagsOrAttrs;

public class GetStreamRecordResponse extends Response {

    /**
     * 请求返回的记录列表
     */
    private List<StreamRecord> records;

    /**
     * 用于发起下一次请求的ShardIterator
     */
    private String nextShardIterator;

    /**
     * 是否有更多记录待拉取
     */
    private Boolean mayMoreRecord;

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

    /**
     * 将时序数据中的tags字段转为Map
     * @return Map
     */
    public static Map<String, String> parseTimeseriesTags(String s) {
        return parseTagsOrAttrs(s);
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

    public Boolean getMayMoreRecord() {
        return mayMoreRecord;
    }

    public void setMayMoreRecord(Boolean mayMoreRecord) {
        this.mayMoreRecord = mayMoreRecord;
    }
}
