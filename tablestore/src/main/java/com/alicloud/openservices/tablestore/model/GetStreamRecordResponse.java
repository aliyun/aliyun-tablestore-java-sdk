package com.alicloud.openservices.tablestore.model;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import static com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesResponseFactory.parseTagsOrAttrs;

public class GetStreamRecordResponse extends Response {

    /**
     * The list of records returned by the request.
     */
    private List<StreamRecord> records;

    /**
     * ShardIterator used to initiate the next request
     */
    private String nextShardIterator;

    /**
     * Whether there are more records to be fetched
     */
    private Boolean mayMoreRecord;

    public GetStreamRecordResponse() {

    }

    public GetStreamRecordResponse(Response meta) {
        super(meta);
    }

    /**
     * Get the record list
     * @return Record list
     */
    public List<StreamRecord> getRecords() {
        return records;
    }

    /**
     * Convert the tags field in time-series data to a Map
     * @return Map
     */
    public static Map<String, String> parseTimeseriesTags(String s) {
        return parseTagsOrAttrs(s);
    }

    public void setRecords(List<StreamRecord> records) {
        this.records = records;
    }

    /**
     * Get the NextShardIterator, which is used to initiate the next request.
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
