package com.alicloud.openservices.tablestore.model.search.groupby;

import com.alicloud.openservices.tablestore.model.search.agg.AggregationResults;

public class GroupByDateHistogramItem {

    private long timestamp;
    private long rowCount;
    private AggregationResults subAggregationResults;
    private GroupByResults subGroupByResults;

    public long getTimestamp() {
        return timestamp;
    }

    public GroupByDateHistogramItem setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public long getRowCount() {
        return rowCount;
    }

    public GroupByDateHistogramItem setRowCount(long rowCount) {
        this.rowCount = rowCount;
        return this;
    }

    public AggregationResults getSubAggregationResults() {
        return subAggregationResults;
    }

    public GroupByDateHistogramItem setSubAggregationResults(AggregationResults subAggregationResults) {
        this.subAggregationResults = subAggregationResults;
        return this;
    }

    public GroupByResults getSubGroupByResults() {
        return subGroupByResults;
    }

    public GroupByDateHistogramItem setSubGroupByResults(GroupByResults subGroupByResults) {
        this.subGroupByResults = subGroupByResults;
        return this;
    }
}
