package com.alicloud.openservices.tablestore.model.search.groupby;

import com.alicloud.openservices.tablestore.model.search.agg.AggregationResults;

public class GroupByFieldResultItem {

    private String key;
    private long rowCount;
    private AggregationResults subAggregationResults;
    private GroupByResults subGroupByResults;

    public AggregationResults getSubAggregationResults() {
        return subAggregationResults;
    }

    public GroupByFieldResultItem setSubAggregationResults(
        AggregationResults subAggregationResults) {
        this.subAggregationResults = subAggregationResults;
        return this;
    }

    public GroupByResults getSubGroupByResults() {
        return subGroupByResults;
    }

    public GroupByFieldResultItem setSubGroupByResults(
        GroupByResults subGroupByResults) {
        this.subGroupByResults = subGroupByResults;
        return this;
    }

    public String getKey() {
        return key;
    }

    public GroupByFieldResultItem setKey(String key) {
        this.key = key;
        return this;
    }

    public long getRowCount() {
        return rowCount;
    }

    public GroupByFieldResultItem setRowCount(long rowCount) {
        this.rowCount = rowCount;
        return this;
    }

}
