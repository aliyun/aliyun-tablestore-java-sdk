package com.alicloud.openservices.tablestore.model.search.groupby;

import com.alicloud.openservices.tablestore.model.search.agg.AggregationResults;

public class GroupByFilterResultItem {

    private long rowCount;
    private AggregationResults subAggregationResults;
    private GroupByResults subGroupByResults;

    public long getRowCount() {
        return rowCount;
    }

    public GroupByFilterResultItem setRowCount(long rowCount) {
        this.rowCount = rowCount;
        return this;
    }

    public AggregationResults getSubAggregationResults() {
        return subAggregationResults;
    }

    public GroupByFilterResultItem setSubAggregationResults(
        AggregationResults subAggregationResults) {
        this.subAggregationResults = subAggregationResults;
        return this;
    }

    public GroupByResults getSubGroupByResults() {
        return subGroupByResults;
    }

    public GroupByFilterResultItem setSubGroupByResults(
        GroupByResults subGroupByResults) {
        this.subGroupByResults = subGroupByResults;
        return this;
    }
}
