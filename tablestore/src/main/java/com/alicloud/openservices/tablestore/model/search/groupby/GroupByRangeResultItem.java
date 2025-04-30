package com.alicloud.openservices.tablestore.model.search.groupby;

import com.alicloud.openservices.tablestore.model.search.agg.AggregationResults;

public class GroupByRangeResultItem {

    private double from;
    private double to;
    private long rowCount;
    private AggregationResults subAggregationResults;
    private GroupByResults subGroupByResults;

    public double getFrom() {
        return from;
    }

    public GroupByRangeResultItem setFrom(double from) {
        this.from = from;
        return this;
    }

    public double getTo() {
        return to;
    }

    public GroupByRangeResultItem setTo(double to) {
        this.to = to;
        return this;
    }

    public long getRowCount() {
        return rowCount;
    }

    public GroupByRangeResultItem setRowCount(long rowCount) {
        this.rowCount = rowCount;
        return this;
    }

    public AggregationResults getSubAggregationResults() {
        return subAggregationResults;
    }

    public GroupByRangeResultItem setSubAggregationResults(
        AggregationResults subAggregationResults) {
        this.subAggregationResults = subAggregationResults;
        return this;
    }

    public GroupByResults getSubGroupByResults() {
        return subGroupByResults;
    }

    public GroupByRangeResultItem setSubGroupByResults(
        GroupByResults subGroupByResults) {
        this.subGroupByResults = subGroupByResults;
        return this;
    }
}
