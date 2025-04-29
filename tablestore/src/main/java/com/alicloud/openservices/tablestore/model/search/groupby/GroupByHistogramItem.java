package com.alicloud.openservices.tablestore.model.search.groupby;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.agg.AggregationResults;

public class GroupByHistogramItem {

    private ColumnValue key;
    private long value;
    private AggregationResults subAggregationResults;
    private GroupByResults subGroupByResults;

    public ColumnValue getKey() {
        return key;
    }

    public GroupByHistogramItem setKey(ColumnValue key) {
        this.key = key;
        return this;
    }

    public long getValue() {
        return value;
    }

    public GroupByHistogramItem setValue(long value) {
        this.value = value;
        return this;
    }

    public AggregationResults getSubAggregationResults() {
        return subAggregationResults;
    }

    public GroupByHistogramItem setSubAggregationResults(
        AggregationResults subAggregationResults) {
        this.subAggregationResults = subAggregationResults;
        return this;
    }

    public GroupByResults getSubGroupByResults() {
        return subGroupByResults;
    }

    public GroupByHistogramItem setSubGroupByResults(GroupByResults subGroupByResults) {
        this.subGroupByResults = subGroupByResults;
        return this;
    }
}
