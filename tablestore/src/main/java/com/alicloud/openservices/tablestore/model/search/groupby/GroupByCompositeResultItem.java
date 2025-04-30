package com.alicloud.openservices.tablestore.model.search.groupby;

import com.alicloud.openservices.tablestore.model.search.agg.AggregationResults;

import java.util.List;

public class GroupByCompositeResultItem {
    /**
     * fields value for each group. the element may be null if this specific field is not exists for the group.
     */
    private List<String> keys;

    private long rowCount;

    private AggregationResults subAggregationResults;

    private GroupByResults subGroupByResults;

    public List<String> getKeys() {
        return keys;
    }

    public void setKeys(List<String> keys) {
        this.keys = keys;
    }

    public long getRowCount() {
        return rowCount;
    }

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    public AggregationResults getSubAggregationResults() {
        return subAggregationResults;
    }

    public void setSubAggregationResults(AggregationResults subAggregationResults) {
        this.subAggregationResults = subAggregationResults;
    }

    public GroupByResults getSubGroupByResults() {
        return subGroupByResults;
    }

    public void setSubGroupByResults(GroupByResults subGroupByResults) {
        this.subGroupByResults = subGroupByResults;
    }
}
