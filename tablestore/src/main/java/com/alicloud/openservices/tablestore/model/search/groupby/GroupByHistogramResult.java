package com.alicloud.openservices.tablestore.model.search.groupby;

import java.util.List;

public class GroupByHistogramResult implements GroupByResult {

    private String groupByName;
    private List<GroupByHistogramItem> groupByHistogramItems;

    @Override
    public String getGroupByName() {
        return groupByName;
    }

    @Override
    public GroupByType getGroupByType() {
        return GroupByType.GROUP_BY_HISTOGRAM;
    }

    public GroupByHistogramResult setGroupByName(String groupByName) {
        this.groupByName = groupByName;
        return this;
    }

    public List<GroupByHistogramItem> getGroupByHistogramItems() {
        return groupByHistogramItems;
    }

    public GroupByHistogramResult setGroupByHistogramItems(
        List<GroupByHistogramItem> groupByHistogramItems) {
        this.groupByHistogramItems = groupByHistogramItems;
        return this;
    }
}
