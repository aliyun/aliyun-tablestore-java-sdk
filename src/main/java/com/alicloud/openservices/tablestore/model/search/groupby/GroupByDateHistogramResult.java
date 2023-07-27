package com.alicloud.openservices.tablestore.model.search.groupby;


import java.util.List;

public class GroupByDateHistogramResult implements GroupByResult {

    private String groupByName;
    private List<GroupByDateHistogramItem> groupByDateHistogramItems;

    @Override
    public String getGroupByName() {
        return groupByName;
    }

    @Override
    public GroupByType getGroupByType() {
        return GroupByType.GROUP_BY_DATE_HISTOGRAM;
    }

    public GroupByDateHistogramResult setGroupByName(String groupByName) {
        this.groupByName = groupByName;
        return this;
    }

    public List<GroupByDateHistogramItem> getGroupByDateHistogramItems() {
        return groupByDateHistogramItems;
    }

    public GroupByDateHistogramResult setGroupByDateHistogramItems(List<GroupByDateHistogramItem> groupByDateHistogramItems) {
        this.groupByDateHistogramItems = groupByDateHistogramItems;
        return this;
    }
}
