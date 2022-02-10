package com.alicloud.openservices.tablestore.model.search.groupby;

import java.util.List;

public class GroupByRangeResult implements GroupByResult {

    private String groupByName;
    private List<GroupByRangeResultItem> groupByRangeResultItems;

    @Override
    public String getGroupByName() {
        return groupByName;
    }

    @Override
    public GroupByType getGroupByType() {
        return GroupByType.GROUP_BY_RANGE;
    }

    public GroupByRangeResult setGroupByName(String groupByName) {
        this.groupByName = groupByName;
        return this;
    }

    public List<GroupByRangeResultItem> getGroupByRangeResultItems() {
        return groupByRangeResultItems;
    }

    public GroupByRangeResult setGroupByRangeResultItems(List<GroupByRangeResultItem> groupByRangeResultItems) {
        this.groupByRangeResultItems = groupByRangeResultItems;
        return this;
    }

}
