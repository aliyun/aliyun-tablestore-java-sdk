package com.alicloud.openservices.tablestore.model.search.groupby;

import java.util.List;

/**
 * {@link GroupByFilter}的统计结果
 */
public class GroupByFilterResult implements GroupByResult {

    private String groupByName;
    private List<GroupByFilterResultItem> groupByFilterResultItems;

    @Override
    public String getGroupByName() {
        return groupByName;
    }

    @Override
    public GroupByType getGroupByType() {
        return GroupByType.GROUP_BY_FILTER;
    }

    public GroupByFilterResult setGroupByName(String groupByName) {
        this.groupByName = groupByName;
        return this;
    }

    public List<GroupByFilterResultItem> getGroupByFilterResultItems() {
        return groupByFilterResultItems;
    }

    public GroupByFilterResult setGroupByFilterResultItems(
        List<GroupByFilterResultItem> groupByFilterResultItems) {
        this.groupByFilterResultItems = groupByFilterResultItems;
        return this;
    }
}
