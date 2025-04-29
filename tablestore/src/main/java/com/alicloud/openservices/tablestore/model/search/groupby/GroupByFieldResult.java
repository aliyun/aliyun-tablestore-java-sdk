package com.alicloud.openservices.tablestore.model.search.groupby;

import java.util.List;

/**
 * The statistical result of {@link GroupByField}
 */
public class GroupByFieldResult implements GroupByResult {

    private String groupByName;
    private List<GroupByFieldResultItem> groupByFieldResultItems;

    @Override
    public String getGroupByName() {
        return groupByName;
    }

    @Override
    public GroupByType getGroupByType() {
        return GroupByType.GROUP_BY_FIELD;
    }

    public GroupByFieldResult setGroupByName(String groupByName) {
        this.groupByName = groupByName;
        return this;
    }

    public List<GroupByFieldResultItem> getGroupByFieldResultItems() {
        return groupByFieldResultItems;
    }

    public GroupByFieldResult setGroupByFieldResultItems(List<GroupByFieldResultItem> groupByFieldResultItems) {
        this.groupByFieldResultItems = groupByFieldResultItems;
        return this;
    }
}
