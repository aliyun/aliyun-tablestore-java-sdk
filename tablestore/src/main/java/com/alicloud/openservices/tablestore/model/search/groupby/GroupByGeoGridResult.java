package com.alicloud.openservices.tablestore.model.search.groupby;

import java.util.List;

public class GroupByGeoGridResult implements GroupByResult {

    private String groupByName;
    private List<GroupByGeoGridResultItem> groupByGeoGridResultItems;

    @Override
    public String getGroupByName() {
        return groupByName;
    }

    @Override
    public GroupByType getGroupByType() {
        return GroupByType.GROUP_BY_GEO_GRID;
    }

    public GroupByGeoGridResult setGroupByName(String groupByName) {
        this.groupByName = groupByName;
        return this;
    }

    public List<GroupByGeoGridResultItem> getGroupByGeoGridResultItems() {
        return groupByGeoGridResultItems;
    }

    public GroupByGeoGridResult setGroupByGeoGridResultItems(List<GroupByGeoGridResultItem> groupByGeoGridResultItems) {
        this.groupByGeoGridResultItems = groupByGeoGridResultItems;
        return this;
    }
}
