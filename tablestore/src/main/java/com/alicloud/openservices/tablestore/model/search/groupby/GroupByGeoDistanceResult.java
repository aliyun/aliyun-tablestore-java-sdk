package com.alicloud.openservices.tablestore.model.search.groupby;

import java.util.List;

public class GroupByGeoDistanceResult implements GroupByResult {

    private String groupByName;
    private List<GroupByGeoDistanceResultItem> groupByGeoDistanceResultItems;

    @Override
    public String getGroupByName() {
        return groupByName;
    }

    @Override
    public GroupByType getGroupByType() {
        return GroupByType.GROUP_BY_GEO_DISTANCE;
    }

    public GroupByGeoDistanceResult setGroupByName(String groupByName) {
        this.groupByName = groupByName;
        return this;
    }

    public List<GroupByGeoDistanceResultItem> getGroupByGeoDistanceResultItems() {
        return groupByGeoDistanceResultItems;
    }

    public GroupByGeoDistanceResult setGroupByGeoDistanceResultItems(
        List<GroupByGeoDistanceResultItem> groupByGeoDistanceResultItems) {
        this.groupByGeoDistanceResultItems = groupByGeoDistanceResultItems;
        return this;
    }

}
