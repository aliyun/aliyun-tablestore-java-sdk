package com.alicloud.openservices.tablestore.model.search.groupby;

import com.alicloud.openservices.tablestore.model.search.GeoGrid;
import com.alicloud.openservices.tablestore.model.search.agg.AggregationResults;

public class GroupByGeoGridResultItem {

    private String key;
    private GeoGrid geoGrid;
    private long rowCount;
    private AggregationResults subAggregationResults;
    private GroupByResults subGroupByResults;

    public String getKey() {
        return key;
    }

    public GroupByGeoGridResultItem setKey(String key) {
        this.key = key;
        return this;
    }

    public GeoGrid getGeoGrid() {
        return geoGrid;
    }

    public GroupByGeoGridResultItem setGeoGrid(GeoGrid geoGrid) {
        this.geoGrid = geoGrid;
        return this;
    }

    public long getRowCount() {
        return rowCount;
    }

    public GroupByGeoGridResultItem setRowCount(long rowCount) {
        this.rowCount = rowCount;
        return this;
    }

    public AggregationResults getSubAggregationResults() {
        return subAggregationResults;
    }

    public GroupByGeoGridResultItem setSubAggregationResults(AggregationResults subAggregationResults) {
        this.subAggregationResults = subAggregationResults;
        return this;
    }

    public GroupByResults getSubGroupByResults() {
        return subGroupByResults;
    }

    public GroupByGeoGridResultItem setSubGroupByResults(GroupByResults subGroupByResults) {
        this.subGroupByResults = subGroupByResults;
        return this;
    }
}
