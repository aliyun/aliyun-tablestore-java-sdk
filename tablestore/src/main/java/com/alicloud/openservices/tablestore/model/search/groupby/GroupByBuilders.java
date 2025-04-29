package com.alicloud.openservices.tablestore.model.search.groupby;

/**
 * The overall builder for GroupBy.
 * This class is used for all instances of GroupBy.
 */
public final class GroupByBuilders {

    public static GroupByField.Builder groupByField(String groupByName, String field) {
        return GroupByField.newBuilder().groupByName(groupByName).fieldName(field);
    }

    public static GroupByRange.Builder groupByRange(String groupByName, String field) {
        return GroupByRange.newBuilder().groupByName(groupByName).fieldName(field);
    }

    public static GroupByHistogram.Builder groupByHistogram(String groupByName, String field) {
        return GroupByHistogram.newBuilder().groupByName(groupByName).fieldName(field);
    }

    public static GroupByDateHistogram.Builder groupByDateHistogram(String groupByName, String field) {
        return GroupByDateHistogram.newBuilder().groupByName(groupByName).fieldName(field);
    }

    public static GroupByFilter.Builder groupByFilter(String groupByName) {
        return GroupByFilter.newBuilder().groupByName(groupByName);
    }

    public static GroupByGeoDistance.Builder groupByGeoDistance(String groupByName, String field) {
        return GroupByGeoDistance.newBuilder().groupByName(groupByName).fieldName(field);
    }

    public static GroupByGeoGrid.Builder groupByGeoGrid(String groupByName, String field) {
        return GroupByGeoGrid.newBuilder().groupByName(groupByName).fieldName(field);
    }

    public static GroupByComposite.Builder groupByComposite(String groupByName) {
        return GroupByComposite.newBuilder().groupByName(groupByName);
    }
}
