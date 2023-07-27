package com.alicloud.openservices.tablestore.model.search.groupby;

/**
 * GroupBy 总的构建器。
 * 所有的 GroupBy 进行使用时候，均用到该类。
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
}
