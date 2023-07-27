package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.model.search.GeoPoint;
import com.alicloud.openservices.tablestore.model.search.groupby.FieldRange;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupBy;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByDateHistogram;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByField;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByFilter;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByGeoDistance;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByHistogram;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByRange;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByType;
import com.alicloud.openservices.tablestore.model.search.groupby.Range;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.google.protobuf.ByteString;

import java.util.List;

/**
 * {@link GroupBy} serialization tool class. For deserialization, please refer to {@link SearchGroupByParser}
 */
public class SearchGroupByBuilder {

    private static Search.GroupByType buildGroupByType(GroupByType type) {
        switch (type) {
            case GROUP_BY_FIELD:
                return Search.GroupByType.GROUP_BY_FIELD;
            case GROUP_BY_RANGE:
                return Search.GroupByType.GROUP_BY_RANGE;
            case GROUP_BY_FILTER:
                return Search.GroupByType.GROUP_BY_FILTER;
            case GROUP_BY_GEO_DISTANCE:
                return Search.GroupByType.GROUP_BY_GEO_DISTANCE;
            case GROUP_BY_HISTOGRAM:
                return Search.GroupByType.GROUP_BY_HISTOGRAM;
            case GROUP_BY_DATE_HISTOGRAM:
                return Search.GroupByType.GROUP_BY_DATE_HISTOGRAM;
            default:
                throw new IllegalArgumentException("unknown GroupByType: " + type.name());
        }
    }

    public static Search.GroupByField buildGroupByField(GroupByField groupBy) {
        Search.GroupByField.Builder builder = Search.GroupByField.newBuilder();
        builder.setFieldName(groupBy.getFieldName());
        if (groupBy.getSize() != null) {
            builder.setSize(groupBy.getSize());
        }
        if (groupBy.getMinDocCount() != null) {
            builder.setMinDocCount(groupBy.getMinDocCount());
        }
        if (groupBy.getSubAggregations() != null) {
            builder.setSubAggs(SearchAggregationBuilder.buildAggregations(groupBy.getSubAggregations()));
        }
        if (groupBy.getSubGroupBys() != null) {
            builder.setSubGroupBys(buildGroupBys(groupBy.getSubGroupBys()));
        }
        if (groupBy.getGroupBySorters() != null) {
            builder.setSort(SearchSortBuilder.buildGroupBySort(groupBy.getGroupBySorters()));
        }
        return builder.build();
    }

    public static Search.GroupByHistogram buildGroupByHistogram(GroupByHistogram groupBy) {
        Search.GroupByHistogram.Builder builder = Search.GroupByHistogram.newBuilder();
        if (groupBy.getFieldName() != null) {
            builder.setFieldName(groupBy.getFieldName());
        }
        if (groupBy.getInterval() != null) {
            builder.setInterval(ByteString.copyFrom(SearchVariantType.toVariant(groupBy.getInterval())));
        }
        if (groupBy.getMinDocCount() != null) {
            builder.setMinDocCount(groupBy.getMinDocCount());
        }
        if (groupBy.getMissing() != null) {
            builder.setMissing(ByteString.copyFrom(SearchVariantType.toVariant(groupBy.getMissing())));
        }
        if (groupBy.getGroupBySorters() != null) {
            builder.setSort(SearchSortBuilder.buildGroupBySort(groupBy.getGroupBySorters()));
        }
        if (groupBy.getSubAggregations() != null) {
            builder.setSubAggs(SearchAggregationBuilder.buildAggregations(groupBy.getSubAggregations()));
        }
        if (groupBy.getSubGroupBys() != null) {
            builder.setSubGroupBys(buildGroupBys(groupBy.getSubGroupBys()));
        }
        if (groupBy.getFieldRange() != null) {
            builder.setFieldRange(buildFieldRange(groupBy.getFieldRange()));
        }
        return builder.build();
    }

    public static Search.GroupByDateHistogram buildGroupByDateHistogram(GroupByDateHistogram groupBy) {
        Search.GroupByDateHistogram.Builder builder = Search.GroupByDateHistogram.newBuilder();
        if (groupBy.getFieldName() != null) {
            builder.setFieldName(groupBy.getFieldName());
        }
        if (groupBy.getInterval() != null) {
            builder.setInterval(SearchProtocolBuilder.buildDateTimeValue(groupBy.getInterval()));
        }
        if (groupBy.getMissing() != null) {
            builder.setMissing(ByteString.copyFrom(SearchVariantType.toVariant(groupBy.getMissing())));
        }
        if (groupBy.getGroupBySorters() != null) {
            builder.setSort(SearchSortBuilder.buildGroupBySort(groupBy.getGroupBySorters()));
        }
        if (groupBy.getMinDocCount() != null) {
            builder.setMinDocCount(groupBy.getMinDocCount());
        }
        if (groupBy.getTimeZone() != null) {
            builder.setTimeZone(groupBy.getTimeZone());
        }
        if (groupBy.getFieldRange() != null) {
            builder.setFieldRange(buildFieldRange(groupBy.getFieldRange()));
        }
        if (groupBy.getSubAggregations() != null) {
            builder.setSubAggs(SearchAggregationBuilder.buildAggregations(groupBy.getSubAggregations()));
        }
        if (groupBy.getSubGroupBys() != null) {
            builder.setSubGroupBys(buildGroupBys(groupBy.getSubGroupBys()));
        }
        return builder.build();
    }

    public static Search.GroupByGeoDistance buildGroupByGeoDistance(GroupByGeoDistance groupBy) {
        Search.GroupByGeoDistance.Builder builder = Search.GroupByGeoDistance.newBuilder();
        builder.setFieldName(groupBy.getFieldName());
        if (groupBy.getOrigin() == null) {
            throw new IllegalArgumentException("GroupByGeoDistance must set origin.");
        }
        builder.setOrigin(buildGeoPoint(groupBy.getOrigin()));

        if (groupBy.getRanges() == null || groupBy.getRanges().size() == 0) {
            throw new IllegalArgumentException("GroupByGeoDistance must add range.");
        }
        for (Range range : groupBy.getRanges()) {
            builder.addRanges(buildRange(range));
        }
        if (groupBy.getSubGroupBys() != null) {
            builder.setSubGroupBys(buildGroupBys(groupBy.getSubGroupBys()));
        }
        if (groupBy.getSubAggregations() != null) {
            builder.setSubAggs(SearchAggregationBuilder.buildAggregations(groupBy.getSubAggregations()));
        }
        return builder.build();
    }

    public static Search.GroupByRange buildGroupByRange(GroupByRange groupBy) {
        Search.GroupByRange.Builder builder = Search.GroupByRange.newBuilder();
        builder.setFieldName(groupBy.getFieldName());
        if (groupBy.getRanges() == null || groupBy.getRanges().size() == 0) {
            throw new IllegalArgumentException("GroupByRange must add range.");
        }
        for (Range range : groupBy.getRanges()) {
            builder.addRanges(buildRange(range));
        }
        if (groupBy.getSubGroupBys() != null) {
            builder.setSubGroupBys(buildGroupBys(groupBy.getSubGroupBys()));
        }
        if (groupBy.getSubAggregations() != null) {
            builder.setSubAggs(SearchAggregationBuilder.buildAggregations(groupBy.getSubAggregations()));
        }
        return builder.build();
    }

    public static Search.GroupByFilter buildGroupByFilter(GroupByFilter groupBy) {
        Search.GroupByFilter.Builder builder = Search.GroupByFilter.newBuilder();
        if (groupBy.getFilters() != null) {
            for (Query query : groupBy.getFilters()) {
                builder.addFilters(SearchQueryBuilder.buildQuery(query));
            }
        }
        if (groupBy.getSubGroupBys() != null) {
            builder.setSubGroupBys(buildGroupBys(groupBy.getSubGroupBys()));
        }
        if (groupBy.getSubAggregations() != null) {
            builder.setSubAggs(SearchAggregationBuilder.buildAggregations(groupBy.getSubAggregations()));
        }
        return builder.build();
    }

    private static Search.Range buildRange(Range range) {
        Search.Range.Builder builder = Search.Range.newBuilder();
        if (!range.getFrom().equals(Double.MIN_VALUE)){
            builder.setFrom(range.getFrom());
        }
        if (!range.getTo().equals(Double.MAX_VALUE)){
            builder.setTo(range.getTo());
        }
        return builder.build();
    }

    private static Search.GeoPoint buildGeoPoint(GeoPoint geoPoint) {
        Search.GeoPoint.Builder builder = Search.GeoPoint.newBuilder();
        builder.setLat(geoPoint.getLat());
        builder.setLon(geoPoint.getLon());
        return builder.build();
    }

    public static Search.GroupBy buildGroupBy(GroupBy groupBy) {
        Search.GroupBy.Builder builder = Search.GroupBy.newBuilder();
        builder.setName(groupBy.getGroupByName());
        builder.setType(buildGroupByType(groupBy.getGroupByType()));
        builder.setBody(groupBy.serialize());
        return builder.build();
    }

    private static Search.FieldRange buildFieldRange(FieldRange groupBy) {
        Search.FieldRange.Builder builder = Search.FieldRange.newBuilder();
        if (groupBy.getMax() != null) {
            builder.setMax(ByteString.copyFrom(SearchVariantType.toVariant(groupBy.getMax())));
        }
        if (groupBy.getMin() != null) {
            builder.setMin(ByteString.copyFrom(SearchVariantType.toVariant(groupBy.getMin())));
        }
        return builder.build();
    }

    public static Search.GroupBys buildGroupBys(List<GroupBy> groupBys) {
        Search.GroupBys.Builder builder = Search.GroupBys.newBuilder();
        for (GroupBy groupBy : groupBys) {
            builder.addGroupBys(buildGroupBy(groupBy));
        }
        return builder.build();
    }

}
